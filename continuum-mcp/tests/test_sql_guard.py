import pytest

from continuum_mcp.sql_guard import (
    SqlGuardError,
    check_condition,
    ensure_single_select,
    extract_relations,
    parse_recency_window,
    placeholders,
    sample_topic,
    slugify,
    subscribe_pattern,
    validate_topic_template,
    validate_trigger_name,
)


class TestEnsureSingleSelect:
    def test_accepts_select_and_with(self):
        assert ensure_single_select("  SELECT 1 FROM t ") == "SELECT 1 FROM t"
        assert ensure_single_select("WITH x AS (SELECT 1) SELECT * FROM x").startswith("WITH")

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT 1; DROP TABLE t",
            "DROP TABLE t",
            "INSERT INTO t VALUES (1)",
            "SELECT * FROM t; ",
            "CREATE VIEW v AS SELECT 1",
            "SELECT * FROM t WHERE x = 1 UNION ALL DELETE FROM t",
            "LATENESS t.ts INTERVAL '1' MINUTE",
            "",
        ],
    )
    def test_rejects_non_select(self, sql):
        with pytest.raises(SqlGuardError):
            ensure_single_select(sql)

    def test_keywords_inside_literals_and_comments_are_fine(self):
        sql = "SELECT 'delete me; now' AS note, x -- create something\nFROM t /* drop */"
        assert ensure_single_select(sql)

    def test_forbidden_keyword_message_names_it(self):
        with pytest.raises(SqlGuardError, match="UPDATE"):
            ensure_single_select("SELECT 1 FROM t UPDATE")


class TestExtractRelations:
    def test_from_and_joins(self):
        sql = """SELECT * FROM payments_flat_mv p
        LEFT JOIN device_signals_mv d ON d.payment_id = p.payment_id
        INNER JOIN geo_signals_mv g ON g.payment_id = p.payment_id"""
        assert extract_relations(sql) == {"payments_flat_mv", "device_signals_mv", "geo_signals_mv"}

    def test_excludes_ctes_and_subqueries(self):
        sql = """WITH recent AS (SELECT * FROM payments_flat_mv WHERE amount > 1),
        agg(x) AS (SELECT 1 FROM recent)
        SELECT * FROM recent r JOIN (SELECT * FROM agg) s ON 1=1 JOIN parties_flat pf ON 1=1"""
        assert extract_relations(sql) == {"payments_flat_mv", "parties_flat"}

    def test_schema_qualified_and_case(self):
        assert extract_relations("select * from Public.Payments_Flat_MV") == {"payments_flat_mv"}

    def test_string_literals_ignored(self):
        assert extract_relations("SELECT 'from nowhere' FROM t") == {"t"}


class TestSlug:
    def test_slugify(self):
        assert slugify("High Value Review!") == "high-value-review"
        assert slugify("  Rail__Health (v2) ") == "rail-health-v2"

    def test_slugify_prefixes_digits(self):
        assert slugify("2fa alerts") == "dp-2fa-alerts"

    def test_slugify_rejects_short(self):
        with pytest.raises(SqlGuardError):
            slugify("ab")

    def test_trigger_name(self):
        assert validate_trigger_name("new-device") == "new-device"
        with pytest.raises(SqlGuardError):
            validate_trigger_name("New Device")


class TestTopics:
    def test_placeholders_ordered_unique(self):
        assert placeholders("a/{x}/b/{y}/{x}") == ["x", "y"]

    def test_valid_template(self):
        check = validate_topic_template(
            "fl/cp/demo/live/{payment_id}", required_prefix="fl/cp/demo/", allowed_fields={"payment_id", "amount"}
        )
        assert check.ok, check.errors

    @pytest.mark.parametrize(
        "topic, needle",
        [
            ("fl/cp/demo/live/static", "placeholder"),
            ("fl/cp/other/live/{payment_id}", "must start with"),
            ("fl/cp/demo/*/{payment_id}", "wildcards"),
            ("fl/cp/demo//{payment_id}", "empty levels"),
            ("fl/cp/demo/{payment_id}/", "empty levels"),
            ("fl/cp/demo/{nope}", "not output columns"),
            ("fl/cp/demo/{payment id}", "malformed"),
            ("fl/cp/demo/" + "x" * 260 + "/{payment_id}", "250 bytes"),
        ],
    )
    def test_invalid_templates(self, topic, needle):
        check = validate_topic_template(topic, required_prefix="fl/cp/demo/", allowed_fields={"payment_id"})
        assert not check.ok
        assert any(needle in e for e in check.errors), check.errors

    def test_subscribe_pattern(self):
        assert subscribe_pattern("fl/cp/demo/live/{payment_id}") == "fl/cp/demo/live/>"
        assert subscribe_pattern("fl/payments/rail-health/{rail}/{region}") == "fl/payments/rail-health/*/>"
        assert subscribe_pattern("fl/x/{a}/static") == "fl/x/*/static"

    def test_sample_topic(self):
        assert sample_topic("fl/cp/demo/live/{payment_id}") == "fl/cp/demo/live/payment_id-001"


class TestConditionsAndWindows:
    def test_recency_window(self):
        assert parse_recency_window("5 minutes") == ("5", "MINUTE")
        assert parse_recency_window("1 HOUR") == ("1", "HOUR")
        with pytest.raises(SqlGuardError):
            parse_recency_window("soon")

    def test_condition_guard(self):
        assert check_condition("risk_band = 'RED' AND amount > 1000")
        with pytest.raises(SqlGuardError):
            check_condition("amount > (SELECT MAX(amount) FROM t)")
        with pytest.raises(SqlGuardError):
            check_condition("1=1; DROP TABLE t")
