"""Pure-Python guards for user-supplied SQL, slugs and topic templates.

Nothing here talks to the engine: these are the cheap, deterministic checks we
run before spending a compile or touching the broker.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

SLUG_RE = re.compile(r"^[a-z][a-z0-9-]{2,40}$")
TRIGGER_NAME_RE = re.compile(r"^[a-z][a-z0-9-]{1,40}$")
IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
PLACEHOLDER_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")
_ANY_BRACE_RE = re.compile(r"\{[^}]*\}")

# Statement kinds that must never appear in a data-product SELECT.
_FORBIDDEN = (
    "create", "drop", "alter", "insert", "update", "delete", "truncate", "merge",
    "grant", "revoke", "lateness", "attach", "detach", "copy", "pragma", "set",
    "call", "execute", "exec", "declare", "begin", "commit", "rollback",
)
_FORBIDDEN_RE = re.compile(r"\b(" + "|".join(_FORBIDDEN) + r")\b", re.IGNORECASE)

_STRING_RE = re.compile(r"'(?:[^']|'')*'")
_LINE_COMMENT_RE = re.compile(r"--[^\n]*")
_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
_FROM_JOIN_RE = re.compile(
    r"\b(?:from|join)\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)",
    re.IGNORECASE,
)
_CTE_RE = re.compile(r"(?:\bwith\b|,)\s*([A-Za-z_][A-Za-z0-9_]*)\s*(?:\([^)]*\))?\s+as\s*\(", re.IGNORECASE)
_SQL_FUNCS = {"unnest", "table", "lateral", "values"}
_RECENCY_RE = re.compile(r"^\s*(\d{1,6})\s*(second|minute|hour|day)s?\s*$", re.IGNORECASE)


class SqlGuardError(ValueError):
    """Raised when user input fails a static check. Message is user-facing."""


def strip_literals_and_comments(sql: str) -> str:
    sql = _BLOCK_COMMENT_RE.sub(" ", sql)
    sql = _LINE_COMMENT_RE.sub(" ", sql)
    return _STRING_RE.sub("''", sql)


def ensure_single_select(sql: str) -> str:
    """Return the trimmed SQL if it is exactly one SELECT (or WITH ... SELECT)."""
    if not sql or not sql.strip():
        raise SqlGuardError("sql is empty")
    cleaned = strip_literals_and_comments(sql)
    if ";" in cleaned:
        raise SqlGuardError("sql must be a single statement without ';'")
    head = cleaned.lstrip().split(None, 1)[0].lower() if cleaned.strip() else ""
    if head not in {"select", "with"}:
        raise SqlGuardError("sql must be a single SELECT statement (optionally starting with WITH)")
    m = _FORBIDDEN_RE.search(cleaned)
    if m:
        raise SqlGuardError(f"sql must be read-only: found forbidden keyword '{m.group(1).upper()}'")
    return sql.strip()


def extract_relations(sql: str) -> set[str]:
    """Names referenced after FROM/JOIN, minus CTE names, lower-cased.

    Regex-based on purpose: good enough to route a query to the pipeline that
    materializes its relations, and to reject unknown ones before compiling.
    """
    cleaned = strip_literals_and_comments(sql)
    ctes = {m.group(1).lower() for m in _CTE_RE.finditer(cleaned)}
    found: set[str] = set()
    for m in _FROM_JOIN_RE.finditer(cleaned):
        name = m.group(1).lower()
        bare = name.split(".")[-1]
        if bare in _SQL_FUNCS or bare in ctes:
            continue
        found.add(bare)
    return found


def slugify(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.strip().lower()).strip("-")
    slug = re.sub(r"-{2,}", "-", slug)
    if not slug or not slug[0].isalpha():
        slug = "dp-" + slug if slug else ""
    slug = slug[:41].rstrip("-")
    if not SLUG_RE.match(slug):
        raise SqlGuardError(
            f"cannot derive a valid slug from name {name!r}: needs 3-41 chars matching ^[a-z][a-z0-9-]*$"
        )
    return slug


def validate_trigger_name(name: str) -> str:
    if not TRIGGER_NAME_RE.match(name or ""):
        raise SqlGuardError(
            f"trigger name {name!r} must match ^[a-z][a-z0-9-]{{1,40}}$ (kebab-case)"
        )
    return name


def validate_identifiers(names: list[str], what: str) -> list[str]:
    bad = [n for n in names if not IDENT_RE.match(n or "")]
    if bad:
        raise SqlGuardError(f"{what} must be plain SQL identifiers, got: {bad}")
    return names


def placeholders(template: str) -> list[str]:
    """Ordered, de-duplicated {field} names in a topic template."""
    seen: list[str] = []
    for m in PLACEHOLDER_RE.finditer(template):
        if m.group(1) not in seen:
            seen.append(m.group(1))
    return seen


@dataclass
class TopicCheck:
    ok: bool
    errors: list[str] = field(default_factory=list)


def validate_topic_template(
    template: str,
    *,
    required_prefix: str | None = None,
    allowed_fields: set[str] | None = None,
    max_bytes: int = 250,
) -> TopicCheck:
    errors: list[str] = []
    if not template:
        return TopicCheck(False, ["topic template is empty"])
    if required_prefix and not template.startswith(required_prefix):
        errors.append(f"topic must start with '{required_prefix}'")
    if any(ch in template for ch in ("*", ">", " ", "\t", "\n")):
        errors.append("topic must not contain wildcards ('*', '>') or whitespace")
    if "//" in template or template.startswith("/") or template.endswith("/"):
        errors.append("topic must not contain empty levels (leading, trailing or double '/')")
    if len(template.encode("utf-8")) > max_bytes:
        errors.append(f"topic must be at most {max_bytes} bytes")
    names = placeholders(template)
    if not names:
        errors.append(
            "topic must contain at least one {column} placeholder — static topics batch "
            "multiple rows into one message and cannot be routed per key"
        )
    malformed = [b for b in _ANY_BRACE_RE.findall(template) if not PLACEHOLDER_RE.fullmatch(b)]
    if malformed:
        errors.append(f"malformed placeholders: {malformed}")
    if "{" in _ANY_BRACE_RE.sub("", template) or "}" in _ANY_BRACE_RE.sub("", template):
        errors.append("unbalanced braces in topic template")
    if allowed_fields is not None:
        unknown = [n for n in names if n not in allowed_fields]
        if unknown:
            errors.append(
                f"placeholders {unknown} are not output columns; available: {sorted(allowed_fields)}"
            )
    return TopicCheck(not errors, errors)


def subscribe_pattern(template: str) -> str:
    """Broker subscription matching every topic the template can produce.

    Each {field} becomes the single-level wildcard '*'; a trailing placeholder
    becomes '>' so values that happen to contain '/' are still matched.
    """
    levels = template.split("/")
    out: list[str] = []
    for i, level in enumerate(levels):
        if PLACEHOLDER_RE.search(level):
            out.append(">" if i == len(levels) - 1 else "*")
        else:
            out.append(level)
    return "/".join(out)


def sample_topic(template: str) -> str:
    return PLACEHOLDER_RE.sub(lambda m: f"{m.group(1)}-001", template)


def parse_recency_window(text: str) -> tuple[str, str]:
    """'5 minutes' -> ('5', 'MINUTE') for INTERVAL '5' MINUTE."""
    m = _RECENCY_RE.match(text or "")
    if not m:
        raise SqlGuardError(
            f"recency_window {text!r} must look like '<n> second|minute|hour|day(s)', e.g. '5 minutes'"
        )
    return m.group(1), m.group(2).upper()


def check_condition(condition: str) -> str:
    """A trigger condition is a boolean expression, not a statement."""
    if not condition or not condition.strip():
        raise SqlGuardError("condition is empty")
    cleaned = strip_literals_and_comments(condition)
    if ";" in cleaned:
        raise SqlGuardError("condition must not contain ';'")
    m = _FORBIDDEN_RE.search(cleaned)
    if m:
        raise SqlGuardError(f"condition must be a boolean expression: found '{m.group(1).upper()}'")
    if re.search(r"\bselect\b", cleaned, re.IGNORECASE):
        raise SqlGuardError("condition must not contain a subquery; reference product columns directly")
    return condition.strip()
