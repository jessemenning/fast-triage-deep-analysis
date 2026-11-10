"""Map the engine's compiled relation schema (Calcite JSON) to our schema[] and JSON Schema."""

from __future__ import annotations

from typing import Any

from continuum_mcp.models import SchemaColumn

_INTEGER = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT"}
_NUMBER = {"REAL", "DOUBLE", "DECIMAL"}
_STRING = {"CHAR", "VARCHAR", "UUID", "BINARY", "VARBINARY", "NULL"}
_TEMPORAL_FORMAT = {"DATE": "date", "TIME": "time", "TIMESTAMP": "date-time", "TIMESTAMP_TZ": "date-time"}
_OBJECT = {"VARIANT", "MAP", "STRUCT"}


def sql_type_label(ct: dict[str, Any]) -> str:
    """Human-readable SQL type, e.g. DECIMAL(18,2), ARRAY<VARCHAR>."""
    t = str(ct.get("type", "UNKNOWN")).upper()
    precision = ct.get("precision")
    scale = ct.get("scale")
    if t == "DECIMAL" and precision is not None and precision >= 0:
        return f"DECIMAL({precision},{scale or 0})"
    if t in {"VARCHAR", "CHAR", "BINARY", "VARBINARY"} and precision is not None and precision >= 0:
        return f"{t}({precision})"
    if t == "ARRAY" and ct.get("component"):
        return f"ARRAY<{sql_type_label(ct['component'])}>"
    if t == "MAP" and ct.get("key") and ct.get("value"):
        return f"MAP<{sql_type_label(ct['key'])},{sql_type_label(ct['value'])}>"
    return t


def json_schema_for_type(ct: dict[str, Any]) -> dict[str, Any]:
    """JSON Schema fragment for one column type (nullability handled by caller)."""
    t = str(ct.get("type", "")).upper()
    if t in _INTEGER:
        return {"type": "integer"}
    if t in _NUMBER:
        return {"type": "number"}
    if t == "BOOLEAN":
        return {"type": "boolean"}
    if t in _TEMPORAL_FORMAT:
        return {"type": "string", "format": _TEMPORAL_FORMAT[t]}
    if t.startswith("INTERVAL"):
        return {"type": "string", "description": t}
    if t == "ARRAY":
        items = json_schema_for_type(ct["component"]) if ct.get("component") else {}
        return {"type": "array", "items": items}
    if t in _OBJECT:
        schema: dict[str, Any] = {"type": "object"}
        if t == "STRUCT" and ct.get("fields"):
            schema["properties"] = {
                f["name"]: json_schema_for_field(f) for f in ct["fields"]
            }
        return schema
    if t in _STRING:
        return {"type": "string"}
    return {"type": "string", "description": f"unmapped SQL type {t}"}


def json_schema_for_field(field: dict[str, Any]) -> dict[str, Any]:
    ct = field.get("columntype", {})
    schema = json_schema_for_type(ct)
    schema["description"] = f"SQL type {sql_type_label(ct)}"
    if ct.get("nullable", True):
        base = schema.get("type")
        if isinstance(base, str):
            schema["type"] = [base, "null"]
    return schema


def relation_columns(relation: dict[str, Any]) -> list[SchemaColumn]:
    return [
        SchemaColumn(
            name=f["name"],
            type=sql_type_label(f.get("columntype", {})),
            nullable=bool(f.get("columntype", {}).get("nullable", True)),
        )
        for f in relation.get("fields", [])
    ]


def row_json_schema(
    relation: dict[str, Any], *, title: str, description: str = "", only: list[str] | None = None
) -> dict[str, Any]:
    """Draft-07 object schema for one row of a relation (optionally a subset of columns)."""
    fields = relation.get("fields", [])
    if only is not None:
        wanted = {c.lower() for c in only}
        fields = [f for f in fields if f["name"].lower() in wanted]
    properties = {f["name"]: json_schema_for_field(f) for f in fields}
    required = [f["name"] for f in fields if not f.get("columntype", {}).get("nullable", True)]
    schema: dict[str, Any] = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "title": title,
        "properties": properties,
        "additionalProperties": False,
    }
    if description:
        schema["description"] = description
    if required:
        schema["required"] = required
    return schema


def envelope_json_schema(row_schema: dict[str, Any], envelope: str) -> dict[str, Any]:
    """Wrap a row schema in the sink's wire envelope ('bare' or 'insert')."""
    if envelope == "bare":
        return row_schema
    inner = {k: v for k, v in row_schema.items() if k != "$schema"}
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "title": row_schema.get("title", "Row") + "Envelope",
        "description": "Change event from the Continuum engine; the row is under 'insert'.",
        "properties": {"insert": inner},
        "required": ["insert"],
    }


def _label_to_columntype(label: str) -> dict[str, Any]:
    """Reverse of sql_type_label for the labels we emit (enough to rebuild a JSON Schema)."""
    base = label.split("(")[0].split("<")[0].strip().upper()
    ct: dict[str, Any] = {"type": base, "nullable": True}
    if base == "ARRAY" and "<" in label:
        ct["component"] = _label_to_columntype(label[label.index("<") + 1 : label.rindex(">")])
    return ct


def json_schema_from_columns(
    columns: list[SchemaColumn],
    *,
    title: str,
    description: str = "",
    only: list[str] | None = None,
    string_columns: set[str] | None = None,
) -> dict[str, Any]:
    """Row JSON Schema from stored schema[] entries (used when program_info is not at hand).

    `string_columns` are rendered as non-null strings: topic placeholder columns are
    COALESCE(CAST(x AS VARCHAR), 'unknown') in the sink projection.
    """
    fields = []
    wanted = {c.lower() for c in only} if only is not None else None
    for c in columns:
        if wanted is not None and c.name.lower() not in wanted:
            continue
        ct = _label_to_columntype(c.type)
        ct["nullable"] = c.nullable
        if string_columns and c.name in string_columns:
            ct = {"type": "VARCHAR", "nullable": False}
        fields.append({"name": c.name, "columntype": ct})
    return row_json_schema({"fields": fields}, title=title, description=description)


def find_relation(program_info: dict[str, Any], name: str) -> dict[str, Any] | None:
    """Locate an output relation by (case-insensitive) name in validate_program output."""
    schema = program_info.get("schema", {}) or {}
    for rel in list(schema.get("outputs", [])) + list(schema.get("inputs", [])):
        if str(rel.get("name", "")).lower() == name.lower():
            return rel
    return None
