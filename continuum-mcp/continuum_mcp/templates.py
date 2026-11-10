"""Render a complete engine program for a data product from Jinja2 templates.

The renderer also keeps a line map so SQL compiler messages can be pointed
back at the user's SELECT (or at the trigger that caused them) instead of at
line 187 of a generated file.

Source streams render differently by kind:

* raw      — the broker payload lands in a VARCHAR `data` column (format `raw`) and the
             stream's hand-authored `decode_sql` builds the relation.
* derived  — the base pipeline's JSON sink is ingested with `format: json` and
             `update_format: insert_delete` into a typed table, so inserts and deletes are
             applied by the engine itself; the relation is a view over that table.
             Streams flagged `snapshot: true` add a one-shot `url_input` connector that
             loads the base pipeline's current rows first (see README, "initial state").
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import quote

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from continuum_mcp.config import Settings
from continuum_mcp.models import StreamDef, TriggerSpec
from continuum_mcp.sql_guard import parse_recency_window, placeholders

TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"
PIPELINE_TAG = "continuum:data-product"
PIPELINE_VERSION_TAG = "continuum:v1"


def pipeline_name(slug: str) -> str:
    return f"cp-{slug}"


def queue_name(slug: str, stream: str) -> str:
    return f"fl-cp-{slug}-{stream}"


def connector_name(slug: str, stream: str) -> str:
    return f"cp-{slug}-{stream}"


def view_name_for(slug: str) -> str:
    return slug.replace("-", "_")


def trigger_view_name(slug: str, trigger: str) -> str:
    return f"trg_{view_name_for(slug)}_{trigger.replace('-', '_')}"


def pipeline_tags(slug: str) -> list[str]:
    return [PIPELINE_TAG, f"continuum:slug={slug}", PIPELINE_VERSION_TAG]


def raw_table_name(stream: StreamDef) -> str:
    return f"{stream.relation}_raw"


@dataclass
class TriggerRender:
    spec: TriggerSpec
    payload_columns: list[str]
    warnings: list[str] = field(default_factory=list)


@dataclass
class RenderSpec:
    name: str
    slug: str
    description: str
    sql: str
    streams: list[StreamDef]
    live_topic: str
    columns: list[str] | None = None  # product output columns; None = validation pass
    event_time_column: str | None = None
    triggers: list[TriggerRender] = field(default_factory=list)
    include_sinks: bool = True


@dataclass
class Section:
    label: str
    start_line: int  # 1-based, inclusive
    end_line: int  # 1-based, inclusive


@dataclass
class RenderedProgram:
    sql: str
    user_sql_start_line: int
    user_sql_line_count: int
    sections: list[Section]

    def locate(self, line: int) -> tuple[str, int | None]:
        """Return (label, remapped_line). Remapped line is user-relative when inside the SELECT."""
        end = self.user_sql_start_line + self.user_sql_line_count - 1
        if self.user_sql_start_line <= line <= end:
            return "your SELECT", line - self.user_sql_start_line + 1
        for s in self.sections:
            if s.start_line <= line <= s.end_line:
                return s.label, line - s.start_line + 1
        return "generated program", line

    def remap_message(self, msg: dict[str, Any]) -> dict[str, Any]:
        """Turn one SqlCompilerMessage into a user-facing dict with a remapped location."""
        line = int(msg.get("start_line_number") or 0)
        label, rel = self.locate(line) if line else ("program", None)
        where = f"{label}" + (f", line {rel}" if rel else "")
        col = msg.get("start_column")
        if col and label == "your SELECT":
            where += f", column {col}"
        return {
            "message": msg.get("message", ""),
            "error_type": msg.get("error_type", ""),
            "warning": bool(msg.get("warning", False)),
            "where": where,
            "snippet": msg.get("snippet"),
        }


class ProgramRenderer:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.env = Environment(
            loader=FileSystemLoader(str(TEMPLATE_DIR)),
            undefined=StrictUndefined,
            trim_blocks=True,
            lstrip_blocks=True,
            keep_trailing_newline=True,
            autoescape=False,
        )
        self.broker = settings.broker_for_engine

    # ------------------------------------------------------------------ envelope
    @property
    def input_update_format(self) -> str:
        """update_format for JSON produced by the engine's own sinks."""
        return "insert_delete" if self.settings.trigger_envelope == "insert" else "raw"

    # ------------------------------------------------------------------ connectors
    def snapshot_url(self, stream: StreamDef) -> str:
        sql = stream.snapshot_sql or f"SELECT * FROM {stream.relation}"
        base = self.settings.feldera_internal_url.rstrip("/")
        return f"{base}/v0/pipelines/{stream.base_pipeline}/query?sql={quote(sql, safe='')}&format=json"

    def stream_connectors(self, stream: StreamDef, slug: str) -> list[dict[str, Any]]:
        live_cfg: dict[str, Any] = {**self.broker, "queue": queue_name(slug, stream.name)}
        connectors: list[dict[str, Any]] = []
        if stream.kind == "raw":
            if stream.include_topic:
                live_cfg["include_topic"] = True
            if stream.include_broker_timestamp:
                live_cfg["include_broker_timestamp"] = True
            live_cfg["topic_pattern"] = stream.topic_pattern
            connectors.append(
                {
                    "name": connector_name(slug, stream.name),
                    "transport": {"name": "solace_input", "config": live_cfg},
                    "format": {"name": "raw", "config": {"column_name": "data"}},
                }
            )
            return connectors
        if stream.snapshot:
            connectors.append(
                {
                    "name": f"{connector_name(slug, stream.name)}-snapshot",
                    "transport": {"name": "url_input", "config": {"path": self.snapshot_url(stream)}},
                    "format": {"name": "json", "config": {"update_format": "raw", "array": False}},
                }
            )
        if stream.changelog == "inserts_only":
            live_cfg["include_broker_timestamp"] = True
        connectors.append(
            {
                "name": connector_name(slug, stream.name),
                "transport": {"name": "solace_input", "config": live_cfg},
                "format": {"name": "json", "config": {"update_format": self.input_update_format, "array": False}},
            }
        )
        return connectors

    @staticmethod
    def connectors_json(connectors: list[dict[str, Any]]) -> str:
        """JSON for the SQL 'connectors' property; single quotes doubled for the SQL string literal."""
        return json.dumps(connectors, indent=4).replace("'", "''")

    # ------------------------------------------------------------------ streams
    def stream_ddl(self, stream: StreamDef) -> str:
        if stream.ddl:
            return stream.ddl.strip().rstrip(";")
        if stream.kind != "derived":
            raise ValueError(f"stream {stream.name!r} has no ddl")
        tpl = self.env.get_template("derived_source.sql.j2")
        return tpl.render(
            part="ddl",
            stream=stream,
            raw_table=raw_table_name(stream),
            with_broker_ts=stream.changelog == "inserts_only",
        ).strip()

    def stream_decode_sql(self, stream: StreamDef) -> str:
        if stream.decode_sql:
            text = stream.decode_sql
            if "{{" in text or "{%" in text:
                text = self.env.from_string(text).render(envelope=self.settings.trigger_envelope)
            return text.strip()
        if stream.kind != "derived":
            raise ValueError(f"stream {stream.name!r} has no decode_sql")
        if not stream.key_columns and stream.changelog == "inserts_only":
            raise ValueError(f"derived stream {stream.name!r} needs key_columns for latest-wins dedup")
        tpl = self.env.get_template("derived_source.sql.j2")
        return tpl.render(
            part="decode",
            stream=stream,
            raw_table=raw_table_name(stream),
            column_names=stream.column_names,
        ).strip()

    def render_source(self, stream: StreamDef, slug: str) -> str:
        if not stream.ingestable:
            raise ValueError(f"stream {stream.name!r} is not ingestable")
        tpl = self.env.get_template("source_table.sql.j2")
        return tpl.render(
            stream=stream,
            queue=queue_name(slug, stream.name),
            ddl=self.stream_ddl(stream),
            connectors_json=self.connectors_json(self.stream_connectors(stream, slug)),
            decode_sql=self.stream_decode_sql(stream),
        )

    # ------------------------------------------------------------------ product
    def render_program(self, spec: RenderSpec) -> RenderedProgram:
        view = view_name_for(spec.slug)
        sources = [self.render_source(s, spec.slug) for s in spec.streams]
        product_view = self.env.get_template("product_view.sql.j2").render(
            name=spec.name,
            slug=spec.slug,
            description=spec.description or "",
            view_name=view,
            sql=spec.sql,
        )
        live_sink = ""
        triggers: list[str] = []
        if spec.include_sinks:
            columns = spec.columns or []
            live_sink = self.env.get_template("live_sink.sql.j2").render(
                slug=spec.slug,
                view_name=view,
                topic=spec.live_topic,
                broker=self.broker,
                columns=columns,
                placeholder_columns=set(placeholders(spec.live_topic)),
            )
            for tr in spec.triggers:
                recency = parse_recency_window(tr.spec.recency_window) if tr.spec.recency_window else None
                triggers.append(
                    self.env.get_template("trigger_sink.sql.j2").render(
                        slug=spec.slug,
                        view_name=view,
                        trigger=tr.spec,
                        trigger_view=trigger_view_name(spec.slug, tr.spec.name),
                        broker=self.broker,
                        payload_columns=tr.payload_columns,
                        placeholder_columns=set(placeholders(tr.spec.output_topic_template)),
                        recency=recency,
                        event_time_column=spec.event_time_column,
                    )
                )
        program = self.env.get_template("program.sql.j2").render(
            name=spec.name,
            slug=spec.slug,
            pipeline=pipeline_name(spec.slug),
            sources=sources,
            product_view=product_view,
            live_sink=live_sink,
            triggers=triggers,
        ).rstrip() + "\n"
        return self._with_line_map(program, spec, view)

    def _with_line_map(self, program: str, spec: RenderSpec, view: str) -> RenderedProgram:
        lines = program.splitlines()
        header = f"CREATE MATERIALIZED VIEW {view} AS"
        try:
            idx = next(i for i, ln in enumerate(lines) if ln.strip() == header)
        except StopIteration as exc:  # pragma: no cover - template invariant
            raise RuntimeError("product view header not found in rendered program") from exc
        start = idx + 2  # 1-based line after the header
        count = len(spec.sql.strip().splitlines())
        sections: list[Section] = []
        markers: list[tuple[int, str]] = []
        for i, ln in enumerate(lines, start=1):
            s = ln.strip()
            if s.startswith("-- Source stream:"):
                markers.append((i, f"source stream {s.split(':', 1)[1].split('(')[0].strip()}"))
            elif s.startswith(f"CREATE MATERIALIZED VIEW {view}_live_sink"):
                markers.append((i, "live sink"))
            elif s.startswith("CREATE MATERIALIZED VIEW trg_"):
                markers.append((i, f"trigger view {s.split()[3]}"))
        for n, (ln_no, label) in enumerate(markers):
            end = markers[n + 1][0] - 1 if n + 1 < len(markers) else len(lines)
            sections.append(Section(label, ln_no, end))
        return RenderedProgram(program, start, count, sections)
