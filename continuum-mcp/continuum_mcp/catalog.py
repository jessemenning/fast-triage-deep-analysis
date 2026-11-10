"""File-backed catalog: stream registry, base-pipeline products and created-product manifests."""

from __future__ import annotations

import logging
import re
import shutil
from pathlib import Path
from typing import Any

import yaml

from continuum_mcp.models import BasePipeline, BaseProduct, ProductManifest, StreamDef

log = logging.getLogger("continuum-mcp.catalog")

_TOKEN_RE = re.compile(r"[a-z0-9]+")


class CatalogError(ValueError):
    pass


def tokenize(text: str | None) -> list[str]:
    return _TOKEN_RE.findall((text or "").lower())


class Catalog:
    def __init__(self, root: Path):
        self.root = Path(root)
        self.streams: dict[str, StreamDef] = {}
        self.base: BasePipeline | None = None
        self.products: dict[str, ProductManifest] = {}

    # ------------------------------------------------------------------ paths
    @property
    def streams_dir(self) -> Path:
        return self.root / "streams"

    @property
    def base_dir(self) -> Path:
        return self.root / "base"

    @property
    def products_dir(self) -> Path:
        return self.root / "products"

    def product_dir(self, slug: str) -> Path:
        return self.products_dir / slug

    def program_path(self, slug: str) -> Path:
        return self.product_dir(slug) / "program.sql"

    def prev_program_path(self, slug: str) -> Path:
        return self.product_dir(slug) / "program.prev.sql"

    # ------------------------------------------------------------------ load
    def load(self) -> Catalog:
        self.streams = {}
        for path in sorted(self.streams_dir.glob("*.yaml")):
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            stream = StreamDef.model_validate(data)
            if stream.name != path.stem:
                raise CatalogError(f"{path}: stream name {stream.name!r} must equal the file stem")
            self.streams[stream.name] = stream
        self.base = None
        base_files = sorted(self.base_dir.glob("*.yaml")) if self.base_dir.exists() else []
        if base_files:
            self.base = BasePipeline.model_validate(
                yaml.safe_load(base_files[0].read_text(encoding="utf-8")) or {}
            )
        self.products = {}
        if self.products_dir.exists():
            for mpath in sorted(self.products_dir.glob("*/manifest.yaml")):
                try:
                    manifest = ProductManifest.model_validate(
                        yaml.safe_load(mpath.read_text(encoding="utf-8")) or {}
                    )
                except Exception as exc:  # noqa: BLE001 - keep loading the rest
                    log.warning("skipping unreadable manifest %s: %s", mpath, exc)
                    continue
                self.products[manifest.slug] = manifest
        log.info(
            "catalog loaded: %d streams, %d base products, %d products",
            len(self.streams),
            len(self.base.products) if self.base else 0,
            len(self.products),
        )
        return self

    # ------------------------------------------------------------------ streams
    def stream(self, name: str) -> StreamDef:
        try:
            return self.streams[name]
        except KeyError:
            raise CatalogError(
                f"unknown source stream {name!r}; known: {sorted(self.streams)}"
            ) from None

    def streams_by_kind(self, kind: str | None) -> list[StreamDef]:
        items = sorted(self.streams.values(), key=lambda s: (s.kind, s.name))
        if kind in (None, "all"):
            return items
        return [s for s in items if s.kind == kind]

    def relation_index(self) -> dict[str, StreamDef]:
        return {s.relation.lower(): s for s in self.streams.values()}

    # ------------------------------------------------------------------ base
    def base_products(self) -> list[BaseProduct]:
        return list(self.base.products) if self.base else []

    def base_pipeline_name(self) -> str | None:
        return self.base.pipeline if self.base else None

    def base_relations(self) -> set[str]:
        return {p.name.lower() for p in self.base_products()}

    # ------------------------------------------------------------------ products
    def product(self, name_or_slug: str) -> ProductManifest | None:
        key = (name_or_slug or "").strip().lower()
        if key in self.products:
            return self.products[key]
        for m in self.products.values():
            if m.name.lower() == key or m.pipeline.lower() == key or m.view_name == key:
                return m
        return None

    def save_manifest(
        self,
        manifest: ProductManifest,
        *,
        program_sql: str | None = None,
        prev_program_sql: str | None = None,
    ) -> None:
        d = self.product_dir(manifest.slug)
        d.mkdir(parents=True, exist_ok=True)
        data = manifest.model_dump(mode="json", by_alias=True)
        (d / "manifest.yaml").write_text(
            yaml.safe_dump(data, sort_keys=False, allow_unicode=True, width=120),
            encoding="utf-8",
        )
        if program_sql is not None:
            self.program_path(manifest.slug).write_text(program_sql, encoding="utf-8")
        if prev_program_sql is not None:
            self.prev_program_path(manifest.slug).write_text(prev_program_sql, encoding="utf-8")
        self.products[manifest.slug] = manifest

    def read_program(self, slug: str) -> str | None:
        p = self.program_path(slug)
        return p.read_text(encoding="utf-8") if p.exists() else None

    def read_prev_program(self, slug: str) -> str | None:
        p = self.prev_program_path(slug)
        return p.read_text(encoding="utf-8") if p.exists() else None

    def delete_product(self, slug: str) -> bool:
        self.products.pop(slug, None)
        d = self.product_dir(slug)
        if d.exists():
            shutil.rmtree(d)
            return True
        return False

    # ------------------------------------------------------------------ search
    def search(self, keywords: str, limit: int = 10) -> list[dict[str, Any]]:
        """Deterministic token-overlap ranking over products, streams and triggers."""
        query = [t for t in tokenize(keywords) if len(t) > 1]
        if not query:
            return []
        results: list[dict[str, Any]] = []
        for entry in self._search_entries():
            score, why = _score_entry(entry, query)
            if score > 0:
                results.append(
                    {
                        "type": entry["type"],
                        "name": entry["name"],
                        "score": round(score, 2),
                        "why": why,
                        **({"product": entry["product"]} if "product" in entry else {}),
                    }
                )
        results.sort(key=lambda r: (-r["score"], r["type"], r["name"]))
        return results[: max(1, limit)]

    def _search_entries(self) -> list[dict[str, Any]]:
        entries: list[dict[str, Any]] = []
        for m in self.products.values():
            entries.append(
                {
                    "type": "product",
                    "name": m.name,
                    "fields": {
                        "name": [m.name, m.slug],
                        "tags": m.tags,
                        "description": [m.description],
                        "columns": m.column_names,
                        "topic": [m.live_topic],
                        "relation": [m.view_name],
                    },
                }
            )
            for t in m.triggers:
                entries.append(
                    {
                        "type": "trigger",
                        "name": t.name,
                        "product": m.name,
                        "fields": {
                            "name": [t.name],
                            "tags": [],
                            "description": [t.description, t.condition],
                            "columns": t.payload_columns,
                            "topic": [t.topic_template],
                            "relation": [t.view_name],
                        },
                    }
                )
        for p in self.base_products():
            entries.append(
                {
                    "type": "product",
                    "name": p.name,
                    "fields": {
                        "name": [p.name],
                        "tags": p.tags,
                        "description": [p.description],
                        "columns": p.key_columns,
                        "topic": [p.live_topic or ""],
                        "relation": [p.name],
                    },
                }
            )
        for s in self.streams.values():
            entries.append(
                {
                    "type": "stream",
                    "name": s.name,
                    "fields": {
                        "name": [s.name],
                        "tags": [s.kind],
                        "description": [s.description],
                        "columns": s.column_names,
                        "topic": [s.topic_pattern or ""] + s.topic_subscriptions,
                        "relation": [s.relation],
                    },
                }
            )
        return entries


_WEIGHTS = {"name": 3.0, "relation": 2.0, "tags": 2.0, "columns": 1.5, "topic": 1.0, "description": 1.0}


def _score_entry(entry: dict[str, Any], query: list[str]) -> tuple[float, list[str]]:
    score = 0.0
    why: list[str] = []
    for field_name, weight in _WEIGHTS.items():
        values = [v for v in entry["fields"].get(field_name, []) if v]
        tokens: set[str] = set()
        for v in values:
            tokens.update(tokenize(v))
        for q in query:
            if q in tokens:
                score += weight
                why.append(f"{field_name} matches '{q}'")
            elif len(q) >= 3 and any(t.startswith(q) for t in tokens):
                score += weight / 2
                why.append(f"{field_name} partially matches '{q}'")
    return score, why
