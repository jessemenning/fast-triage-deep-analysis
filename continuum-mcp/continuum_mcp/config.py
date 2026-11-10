"""Runtime configuration, loaded from environment variables (and an optional .env)."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PACKAGE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = PACKAGE_ROOT.parent


class Settings(BaseSettings):
    """All knobs the server reads. Every value has a local-stack default."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # --- Continuum engine (REST) ------------------------------------------------
    feldera_url: str = Field("http://localhost:8081", alias="FELDERA_URL")
    # The engine's own address as seen from inside the docker network: used by the one-shot
    # snapshot connector (url_input) that a product pipeline runs against the base pipeline.
    feldera_internal_url: str = Field("http://feldera:8080", alias="FELDERA_INTERNAL_URL")
    base_pipeline: str = Field("arrival-capacity", alias="BASE_PIPELINE")
    compile_timeout_secs: int = Field(600, alias="COMPILE_TIMEOUT_SECS")
    poll_interval_secs: float = Field(5.0, alias="POLL_INTERVAL_SECS")
    default_workers: int = Field(2, alias="DEFAULT_WORKERS")
    # Feldera compilation profile for generated product pipelines. "unoptimized" compiles
    # in seconds-to-tens-of-seconds (vs ~minutes optimized) at the cost of runtime
    # throughput — the right trade for demo-volume, fast scenario generation. Set
    # FELDERA_COMPILATION_PROFILE=optimized to restore the production profile.
    compilation_profile: str = Field("unoptimized", alias="FELDERA_COMPILATION_PROFILE")

    # --- Broker management (SEMP v2) -------------------------------------------
    semp_url: str = Field("http://localhost:8089", alias="SEMP_URL")
    semp_user: str = Field("admin", alias="SEMP_USER")
    semp_pass: str = Field("admin", alias="SEMP_PASS")
    semp_vpn: str = Field("default", alias="SEMP_VPN")
    queue_max_ttl_secs: int = Field(300, alias="QUEUE_MAX_TTL_SECS")
    queue_max_spool_mb: int = Field(500, alias="QUEUE_MAX_SPOOL_MB")

    # --- Broker coordinates as the engine container sees them -------------------
    fl_solace_hostname: str = Field("solace", alias="FL_SOLACE_HOSTNAME")
    fl_solace_port: int = Field(55555, alias="FL_SOLACE_PORT")
    fl_solace_vpn: str = Field("default", alias="FL_SOLACE_VPN")
    fl_solace_user: str = Field("default", alias="FL_SOLACE_USER")
    fl_solace_password: str = Field("default", alias="FL_SOLACE_PASSWORD")

    # --- Event Portal -----------------------------------------------------------
    ep_base_url: str = Field(
        "https://api.solace.cloud/api/v2/architecture", alias="EP_BASE_URL"
    )
    solace_api_token: str | None = Field(None, alias="SOLACE_API_TOKEN")
    ep_domain: str = Field("FAA Arrival Capacity", alias="CONTINUUM_EP_DOMAIN")

    # --- MCP server -------------------------------------------------------------
    mcp_host: str = Field("0.0.0.0", alias="MCP_HOST")
    mcp_port: int = Field(8091, alias="MCP_PORT")
    mcp_token: str | None = Field(None, alias="CONTINUUM_MCP_TOKEN")
    # Comma-separated Host header values to accept (enables the MCP SDK's DNS-rebinding
    # protection). Empty = accept any Host, which is what docker-network clients need;
    # the bearer token is the access control.
    mcp_allowed_hosts: str = Field("", alias="MCP_ALLOWED_HOSTS")
    catalog_dir: Path = Field(PROJECT_ROOT / "catalog", alias="CATALOG_DIR")

    # Wire envelope of the engine's JSON sink. Confirmed on the live stack: `format: json`
    # publishes the insert_delete envelope {"insert": {...row...}} / {"delete": {...}}.
    # `bare` is kept as an escape hatch for builds that emit the row itself. Drives the
    # JSON Schemas, SAM event-rule snippets and the update_format of derived-stream inputs.
    trigger_envelope: Literal["bare", "insert"] = Field("insert", alias="TRIGGER_ENVELOPE")

    @property
    def ep_enabled(self) -> bool:
        return bool(self.solace_api_token)

    @property
    def allowed_hosts(self) -> list[str]:
        return [h.strip() for h in self.mcp_allowed_hosts.split(",") if h.strip()]

    @property
    def broker_for_engine(self) -> dict[str, str | int]:
        """Connector-JSON coordinates of the broker, as the engine container sees it."""
        return {
            "host": self.fl_solace_hostname,
            "port": self.fl_solace_port,
            "vpn": self.fl_solace_vpn,
            "username": self.fl_solace_user,
            "password": self.fl_solace_password,
        }


def load_settings(**overrides: object) -> Settings:
    return Settings(**overrides)  # type: ignore[arg-type]
