from functools import lru_cache
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # ── Feldera (Solace fork) ──────────────────────────────────────────────
    feldera_url: str = "http://feldera:8080"
    feldera_pipeline: str = "arrival-capacity"
    # Second pipeline, backing the 5-minute demo console (static/console.html).
    # Created and compiled by the init service but left STOPPED — the presenter
    # starts it on stage, so the console polls its status.
    watch_pipeline: str = "arrival-capacity-watch"

    log_level: str = "INFO"
    api_port: int = 8080

    # ── Continuum MCP (generation engine) — for the "add data product" proxy ──
    continuum_mcp_url: str = "http://continuum-mcp:8091/mcp"
    continuum_mcp_token: str = "continuum-dev-token-local"
    products_dir: str = "/app/products"   # read-only mount of demo/products/*.json (prewarmed specs)

    # ── Solace Platform ────────────────────────────────────────────────────
    solace_host: str = "tcp://solace:55555"
    solace_mqtt_ws_url: str = "ws://solace:8000/mqtt"
    solace_vpn: str = "default"
    solace_user: str = "default"
    solace_password: str = "default"

    @property
    def solace_broker_props(self) -> dict:
        return {
            "solace.messaging.transport.host": self.solace_host,
            "solace.messaging.service.vpn-name": self.solace_vpn,
            "solace.messaging.authentication.scheme.basic.username": self.solace_user,
            "solace.messaging.authentication.scheme.basic.password": self.solace_password,
        }

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


@lru_cache
def get_settings() -> Settings:
    return Settings()
