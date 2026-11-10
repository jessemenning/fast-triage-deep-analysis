import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from src.config import get_settings
from src.routes.mqtt_proxy import router as mqtt_proxy_router
from src.routes.faa import router as faa_router
from src.services.feldera_client import get_feldera_client
from src.services.solace_receiver import init_receiver, stop_receiver

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    init_receiver(settings.solace_broker_props)
    logger.info("Solace receiver started")
    yield
    stop_receiver()
    logger.info("Solace receiver stopped")


app = FastAPI(
    title="arrival-capacity API",
    description="FAA arrival-capacity context queries — Continuum data plane",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(faa_router)
# WebSocket proxy must be registered before the static mount so Starlette
# routes it here rather than trying to serve a static file.
app.include_router(mqtt_proxy_router)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/ready")
async def ready():
    try:
        status = await get_feldera_client().deployment_status()
        if status == "Running":
            return {"status": "ready", "pipeline": status}
        return {"status": "not ready", "pipeline": status}, 503
    except Exception as exc:
        return {"status": "not ready", "detail": str(exc)}, 503


# Serve the demo UI at the root so http://localhost:8080/demo.html works (the demo
# runs standalone, not behind an ALB). Mounted LAST as the catch-all: the API routers
# (/api/faa/*, /health, /ready) and the WS proxy (/fx/mqtt) are registered above and
# win over this mount. The browser MQTT client still connects to /fx/mqtt (that route
# is unaffected by where the static files mount).
app.mount("/", StaticFiles(directory="static", html=True), name="static")
