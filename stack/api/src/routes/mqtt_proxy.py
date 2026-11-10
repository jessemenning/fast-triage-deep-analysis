import asyncio
import logging

import websockets
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from src.config import get_settings

router = APIRouter()
logger = logging.getLogger(__name__)


@router.websocket("/fx/mqtt")
async def mqtt_proxy(client_ws: WebSocket):
    """Proxy MQTT-over-WebSocket from browser to the Solace broker.

    The ALB only routes HTTP(S) traffic to port 8080, so the browser cannot
    reach the broker's MQTT-WS port (8008) directly.  This endpoint bridges
    the two using the 'mqtt' subprotocol that mqtt.js expects.
    """
    settings = get_settings()
    requested = client_ws.headers.get("sec-websocket-protocol", "")
    subprotocol = "mqtt" if "mqtt" in [s.strip() for s in requested.split(",")] else None
    await client_ws.accept(subprotocol=subprotocol)

    broker_url = settings.solace_mqtt_ws_url
    try:
        async with websockets.connect(
            broker_url,
            subprotocols=["mqtt"],
            ping_interval=None,  # let the MQTT keep-alive handle pings
        ) as broker_ws:
            async def client_to_broker():
                async for data in client_ws.iter_bytes():
                    await broker_ws.send(data)

            async def broker_to_client():
                async for data in broker_ws:
                    payload = data if isinstance(data, bytes) else data.encode()
                    await client_ws.send_bytes(payload)

            tasks = [
                asyncio.create_task(client_to_broker()),
                asyncio.create_task(broker_to_client()),
            ]
            _, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        logger.debug("mqtt_proxy closed: %s", exc)
        try:
            await client_ws.close()
        except Exception:
            pass
