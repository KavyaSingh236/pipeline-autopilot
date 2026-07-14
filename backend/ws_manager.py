"""WebSocket connection manager for real-time Control Tower updates."""
from __future__ import annotations
import asyncio
import json
from datetime import datetime, date
from decimal import Decimal
from typing import Any

from fastapi import WebSocket
import structlog

log = structlog.get_logger(__name__)


def _default(o: Any):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, Decimal):
        return float(o)
    return str(o)


class ConnectionManager:
    def __init__(self) -> None:
        self.active: list[WebSocket] = []
        self._lock = asyncio.Lock()

    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        async with self._lock:
            self.active.append(ws)
        log.info("ws_connected", clients=len(self.active))

    async def disconnect(self, ws: WebSocket) -> None:
        async with self._lock:
            if ws in self.active:
                self.active.remove(ws)
        log.info("ws_disconnected", clients=len(self.active))

    async def broadcast(self, event: str, payload: dict) -> None:
        message = json.dumps({"event": event, "payload": payload}, default=_default)
        dead: list[WebSocket] = []
        for ws in list(self.active):
            try:
                await ws.send_text(message)
            except Exception:
                dead.append(ws)
        for ws in dead:
            await self.disconnect(ws)


manager = ConnectionManager()
