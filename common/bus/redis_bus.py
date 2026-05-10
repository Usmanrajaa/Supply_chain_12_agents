"""Redis Streams event bus."""
from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable

import redis.asyncio as redis
import structlog

from common.config.settings import get_settings
from common.events.schemas import EVENT_STREAMS, BaseEvent, EventType

logger = structlog.get_logger(__name__)
settings = get_settings()


def _stream_name(event_type: EventType) -> str:
    base = EVENT_STREAMS[event_type]
    return f"{settings.event_stream_prefix}.{base}"


def _serialize(event: BaseEvent) -> dict[str, str]:
    payload = event.model_dump(mode="json")
    return {"data": json.dumps(payload)}


def _deserialize(raw: dict[bytes, bytes], event_cls: type[BaseEvent]) -> BaseEvent:
    payload = json.loads(raw[b"data"].decode())
    return event_cls.model_validate(payload)


class EventBus:
    def __init__(self, url: str | None = None):
        self._url = url or settings.redis_url
        self._redis: redis.Redis | None = None

    async def connect(self) -> None:
        if self._redis is None:
            self._redis = redis.from_url(self._url, decode_responses=False)
            await self._redis.ping()
            logger.info("event_bus_connected", url=self._url)

    async def close(self) -> None:
        if self._redis:
            await self._redis.aclose()
            self._redis = None

    @property
    def client(self) -> redis.Redis:
        if self._redis is None:
            raise RuntimeError("EventBus not connected. Call connect() first.")
        return self._redis

    async def publish(self, event: BaseEvent) -> str:
        stream = _stream_name(event.event_type)
        msg_id = await self.client.xadd(stream, _serialize(event))
        result = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
        logger.info(
            "event_published",
            stream=stream,
            event_type=event.event_type,
            correlation_id=str(event.correlation_id),
            msg_id=result,
        )
        return result

    async def ensure_group(self, event_type: EventType, group: str) -> None:
        stream = _stream_name(event_type)
        try:
            await self.client.xgroup_create(stream, group, id="0", mkstream=True)
            logger.info("consumer_group_created", stream=stream, group=group)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def consume(
        self,
        event_type: EventType,
        event_cls: type[BaseEvent],
        group: str,
        consumer: str,
        handler: Callable[[BaseEvent], Awaitable[None]],
        block_ms: int = 5000,
    ) -> None:
        stream = _stream_name(event_type)
        await self.ensure_group(event_type, group)
        log = logger.bind(stream=stream, group=group, consumer=consumer)
        log.info("consumer_started")

        while True:
            try:
                resp = await self.client.xreadgroup(
                    group, consumer, {stream: ">"}, count=10, block=block_ms
                )
            except asyncio.CancelledError:
                log.info("consumer_cancelled")
                raise
            except Exception as e:
                log.exception("xreadgroup_failed", error=str(e))
                await asyncio.sleep(1)
                continue

            if not resp:
                continue

            for _stream_key, messages in resp:
                for msg_id, raw in messages:
                    try:
                        event = _deserialize(raw, event_cls)
                        await handler(event)
                        await self.client.xack(stream, group, msg_id)
                    except Exception as e:
                        log.exception(
                            "handler_failed", msg_id=msg_id.decode(), error=str(e)
                        )


_bus_singleton: EventBus | None = None


async def get_bus() -> EventBus:
    global _bus_singleton
    if _bus_singleton is None:
        _bus_singleton = EventBus()
        await _bus_singleton.connect()
    return _bus_singleton
