"""Common scaffolding every agent uses."""
from __future__ import annotations

import asyncio
import signal
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable

import structlog

from common.bus.redis_bus import EventBus, get_bus
from common.events.schemas import BaseEvent, EventType

logger = structlog.get_logger(__name__)
EventHandler = Callable[[BaseEvent], Awaitable[None]]


class BaseAgent(ABC):
    name: str

    def __init__(self) -> None:
        self.bus: EventBus | None = None
        self._tasks: list[asyncio.Task] = []
        self._stopping = asyncio.Event()
        self.log = logger.bind(agent=self.name)

    @abstractmethod
    def subscriptions(self) -> dict[EventType, tuple[type[BaseEvent], EventHandler]]:
        ...

    async def setup(self) -> None:
        ...

    async def teardown(self) -> None:
        ...

    async def run(self) -> None:
        self.bus = await get_bus()
        await self.setup()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._stopping.set)
            except NotImplementedError:
                # Windows doesn't support add_signal_handler in some envs
                pass

        subs = self.subscriptions()
        if not subs:
            self.log.info("agent_started_no_subscriptions")
            await self._stopping.wait()
            await self.teardown()
            return

        for event_type, (schema_cls, handler) in subs.items():
            task = asyncio.create_task(
                self.bus.consume(
                    event_type=event_type,
                    event_cls=schema_cls,
                    group=self.name,
                    consumer=f"{self.name}-1",
                    handler=handler,
                ),
                name=f"{self.name}:{event_type}",
            )
            self._tasks.append(task)

        self.log.info("agent_started", subscriptions=list(subs.keys()))
        await self._stopping.wait()
        self.log.info("agent_stopping")

        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.teardown()
        self.log.info("agent_stopped")
