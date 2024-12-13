from __future__ import annotations

import asyncio
from typing import TypeVar, Union

from redis.asyncio import Redis
from redis.commands.core import AsyncScript
from redis.typing import FieldT

from queuify.aio.base import AsyncQueue
from queuify.const import QUEUE_DELETION_MSG
from queuify.redis.base import _BaseRedisQueue
from queuify.redis.enums import RedisOperation

from .utils import get_lua_script, initialize_queue

T = TypeVar("T", bound=FieldT)

__all__ = ("BaseAsyncRedisQueue",)


class BaseAsyncRedisQueue(_BaseRedisQueue, AsyncQueue[T]):
    def __init__(self, client: Redis[Union[str, bytes]], queue_name: str, maxsize: int = 0) -> None:
        super().__init__(queue_name, maxsize)
        self.client = client

        self._scripts: dict[RedisOperation, AsyncScript] = {}
        self._initialized: bool = False
        self._init_lock: asyncio.Lock = asyncio.Lock()

    async def _ensure_initialized(self) -> None:
        if not self._initialized or not self._scripts:
            async with self._init_lock:
                if not self._initialized or not self._scripts:
                    for operation in RedisOperation:
                        self._scripts[operation] = self.client.register_script(await get_lua_script(operation))
                    await initialize_queue(
                        self.client,
                        self._key,
                        self._semaphore_key,
                        self._semaphore_lock_key,
                        self._unfinished_tasks_key,
                        self._maxsize,
                    )
                    self._initialized = True

    async def delete(self) -> None:
        await self.client.publish(self._join_channel_key, QUEUE_DELETION_MSG)
        await self.client.delete(
            self._key, self._semaphore_key, self._semaphore_lock_key, self._unfinished_tasks_key, self._join_channel_key
        )

    async def __aenter__(self):
        await self._ensure_initialized()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
