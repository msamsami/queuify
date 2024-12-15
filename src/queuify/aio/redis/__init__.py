from typing import Optional, TypeVar, cast

import redis.exceptions
from redis.typing import FieldT

from queuify.const import (
    NO_REMAINING_TASK_MSG,
    QUEUE_DELETION_MSG,
    SEMAPHORE_TOKEN,
    TASK_DONE_TOO_MANY_TIMES_ERROR_MSG,
    TIMEOUT_NEGATIVE_ERROR_MSG,
)
from queuify.exceptions import QueueEmpty, QueueFull
from queuify.redis._enums import RedisOperation

from .base import BaseAsyncRedisQueue

T = TypeVar("T", bound=FieldT)

__all__ = ("RedisQueue",)


class RedisQueue(BaseAsyncRedisQueue[T]):
    async def _has_unfinished_tasks(self) -> bool:
        """
        Check if there are any unfinished tasks in the queue.
        """
        remaining = await self.client.get(self._unfinished_tasks_key)
        if remaining is None or int(remaining) <= 0:
            return False
        return True

    async def task_done(self) -> None:
        await self._ensure_initialized()
        try:
            await self._scripts[RedisOperation.task_done](
                keys=[self._unfinished_tasks_key, self._join_channel_key],
                args=[NO_REMAINING_TASK_MSG, TASK_DONE_TOO_MANY_TIMES_ERROR_MSG],
            )
        except redis.exceptions.ResponseError as e:
            if TASK_DONE_TOO_MANY_TIMES_ERROR_MSG in str(e):
                raise ValueError(TASK_DONE_TOO_MANY_TIMES_ERROR_MSG)
            else:
                raise

    async def join(self) -> None:
        await self._ensure_initialized()
        if not await self._has_unfinished_tasks():
            return
        async with self.client.pubsub() as pubsub:
            await pubsub.subscribe(self._join_channel_key)
            try:
                if not await self._has_unfinished_tasks():
                    return
                async for message in pubsub.listen():
                    if message["type"] != "message":
                        continue
                    if self._to_str(message["channel"]) != self._join_channel_key:
                        continue
                    if self._to_str(message["data"]) in (NO_REMAINING_TASK_MSG, QUEUE_DELETION_MSG):
                        break
            finally:
                await pubsub.unsubscribe(self._join_channel_key)

    async def qsize(self) -> int:
        await self._ensure_initialized()
        return await self.client.llen(self._key)

    async def empty(self) -> bool:
        return not (await self.qsize())

    async def full(self) -> bool:
        if self._maxsize <= 0:
            return False
        else:
            return (await self.qsize()) >= self._maxsize

    async def put(self, item: T, timeout: Optional[float] = None) -> None:
        await self._ensure_initialized()
        if timeout and timeout < 0:
            raise ValueError(TIMEOUT_NEGATIVE_ERROR_MSG)
        token = None
        if self._maxsize > 0:
            token = await self.client.blpop([self._semaphore_key], timeout=timeout)
            if token is None:
                raise QueueFull
        try:
            await self._scripts[RedisOperation.put](
                keys=[self._key, self._unfinished_tasks_key],
                args=[item],
            )
        except Exception:
            if token is not None:
                await self.client.lpush(self._semaphore_key, token[1])
            raise

    async def get(self, timeout: Optional[float] = None) -> T:
        await self._ensure_initialized()
        if timeout and timeout < 0:
            raise ValueError(TIMEOUT_NEGATIVE_ERROR_MSG)
        item = await self.client.brpop([self._key], timeout=timeout)
        if item is None:
            raise QueueEmpty
        if self._maxsize > 0:
            await self.client.lpush(self._semaphore_key, SEMAPHORE_TOKEN)
        return cast(T, item[1])

    async def put_nowait(self, item: T) -> None:
        await self._ensure_initialized()
        result = await self._scripts[RedisOperation.put_nowait](
            keys=[self._key, self._unfinished_tasks_key], args=[item, str(self._maxsize)]
        )
        if int(result) == 1:
            return
        else:
            raise QueueFull

    async def get_nowait(self) -> T:
        await self._ensure_initialized()
        error_msg = "QueueEmpty"
        try:
            item = await self._scripts[RedisOperation.get_nowait](
                keys=[self._key, self._semaphore_key], args=[str(self._maxsize), SEMAPHORE_TOKEN, error_msg]
            )
        except redis.exceptions.ResponseError as e:
            if error_msg in str(e):
                raise QueueEmpty
            else:
                raise
        return cast(T, item)

    def __repr__(self) -> str:
        return f"RedisQueue<(queue_name='{self._queue_name}', maxsize={self._maxsize})>"
