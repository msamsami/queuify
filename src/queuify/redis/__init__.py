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

from ._enums import RedisOperation
from .base import BaseRedisQueue

T = TypeVar("T", bound=FieldT)

__all__ = ("RedisQueue",)


class RedisQueue(BaseRedisQueue[T]):
    def _has_unfinished_tasks(self) -> bool:
        """
        Check if there are any unfinished tasks in the queue.
        """
        remaining = self.client.get(self._unfinished_tasks_key)
        if remaining is None or int(remaining) <= 0:
            return False
        return True

    def task_done(self) -> None:
        try:
            self._scripts[RedisOperation.task_done](
                keys=[self._unfinished_tasks_key, self._join_channel_key],
                args=[NO_REMAINING_TASK_MSG, TASK_DONE_TOO_MANY_TIMES_ERROR_MSG],
            )
        except redis.exceptions.ResponseError as e:
            if TASK_DONE_TOO_MANY_TIMES_ERROR_MSG in str(e):
                raise ValueError(TASK_DONE_TOO_MANY_TIMES_ERROR_MSG)
            else:
                raise

    def join(self) -> None:
        if not self._has_unfinished_tasks():
            return
        with self.client.pubsub() as pubsub:
            pubsub.subscribe(self._join_channel_key)
            try:
                if not self._has_unfinished_tasks():
                    return
                for message in pubsub.listen():  # type: ignore [no-untyped-call]
                    if message["type"] != "message":
                        continue
                    if self._to_str(message["channel"]) != self._join_channel_key:
                        continue
                    if self._to_str(message["data"]) in (NO_REMAINING_TASK_MSG, QUEUE_DELETION_MSG):
                        break
            finally:
                pubsub.unsubscribe(self._join_channel_key)

    def qsize(self) -> int:
        return self.client.llen(self._key)

    def empty(self) -> bool:
        return not self.qsize()

    def full(self) -> bool:
        if self._maxsize <= 0:
            return False
        else:
            return self.qsize() >= self._maxsize

    def put(self, item: T, timeout: Optional[float] = None) -> None:
        if timeout and timeout < 0:
            raise ValueError(TIMEOUT_NEGATIVE_ERROR_MSG)
        token = None
        if self._maxsize > 0:
            token = self.client.blpop([self._semaphore_key], timeout=timeout)
            if token is None:
                raise QueueFull
        try:
            self._scripts[RedisOperation.put](
                keys=[self._key, self._unfinished_tasks_key],
                args=[item],
            )
        except Exception:
            if token is not None:
                self.client.lpush(self._semaphore_key, self._to_str(token[1]))
            raise

    def get(self, timeout: Optional[float] = None) -> T:
        if timeout and timeout < 0:
            raise ValueError(TIMEOUT_NEGATIVE_ERROR_MSG)
        item = self.client.brpop([self._key], timeout=timeout)
        if item is None:
            raise QueueEmpty
        if self._maxsize > 0:
            self.client.lpush(self._semaphore_key, SEMAPHORE_TOKEN)
        return cast(T, item[1])

    def put_nowait(self, item: T) -> None:
        result = self._scripts[RedisOperation.put_nowait](
            keys=[self._key, self._unfinished_tasks_key], args=[item, str(self._maxsize)]
        )
        if int(result) == 1:
            return
        else:
            raise QueueFull

    def get_nowait(self) -> T:
        error_msg = "QueueEmpty"
        try:
            item = self._scripts[RedisOperation.get_nowait](
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
