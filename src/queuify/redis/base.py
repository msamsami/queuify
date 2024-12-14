from __future__ import annotations

import threading
from abc import ABCMeta, abstractmethod
from typing import Awaitable, Optional, TypeVar, Union

from redis import Redis
from redis.commands.core import Script
from redis.typing import FieldT

from queuify.base import Queue
from queuify.const import QUEUE_DELETION_MSG

from .enums import RedisOperation
from .utils import get_lua_script, initialize_queue

T = TypeVar("T", bound=FieldT)

__all__ = ("BaseRedisQueue",)


class _BaseRedisQueue(metaclass=ABCMeta):
    namespace_prefix: str = "queuify:queue"

    def __init__(self, queue_name: str, maxsize: int = 0) -> None:
        self._queue_name = queue_name
        self._maxsize = maxsize

        self._key = f"{self.namespace_prefix}:{self._queue_name}"
        self._semaphore_key = f"{self._key}:semaphore"
        self._semaphore_lock_key = f"{self._semaphore_key}:lock"
        self._unfinished_tasks_key = f"{self._key}:unfinished_tasks"
        self._join_channel_key = f"{self._key}:join_channel"

    @staticmethod
    def _to_str(value: Union[str, bytes]) -> str:
        if isinstance(value, bytes):
            return value.decode()
        elif isinstance(value, str):
            return value
        else:
            raise TypeError(f"Expected str or bytes, got {type(value)}")

    @abstractmethod
    def delete(self) -> None | Awaitable[None]:
        """
        Delete the queue.
        """
        pass

    @property
    def maxsize(self) -> int:
        return self._maxsize

    @property
    def key(self) -> str:
        """
        Return the Redis key for this queue.
        """
        return self._key

    @property
    def semaphore_key(self) -> Optional[str]:
        """
        Return the Redis semaphore key for this queue.
        """
        return self._semaphore_key if self._maxsize > 0 else None

    @property
    def semaphore_lock_key(self) -> Optional[str]:
        """
        Return the Redis semaphore lock key for this queue.
        """
        return self._semaphore_lock_key if self._maxsize > 0 else None

    @property
    def unfinished_tasks_key(self) -> str:
        """
        Return the Redis key for the number of unfinished tasks in this queue.
        """
        return self._unfinished_tasks_key

    @property
    def join_channel_key(self) -> str:
        """
        Return the Redis key for the join channel for this queue.
        """
        return self._join_channel_key

    def __hash__(self):
        return hash(self._key)

    def __eq__(self, other):
        if not isinstance(other, _BaseRedisQueue):
            raise TypeError("Cannot compare Redis queues to other objects")
        return self._key == other._key


class BaseRedisQueue(_BaseRedisQueue, Queue[T]):
    def __init__(self, client: Redis[Union[str, bytes]], queue_name: str, maxsize: int = 0) -> None:
        super().__init__(queue_name, maxsize)
        self.client = client

        self._scripts: dict[RedisOperation, Script] = {}
        self._initialized: bool = False
        self._init_lock: threading.Lock = threading.Lock()
        self._ensure_initialized()

    def _ensure_initialized(self) -> None:
        if not self._initialized or not self._scripts:
            with self._init_lock:
                if not self._scripts:
                    for operation in RedisOperation:
                        self._scripts[operation] = self.client.register_script(get_lua_script(operation))
                if not self._initialized:
                    initialize_queue(
                        self.client,
                        self._key,
                        self._semaphore_key,
                        self._semaphore_lock_key,
                        self._unfinished_tasks_key,
                        self._maxsize,
                    )
                    self._initialized = True

    def delete(self) -> None:
        self.client.publish(self._join_channel_key, QUEUE_DELETION_MSG)
        self.client.delete(
            self._key, self._semaphore_key, self._semaphore_lock_key, self._unfinished_tasks_key, self._join_channel_key
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
