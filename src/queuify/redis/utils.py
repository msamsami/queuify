from __future__ import annotations

import pathlib
import uuid
from typing import Any

from redis import Redis

from queuify.const import SEMAPHORE_TOKEN

from .enums import RedisOperation


def get_lua_script(operation: RedisOperation) -> str:
    """Get the lua script for the given operation.

    Args:
        operation (RedisOperation): Name of the operation to get the lua script for.

    Returns:
        str: The lua script for the given operation.
    """
    path = (pathlib.Path(__file__).resolve().parent / "scripts") / f"{operation.value.lower()}.lua"
    with open(path, "r") as f:
        return f.read()


def initialize_queue(
    client: Redis[Any],
    key: str,
    semaphore_key: str,
    semaphore_lock_key: str,
    unfinished_tasks_key: str,
    maxsize: int,
) -> None:
    """Initialize a queue and an optional semaphore queue in the Redis database (synchronous).

    If a `maxsize` greater than 0 is provided, a semaphore queue is also initialized with pre-filled tokens. It will be created if it does not exist.

    Args:
        client (Redis[Any]): The redis client (synchronous).
        key (str): The name of the Redis key for the queue.
        semaphore_key (str): The name of the Redis semaphore key for the queue.
        semaphore_lock_key (str): The name of the Redis semaphore lock key.
        unfinished_tasks_key (str): The name of the Redis key for the number of unfinished tasks in the queue.
        maxsize (int): The maximum number of tokens (messages) to initialize in the semaphore queue. If maxsize<=0, no semaphore is created.
    """
    if maxsize > 0:
        unique_lock_value = str(uuid.uuid4())
        lock_acquired = client.set(semaphore_lock_key, unique_lock_value, nx=True, ex=30)
        if lock_acquired:
            try:
                client.eval(get_lua_script(RedisOperation.initialize), 2, key, semaphore_key, maxsize, SEMAPHORE_TOKEN)  # type: ignore [no-untyped-call]
            finally:
                lock_value = client.get(semaphore_lock_key)
                if lock_value:
                    lock_value_str = lock_value.decode() if isinstance(lock_value, bytes) else str(lock_value)
                    if lock_value_str == unique_lock_value:
                        client.delete(semaphore_lock_key)
        else:
            token = client.brpop([semaphore_key])
            if token:
                client.rpush(semaphore_key, token[1])

    client.setnx(unfinished_tasks_key, 0)
