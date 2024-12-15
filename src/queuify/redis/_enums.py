from enum import Enum


class RedisOperation(str, Enum):
    """
    Names of operations used by the Redis queue.
    """

    put = "put"
    put_nowait = "put_nowait"
    get_nowait = "get_nowait"
    task_done = "task_done"
    initialize = "initialize"
