import re
import time
from typing import Callable

import pytest
from redis import Redis
from redis.exceptions import ConnectionError as RedisConnectionError

from queuify.const import TASK_DONE_TOO_MANY_TIMES_ERROR_MSG
from queuify.exceptions import QueueEmpty, QueueFull
from queuify.redis import RedisQueue
from queuify.utils import timeout

MAXSIZE = 5


@pytest.fixture
def redis_client():
    conn_str = "redis://localhost:6379/0"
    with Redis.from_url(conn_str, decode_responses=True) as client:
        yield client


@pytest.fixture
def redis_queue(redis_client: Redis):
    queue = RedisQueue(client=redis_client, queue_name="queue1", maxsize=MAXSIZE)
    yield queue
    queue.delete()


@pytest.fixture
def redis_queue_no_maxsize(redis_client: Redis):
    queue = RedisQueue(client=redis_client, queue_name="queue2")
    yield queue
    queue.delete()


def test_put_and_get(redis_queue: RedisQueue[str], random_message: str):
    redis_queue.put(random_message)
    assert redis_queue.qsize() == 1
    message = redis_queue.get()
    assert message == random_message
    redis_queue.task_done()
    assert redis_queue.qsize() == 0


@pytest.mark.parametrize("count", range(1, MAXSIZE + 1))
def test_multiple_put_and_get(redis_queue: RedisQueue[str], get_random_message: Callable[[], str], count: int):
    messages = [get_random_message() for _ in range(count)]
    for message in messages:
        redis_queue.put(message)
    assert redis_queue.qsize() == count
    for _ in range(count):
        message = redis_queue.get()
        assert message == messages.pop(0)
        redis_queue.task_done()
    assert redis_queue.qsize() == 0


def test_put_and_get_with_delays(redis_queue: RedisQueue[str]):
    for i in range(MAXSIZE):
        redis_queue.put(f"message{i}")
        time.sleep(0.1)
    assert redis_queue.qsize() == MAXSIZE
    for i in range(MAXSIZE):
        message = redis_queue.get()
        assert message == f"message{i}"
        redis_queue.task_done()
        time.sleep(0.1)
    assert redis_queue.qsize() == 0


def test_put_nowait_and_get_nowait(redis_queue: RedisQueue[str], random_message: str):
    redis_queue.put_nowait(random_message)
    assert redis_queue.qsize() == 1
    message = redis_queue.get_nowait()
    assert message == random_message
    redis_queue.task_done()
    assert redis_queue.qsize() == 0


@pytest.mark.parametrize("count", range(1, MAXSIZE + 1))
def test_multiple_put_nowait_and_get_nowait(redis_queue: RedisQueue[str], get_random_message: Callable[[], str], count: int):
    messages = [get_random_message() for _ in range(count)]
    for message in messages:
        redis_queue.put_nowait(message)
    assert redis_queue.qsize() == count
    for _ in range(count):
        message = redis_queue.get_nowait()
        assert message == messages.pop(0)
        redis_queue.task_done()
    assert redis_queue.qsize() == 0


@pytest.mark.parametrize("count", range(1, MAXSIZE + 1))
def test_qsize(redis_queue: RedisQueue[str], count: int):
    assert redis_queue.qsize() == 0
    for i in range(count):
        redis_queue.put("message")
        assert redis_queue.qsize() == i + 1
    for i in range(count):
        redis_queue.get()
        redis_queue.task_done()
        assert redis_queue.qsize() == count - i - 1


def test_empty(redis_queue: RedisQueue[str]):
    assert redis_queue.empty() is True
    redis_queue.put("message")
    assert redis_queue.empty() is False
    message = redis_queue.get_nowait()
    redis_queue.task_done()
    assert message == "message"
    assert redis_queue.empty() is True


def test_get_nowait_empty_queue(redis_queue: RedisQueue[str]):
    with pytest.raises(QueueEmpty):
        redis_queue.get_nowait()


def test_full(redis_queue: RedisQueue[str]):
    for i in range(MAXSIZE):
        redis_queue.put(f"message{i}")
    assert redis_queue.full() is True
    with pytest.raises(QueueFull):
        redis_queue.put_nowait("message_overflow")
    for i in range(MAXSIZE):
        message = redis_queue.get()
        assert message == f"message{i}"
        redis_queue.task_done()
        assert redis_queue.full() is False


def test_has_unfinished_tasks(redis_queue: RedisQueue[str]):
    assert redis_queue._has_unfinished_tasks() is False
    redis_queue.put("message")
    assert redis_queue._has_unfinished_tasks() is True
    redis_queue.get()
    assert redis_queue._has_unfinished_tasks() is True
    redis_queue.task_done()
    assert redis_queue._has_unfinished_tasks() is False


def test_task_done_without_get(redis_queue: RedisQueue[str]):
    with pytest.raises(ValueError):
        redis_queue.task_done()


def test_task_done_too_many_times(redis_queue: RedisQueue[str]):
    redis_queue.put("message")
    redis_queue.get()
    redis_queue.task_done()
    with pytest.raises(ValueError, match=re.escape(TASK_DONE_TOO_MANY_TIMES_ERROR_MSG)):
        redis_queue.task_done()


def test_join(redis_queue: RedisQueue[str]):
    redis_queue.put("message")
    redis_queue.get()
    redis_queue.task_done()
    redis_queue.join()
    assert redis_queue.qsize() == 0


def test_delete(redis_queue: RedisQueue[str]):
    redis_queue.put("message")
    redis_queue.delete()
    assert redis_queue.qsize() == 0
    assert redis_queue.empty() is True


def test_join_empty_queue(redis_queue: RedisQueue[str]):
    redis_queue.join()
    assert redis_queue.qsize() == 0


def test_join_with_multiple_tasks(redis_queue: RedisQueue[str]):
    for i in range(5):
        redis_queue.put(f"message{i}")
    for i in range(5):
        redis_queue.get()
        redis_queue.task_done()
    redis_queue.join()
    assert redis_queue.qsize() == 0


def test_join_non_empty_queue(redis_queue: RedisQueue[str]):
    redis_queue.put("message")
    assert redis_queue.qsize() == 1
    assert redis_queue._has_unfinished_tasks() is True
    with pytest.raises((TimeoutError, RedisConnectionError)):
        with timeout(3):
            redis_queue.join()
    assert redis_queue._has_unfinished_tasks() is True


def test_put_to_full_queue(redis_queue: RedisQueue[str], random_message: str):
    for _ in range(redis_queue.maxsize):
        redis_queue.put(random_message)
    assert redis_queue.full() is True
    with pytest.raises((TimeoutError, RedisConnectionError)):
        with timeout(3):
            redis_queue.put("message_overflow")
    assert redis_queue.full() is True


def test_get_from_empty_queue(redis_queue: RedisQueue[str]):
    assert redis_queue.empty() is True
    with pytest.raises((TimeoutError, RedisConnectionError)):
        with timeout(3):
            redis_queue.get()
    assert redis_queue.empty() is True


@pytest.mark.parametrize("timeout", [0.5, 1, 2, 5])
def test_put_with_timeout(redis_queue: RedisQueue[str], random_message: str, timeout: float):
    for _ in range(redis_queue.maxsize):
        redis_queue.put(random_message)
    start_time = time.monotonic()
    with pytest.raises(QueueFull):
        redis_queue.put("message_overflow", timeout=timeout)
    end_time = time.monotonic()
    assert end_time - start_time == pytest.approx(timeout, abs=0.15)


@pytest.mark.parametrize("timeout", [0.5, 1, 2, 5])
def test_get_with_timeout(redis_queue: RedisQueue[str], timeout: float):
    assert redis_queue.empty() is True
    start_time = time.monotonic()
    with pytest.raises(QueueEmpty):
        redis_queue.get(timeout=timeout)
    end_time = time.monotonic()
    assert end_time - start_time == pytest.approx(timeout, abs=0.15)


@pytest.mark.parametrize("count", [50, 100, 500])
def test_put_and_get_unlimited_queue(redis_queue_no_maxsize: RedisQueue[str], count: int):
    for i in range(count):
        redis_queue_no_maxsize.put(f"message{i}")
    assert redis_queue_no_maxsize.qsize() == count
    for i in range(count):
        message = redis_queue_no_maxsize.get()
        assert message == f"message{i}"
        redis_queue_no_maxsize.task_done()
    assert redis_queue_no_maxsize.qsize() == 0
