import asyncio
import re
from typing import Callable

import async_timeout
import pytest
import pytest_asyncio
from redis.asyncio import Redis

from queuify.aio.redis import RedisQueue
from queuify.const import TASK_DONE_TOO_MANY_TIMES_ERROR_MSG
from queuify.exceptions import QueueEmpty, QueueFull

MAXSIZE = 5


@pytest_asyncio.fixture
async def redis_client():
    conn_str = "redis://localhost:6379/0"
    async with Redis.from_url(conn_str, decode_responses=True) as client:
        yield client


@pytest_asyncio.fixture
async def redis_queue(redis_client: Redis):
    async with RedisQueue(client=redis_client, queue_name="queue1", maxsize=MAXSIZE) as queue:
        yield queue
        await queue.delete()


@pytest_asyncio.fixture
async def redis_queue_no_maxsize(redis_client: Redis):
    async with RedisQueue(client=redis_client, queue_name="queue2") as queue:
        yield queue
        await queue.delete()


@pytest.mark.asyncio
async def test_put_and_get(redis_queue: RedisQueue, random_message: str):
    await redis_queue.put(random_message)
    assert await redis_queue.qsize() == 1
    message = await redis_queue.get()
    assert message == random_message
    await redis_queue.task_done()
    assert await redis_queue.qsize() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("count", range(1, MAXSIZE + 1))
async def test_multiple_put_and_get(redis_queue: RedisQueue, get_random_message: Callable[[], str], count: int):
    messages = [get_random_message() for _ in range(count)]
    for message in messages:
        await redis_queue.put(message)
    assert await redis_queue.qsize() == count
    for _ in range(count):
        message = await redis_queue.get()
        assert message == messages.pop(0)
        await redis_queue.task_done()
    assert await redis_queue.qsize() == 0


@pytest.mark.asyncio
async def test_put_and_get_with_delays(redis_queue: RedisQueue):
    for i in range(MAXSIZE):
        await redis_queue.put(f"message{i}")
        await asyncio.sleep(0.1)
    assert await redis_queue.qsize() == MAXSIZE
    for i in range(MAXSIZE):
        message = await redis_queue.get()
        assert message == f"message{i}"
        await redis_queue.task_done()
        await asyncio.sleep(0.1)
    assert await redis_queue.qsize() == 0


@pytest.mark.asyncio
async def test_put_nowait_and_get_nowait(redis_queue: RedisQueue, random_message: str):
    await redis_queue.put_nowait(random_message)
    assert await redis_queue.qsize() == 1
    message = await redis_queue.get_nowait()
    assert message == random_message
    await redis_queue.task_done()
    assert await redis_queue.qsize() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("count", range(1, MAXSIZE + 1))
async def test_multiple_put_nowait_and_get_nowait(redis_queue: RedisQueue, get_random_message: Callable[[], str], count: int):
    messages = [get_random_message() for _ in range(count)]
    for message in messages:
        await redis_queue.put_nowait(message)
    assert await redis_queue.qsize() == count
    for _ in range(count):
        message = await redis_queue.get_nowait()
        assert message == messages.pop(0)
        await redis_queue.task_done()
    assert await redis_queue.qsize() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("count", range(1, MAXSIZE + 1))
async def test_qsize(redis_queue: RedisQueue, count: int):
    assert await redis_queue.qsize() == 0
    for i in range(count):
        await redis_queue.put("message")
        assert await redis_queue.qsize() == i + 1
    for i in range(count):
        await redis_queue.get()
        await redis_queue.task_done()
        assert await redis_queue.qsize() == count - i - 1


@pytest.mark.asyncio
async def test_empty(redis_queue: RedisQueue):
    assert await redis_queue.empty() is True
    await redis_queue.put("message")
    assert await redis_queue.empty() is False
    message = await redis_queue.get_nowait()
    await redis_queue.task_done()
    assert message == "message"
    assert await redis_queue.empty() is True


@pytest.mark.asyncio
async def test_get_nowait_empty_queue(redis_queue: RedisQueue):
    with pytest.raises(QueueEmpty):
        await redis_queue.get_nowait()


@pytest.mark.asyncio
async def test_full(redis_queue: RedisQueue):
    for i in range(MAXSIZE):
        await redis_queue.put(f"message{i}")
    assert await redis_queue.full() is True
    with pytest.raises(QueueFull):
        await redis_queue.put_nowait("message_overflow")
    for i in range(MAXSIZE):
        message = await redis_queue.get()
        assert message == f"message{i}"
        await redis_queue.task_done()
        assert await redis_queue.full() is False


@pytest.mark.asyncio
async def test_has_unfinished_tasks(redis_queue: RedisQueue):
    assert await redis_queue._has_unfinished_tasks() is False
    await redis_queue.put("message")
    assert await redis_queue._has_unfinished_tasks() is True
    await redis_queue.get()
    assert await redis_queue._has_unfinished_tasks() is True
    await redis_queue.task_done()
    assert await redis_queue._has_unfinished_tasks() is False


@pytest.mark.asyncio
async def test_task_done_without_get(redis_queue: RedisQueue):
    with pytest.raises(ValueError):
        await redis_queue.task_done()


@pytest.mark.asyncio
async def test_task_done_too_many_times(redis_queue: RedisQueue):
    await redis_queue.put("message")
    await redis_queue.get()
    await redis_queue.task_done()
    with pytest.raises(ValueError, match=re.escape(TASK_DONE_TOO_MANY_TIMES_ERROR_MSG)):
        await redis_queue.task_done()


@pytest.mark.asyncio
async def test_join(redis_queue: RedisQueue):
    await redis_queue.put("message")
    await redis_queue.get()
    await redis_queue.task_done()
    await redis_queue.join()
    assert await redis_queue.qsize() == 0


@pytest.mark.asyncio
async def test_delete(redis_queue: RedisQueue):
    await redis_queue.put("message")
    await redis_queue.delete()
    assert await redis_queue.qsize() == 0
    assert await redis_queue.empty() is True


@pytest.mark.asyncio
async def test_join_empty_queue(redis_queue: RedisQueue):
    await redis_queue.join()
    assert await redis_queue.qsize() == 0


@pytest.mark.asyncio
async def test_join_with_multiple_tasks(redis_queue: RedisQueue):
    for i in range(5):
        await redis_queue.put(f"message{i}")
    for i in range(5):
        await redis_queue.get()
        await redis_queue.task_done()
    await redis_queue.join()
    assert await redis_queue.qsize() == 0


@pytest.mark.asyncio
async def test_join_non_empty_queue(redis_queue: RedisQueue):
    await redis_queue.put("message")
    assert await redis_queue.qsize() == 1
    assert await redis_queue._has_unfinished_tasks() is True
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(3):
            await redis_queue.join()


@pytest.mark.asyncio
async def test_put_to_full_queue(redis_queue: RedisQueue, random_message: str):
    for i in range(redis_queue.maxsize):
        await redis_queue.put(random_message)
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(3):
            await redis_queue.put("message_overflow")


@pytest.mark.asyncio
async def test_get_from_empty_queue(redis_queue: RedisQueue):
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(3):
            await redis_queue.get()


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [50, 100, 500])
async def test_put_and_get_unlimited_queue(redis_queue_no_maxsize: RedisQueue, count: int):
    for i in range(count):
        await redis_queue_no_maxsize.put(f"message{i}")
    assert await redis_queue_no_maxsize.qsize() == count
    for i in range(count):
        message = await redis_queue_no_maxsize.get()
        assert message == f"message{i}"
        await redis_queue_no_maxsize.task_done()
    assert await redis_queue_no_maxsize.qsize() == 0
