import asyncio
import os
import re
import time
from sqlite3 import OperationalError
from typing import Callable

import async_timeout
import pytest
import pytest_asyncio

from queuify.aio.disk import DiskQueue
from queuify.const import TASK_DONE_TOO_MANY_TIMES_ERROR_MSG
from queuify.exceptions import QueueEmpty, QueueFull

MAXSIZE = 5


@pytest_asyncio.fixture
async def disk_queue(temp_dir: str):
    file_path = os.path.join(temp_dir, "some.queue")
    async with DiskQueue(file_path=file_path, queue_name="main", maxsize=MAXSIZE) as queue:
        yield queue
        await queue.delete()


@pytest_asyncio.fixture
async def disk_queue_no_maxsize(temp_dir: str):
    file_path = os.path.join(temp_dir, "other.queue")
    async with DiskQueue(file_path=file_path, queue_name="main") as queue:
        yield queue


@pytest.mark.asyncio
async def test_put_and_get(disk_queue: DiskQueue[str], random_message: str):
    await disk_queue.put(random_message)
    assert await disk_queue.qsize() == 1
    message = await disk_queue.get()
    assert message == random_message
    await disk_queue.task_done()
    assert await disk_queue.qsize() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("count", range(1, MAXSIZE + 1))
async def test_multiple_put_and_get(disk_queue: DiskQueue[str], get_random_message: Callable[[], str], count: int):
    messages = [get_random_message() for _ in range(count)]
    for message in messages:
        await disk_queue.put(message)
    assert await disk_queue.qsize() == count
    for _ in range(count):
        message = await disk_queue.get()
        assert message == messages.pop(0)
        await disk_queue.task_done()
    assert await disk_queue.qsize() == 0


@pytest.mark.asyncio
async def test_put_and_get_with_delays(disk_queue: DiskQueue[str]):
    for i in range(MAXSIZE):
        await disk_queue.put(f"message{i}")
        await asyncio.sleep(0.1)
    assert await disk_queue.qsize() == MAXSIZE
    for i in range(MAXSIZE):
        message = await disk_queue.get()
        assert message == f"message{i}"
        await disk_queue.task_done()
        await asyncio.sleep(0.1)
    assert await disk_queue.qsize() == 0


@pytest.mark.asyncio
async def test_put_nowait_and_get_nowait(disk_queue: DiskQueue[str], random_message: str):
    await disk_queue.put_nowait(random_message)
    assert await disk_queue.qsize() == 1
    message = await disk_queue.get_nowait()
    assert message == random_message
    await disk_queue.task_done()
    assert await disk_queue.qsize() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("count", range(1, MAXSIZE + 1))
async def test_multiple_put_nowait_and_get_nowait(disk_queue: DiskQueue[str], get_random_message: Callable[[], str], count: int):
    messages = [get_random_message() for _ in range(count)]
    for message in messages:
        await disk_queue.put_nowait(message)
    assert await disk_queue.qsize() == count
    for _ in range(count):
        message = await disk_queue.get_nowait()
        assert message == messages.pop(0)
        await disk_queue.task_done()
    assert await disk_queue.qsize() == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("count", range(1, MAXSIZE + 1))
async def test_qsize(disk_queue: DiskQueue[str], count: int):
    assert await disk_queue.qsize() == 0
    for i in range(count):
        await disk_queue.put("message")
        assert await disk_queue.qsize() == i + 1
    for i in range(count):
        await disk_queue.get()
        await disk_queue.task_done()
        assert await disk_queue.qsize() == count - i - 1


@pytest.mark.asyncio
async def test_empty(disk_queue: DiskQueue[str]):
    assert await disk_queue.empty() is True
    await disk_queue.put("message")
    assert await disk_queue.empty() is False
    message = await disk_queue.get_nowait()
    await disk_queue.task_done()
    assert message == "message"
    assert await disk_queue.empty() is True


@pytest.mark.asyncio
async def test_get_nowait_empty_queue(disk_queue: DiskQueue[str]):
    with pytest.raises(QueueEmpty):
        await disk_queue.get_nowait()


@pytest.mark.asyncio
async def test_full(disk_queue: DiskQueue[str]):
    for i in range(MAXSIZE):
        await disk_queue.put(f"message{i}")
    assert await disk_queue.full() is True
    with pytest.raises(QueueFull):
        await disk_queue.put_nowait("message_overflow")
    for i in range(MAXSIZE):
        message = await disk_queue.get()
        assert message == f"message{i}"
        await disk_queue.task_done()
        assert await disk_queue.full() is False


@pytest.mark.asyncio
async def test_has_unfinished_tasks(disk_queue: DiskQueue[str]):
    assert await disk_queue._has_unfinished_tasks() is False
    await disk_queue.put("message")
    assert await disk_queue._has_unfinished_tasks() is True
    await disk_queue.get()
    assert await disk_queue._has_unfinished_tasks() is True
    await disk_queue.task_done()
    assert await disk_queue._has_unfinished_tasks() is False


@pytest.mark.asyncio
async def test_task_done_without_get(disk_queue: DiskQueue[str]):
    with pytest.raises(ValueError):
        await disk_queue.task_done()


@pytest.mark.asyncio
async def test_task_done_too_many_times(disk_queue: DiskQueue[str]):
    await disk_queue.put("message")
    await disk_queue.get()
    await disk_queue.task_done()
    with pytest.raises(ValueError, match=re.escape(TASK_DONE_TOO_MANY_TIMES_ERROR_MSG)):
        await disk_queue.task_done()


@pytest.mark.asyncio
async def test_join(disk_queue: DiskQueue[str]):
    await disk_queue.put("message")
    await disk_queue.get()
    await disk_queue.task_done()
    await disk_queue.join()
    assert await disk_queue.qsize() == 0


@pytest.mark.asyncio
async def test_delete(disk_queue: DiskQueue[str]):
    await disk_queue.put("message")
    await disk_queue.delete()
    with pytest.raises(OperationalError):
        await disk_queue.qsize()


@pytest.mark.asyncio
async def test_join_empty_queue(disk_queue: DiskQueue[str]):
    await disk_queue.join()
    assert await disk_queue.qsize() == 0


@pytest.mark.asyncio
async def test_join_with_multiple_tasks(disk_queue: DiskQueue[str]):
    for i in range(5):
        await disk_queue.put(f"message{i}")
    for i in range(5):
        await disk_queue.get()
        await disk_queue.task_done()
    await disk_queue.join()
    assert await disk_queue.qsize() == 0


@pytest.mark.asyncio
async def test_join_non_empty_queue(disk_queue: DiskQueue[str]):
    await disk_queue.put("message")
    assert await disk_queue.qsize() == 1
    assert await disk_queue._has_unfinished_tasks() is True
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(3):
            await disk_queue.join()
    assert await disk_queue._has_unfinished_tasks() is True


@pytest.mark.asyncio
async def test_put_to_full_queue(disk_queue: DiskQueue[str], random_message: str):
    for i in range(disk_queue.maxsize):
        await disk_queue.put(random_message)
    assert await disk_queue.full() is True
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(3):
            await disk_queue.put("message_overflow")
    assert await disk_queue.full() is True


@pytest.mark.asyncio
async def test_get_from_empty_queue(disk_queue: DiskQueue[str]):
    assert await disk_queue.empty() is True
    with pytest.raises(asyncio.TimeoutError):
        async with async_timeout.timeout(3):
            await disk_queue.get()
    assert await disk_queue.empty() is True


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [0.5, 1, 2, 5])
async def test_put_with_timeout(disk_queue: DiskQueue[str], random_message: str, timeout: float):
    for _ in range(disk_queue.maxsize):
        await disk_queue.put(random_message)
    start_time = time.monotonic()
    with pytest.raises(QueueFull):
        await disk_queue.put("message_overflow", timeout=timeout)
    end_time = time.monotonic()
    assert end_time - start_time == pytest.approx(timeout, abs=0.15)


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [0.5, 1, 2, 5])
async def test_get_with_timeout(disk_queue: DiskQueue[str], timeout: float):
    assert await disk_queue.empty() is True
    start_time = time.monotonic()
    with pytest.raises(QueueEmpty):
        await disk_queue.get(timeout=timeout)
    end_time = time.monotonic()
    assert end_time - start_time == pytest.approx(timeout, abs=0.15)


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [50, 100, 500])
async def test_put_and_get_unlimited_queue(disk_queue_no_maxsize: DiskQueue[str], count: int):
    for i in range(count):
        await disk_queue_no_maxsize.put(f"message{i}")
    assert await disk_queue_no_maxsize.qsize() == count
    for i in range(count):
        message = await disk_queue_no_maxsize.get()
        assert message == f"message{i}"
        await disk_queue_no_maxsize.task_done()
    assert await disk_queue_no_maxsize.qsize() == 0
