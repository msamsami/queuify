import os
import re
import time
from sqlite3 import OperationalError
from typing import Callable

import pytest

from queuify.const import TASK_DONE_TOO_MANY_TIMES_ERROR_MSG
from queuify.disk import DiskQueue
from queuify.exceptions import QueueEmpty, QueueFull
from queuify.utils import timeout

MAXSIZE = 5


@pytest.fixture
def disk_queue(temp_dir: str):
    file_path = os.path.join(temp_dir, "some.queue")
    with DiskQueue(file_path=file_path, queue_name="main", maxsize=MAXSIZE) as queue:
        yield queue
        queue.delete()


@pytest.fixture
def disk_queue_no_maxsize(temp_dir: str):
    file_path = os.path.join(temp_dir, "other.queue")
    with DiskQueue(file_path=file_path, queue_name="main") as queue:
        yield queue


def test_put_and_get(disk_queue: DiskQueue[str], random_message: str):
    disk_queue.put(random_message)
    assert disk_queue.qsize() == 1
    message = disk_queue.get()
    assert message == random_message
    disk_queue.task_done()
    assert disk_queue.qsize() == 0


@pytest.mark.parametrize("count", range(1, MAXSIZE + 1))
def test_multiple_put_and_get(disk_queue: DiskQueue[str], get_random_message: Callable[[], str], count: int):
    messages = [get_random_message() for _ in range(count)]
    for message in messages:
        disk_queue.put(message)
    assert disk_queue.qsize() == count
    for _ in range(count):
        message = disk_queue.get()
        assert message == messages.pop(0)
        disk_queue.task_done()
    assert disk_queue.qsize() == 0


def test_put_and_get_with_delays(disk_queue: DiskQueue[str]):
    for i in range(MAXSIZE):
        disk_queue.put(f"message{i}")
        time.sleep(0.1)
    assert disk_queue.qsize() == MAXSIZE
    for i in range(MAXSIZE):
        message = disk_queue.get()
        assert message == f"message{i}"
        disk_queue.task_done()
        time.sleep(0.1)
    assert disk_queue.qsize() == 0


def test_put_nowait_and_get_nowait(disk_queue: DiskQueue[str], random_message: str):
    disk_queue.put_nowait(random_message)
    assert disk_queue.qsize() == 1
    message = disk_queue.get_nowait()
    assert message == random_message
    disk_queue.task_done()
    assert disk_queue.qsize() == 0


@pytest.mark.parametrize("count", range(1, MAXSIZE + 1))
def test_multiple_put_nowait_and_get_nowait(disk_queue: DiskQueue[str], get_random_message: Callable[[], str], count: int):
    messages = [get_random_message() for _ in range(count)]
    for message in messages:
        disk_queue.put_nowait(message)
    assert disk_queue.qsize() == count
    for _ in range(count):
        message = disk_queue.get_nowait()
        assert message == messages.pop(0)
        disk_queue.task_done()
    assert disk_queue.qsize() == 0


@pytest.mark.parametrize("count", range(1, MAXSIZE + 1))
def test_qsize(disk_queue: DiskQueue[str], count: int):
    assert disk_queue.qsize() == 0
    for i in range(count):
        disk_queue.put("message")
        assert disk_queue.qsize() == i + 1
    for i in range(count):
        disk_queue.get()
        disk_queue.task_done()
        assert disk_queue.qsize() == count - i - 1


def test_empty(disk_queue: DiskQueue[str]):
    assert disk_queue.empty() is True
    disk_queue.put("message")
    assert disk_queue.empty() is False
    message = disk_queue.get_nowait()
    disk_queue.task_done()
    assert message == "message"
    assert disk_queue.empty() is True


def test_get_nowait_empty_queue(disk_queue: DiskQueue[str]):
    with pytest.raises(QueueEmpty):
        disk_queue.get_nowait()


def test_full(disk_queue: DiskQueue[str]):
    for i in range(MAXSIZE):
        disk_queue.put(f"message{i}")
    assert disk_queue.full() is True
    with pytest.raises(QueueFull):
        disk_queue.put_nowait("message_overflow")
    for i in range(MAXSIZE):
        message = disk_queue.get()
        assert message == f"message{i}"
        disk_queue.task_done()
        assert disk_queue.full() is False


def test_has_unfinished_tasks(disk_queue: DiskQueue[str]):
    assert disk_queue._has_unfinished_tasks() is False
    disk_queue.put("message")
    assert disk_queue._has_unfinished_tasks() is True
    disk_queue.get()
    assert disk_queue._has_unfinished_tasks() is True
    disk_queue.task_done()
    assert disk_queue._has_unfinished_tasks() is False


def test_task_done_without_get(disk_queue: DiskQueue[str]):
    with pytest.raises(ValueError):
        disk_queue.task_done()


def test_task_done_too_many_times(disk_queue: DiskQueue[str]):
    disk_queue.put("message")
    disk_queue.get()
    disk_queue.task_done()
    with pytest.raises(ValueError, match=re.escape(TASK_DONE_TOO_MANY_TIMES_ERROR_MSG)):
        disk_queue.task_done()


def test_join(disk_queue: DiskQueue[str]):
    disk_queue.put("message")
    disk_queue.get()
    disk_queue.task_done()
    disk_queue.join()
    assert disk_queue.qsize() == 0


def test_delete(disk_queue: DiskQueue[str]):
    disk_queue.put("message")
    disk_queue.delete()
    with pytest.raises(OperationalError):
        disk_queue.qsize()


def test_join_empty_queue(disk_queue: DiskQueue[str]):
    disk_queue.join()
    assert disk_queue.qsize() == 0


def test_join_with_multiple_tasks(disk_queue: DiskQueue[str]):
    for i in range(5):
        disk_queue.put(f"message{i}")
    for i in range(5):
        disk_queue.get()
        disk_queue.task_done()
    disk_queue.join()
    assert disk_queue.qsize() == 0


def test_join_non_empty_queue(disk_queue: DiskQueue[str]):
    disk_queue.put("message")
    assert disk_queue.qsize() == 1
    assert disk_queue._has_unfinished_tasks() is True
    with pytest.raises((TimeoutError, KeyboardInterrupt)):
        with timeout(3):
            disk_queue.join()
    assert disk_queue._has_unfinished_tasks() is True


def test_put_to_full_queue(disk_queue: DiskQueue[str], random_message: str):
    for _ in range(disk_queue.maxsize):
        disk_queue.put(random_message)
    assert disk_queue.full() is True
    with pytest.raises((TimeoutError, KeyboardInterrupt)):
        with timeout(3):
            disk_queue.put("message_overflow")
    assert disk_queue.full() is True


def test_get_from_empty_queue(disk_queue: DiskQueue[str]):
    assert disk_queue.empty() is True
    with pytest.raises((TimeoutError, KeyboardInterrupt)):
        with timeout(3):
            disk_queue.get()
    assert disk_queue.empty() is True


@pytest.mark.parametrize("timeout", [0.5, 1, 2, 5])
def test_put_with_timeout(disk_queue: DiskQueue[str], random_message: str, timeout: float):
    for _ in range(disk_queue.maxsize):
        disk_queue.put(random_message)
    start_time = time.monotonic()
    with pytest.raises(QueueFull):
        disk_queue.put("message_overflow", timeout=timeout)
    end_time = time.monotonic()
    assert end_time - start_time == pytest.approx(timeout, abs=0.15)


@pytest.mark.parametrize("timeout", [0.5, 1, 2, 5])
def test_get_with_timeout(disk_queue: DiskQueue[str], timeout: float):
    assert disk_queue.empty() is True
    start_time = time.monotonic()
    with pytest.raises(QueueEmpty):
        disk_queue.get(timeout=timeout)
    end_time = time.monotonic()
    assert end_time - start_time == pytest.approx(timeout, abs=0.15)


@pytest.mark.parametrize("count", [50, 100, 500])
def test_put_and_get_unlimited_queue(disk_queue_no_maxsize: DiskQueue, count: int):
    for i in range(count):
        disk_queue_no_maxsize.put(f"message{i}")
    assert disk_queue_no_maxsize.qsize() == count
    for i in range(count):
        message = disk_queue_no_maxsize.get()
        assert message == f"message{i}"
        disk_queue_no_maxsize.task_done()
    assert disk_queue_no_maxsize.qsize() == 0
