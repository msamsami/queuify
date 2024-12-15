import pickle
import threading
import time
from typing import Optional, TypeVar, cast

from watchfiles import watch

from queuify.const import TASK_DONE_TOO_MANY_TIMES_ERROR_MSG, TIMEOUT_NEGATIVE_ERROR_MSG
from queuify.exceptions import QueueEmpty, QueueFull

from ._enums import SqlOperation
from .base import BaseDiskQueue
from .const import WATCHFILES_KWARGS
from .exceptions import QueueFileBroken

T = TypeVar("T")

__all__ = ("DiskQueue",)


class DiskQueue(BaseDiskQueue[T]):
    def _has_unfinished_tasks(self) -> bool:
        """
        Check if there are any unfinished tasks in the queue.
        """
        with self._get_connection() as connection:
            result = connection.execute(
                self._queries[SqlOperation.count_unfinished_tasks].format(table_name=self._unfinished_tasks_table_name)
            ).fetchone()
        result = cast(Optional[tuple[str]], result)
        if result is not None:
            count = int(result[0])
            return count > 0
        else:
            raise QueueFileBroken

    def task_done(self) -> None:
        with self._get_connection(commit=True) as connection:
            result = connection.execute(
                self._queries[SqlOperation.count_unfinished_tasks].format(table_name=self._unfinished_tasks_table_name)
            ).fetchone()
            result = cast(Optional[tuple[str]], result)
            if result is not None:
                if int(result[0]) <= 0:
                    raise ValueError(TASK_DONE_TOO_MANY_TIMES_ERROR_MSG)
                connection.execute(
                    self._queries[SqlOperation.decrement_unfinished_tasks].format(table_name=self._unfinished_tasks_table_name)
                )
            else:
                raise QueueFileBroken

    def join(self) -> None:
        if not self._has_unfinished_tasks():
            return
        for _ in watch(self.file_path, **WATCHFILES_KWARGS):
            if not self._has_unfinished_tasks():
                return

    def qsize(self) -> int:
        with self._get_connection() as connection:
            result = connection.execute(self._queries[SqlOperation.count_items].format(table_name=self._table_name)).fetchone()
        result = cast(Optional[tuple[int]], result)
        if result is not None:
            return int(result[0])
        else:
            raise QueueFileBroken

    def empty(self) -> bool:
        return not self.qsize()

    def full(self) -> bool:
        if self._maxsize <= 0:
            return False
        else:
            return self.qsize() >= self._maxsize

    def _get_timeout_event_and_thread(
        self, timeout: Optional[float] = None
    ) -> tuple[Optional[threading.Event], Optional[threading.Thread]]:
        if not timeout:
            return None, None
        if timeout < 0:
            raise ValueError(TIMEOUT_NEGATIVE_ERROR_MSG)
        stop_event = threading.Event()

        def stop_on_timeout() -> None:
            time.sleep(timeout)
            stop_event.set()

        stop_on_timeout_thread = threading.Thread(target=stop_on_timeout, daemon=True)
        return stop_event, stop_on_timeout_thread

    def put(self, item: T, timeout: Optional[float] = None) -> None:
        try:
            return self.put_nowait(item)
        except QueueFull:
            pass
        timeout_event, timeout_thread = self._get_timeout_event_and_thread(timeout)
        if timeout_thread:
            timeout_thread.start()
        for _ in watch(self.file_path, stop_event=timeout_event, **WATCHFILES_KWARGS):
            try:
                return self.put_nowait(item)
            except QueueFull:
                pass
        else:
            if timeout_thread and timeout_event and timeout_event.is_set():
                timeout_thread.join()
            raise QueueFull

    def get(self, timeout: Optional[float] = None) -> T:
        try:
            return self.get_nowait()
        except QueueEmpty:
            pass
        timeout_event, timeout_thread = self._get_timeout_event_and_thread(timeout)
        if timeout_thread:
            timeout_thread.start()
        for _ in watch(self.file_path, stop_event=timeout_event, **WATCHFILES_KWARGS):
            try:
                return self.get_nowait()
            except QueueEmpty:
                pass
        else:
            if timeout_thread and timeout_event and timeout_event.is_set():
                timeout_thread.join()
            raise QueueEmpty

    def put_nowait(self, item: T) -> None:
        if self.full():
            raise QueueFull
        serialized_item = pickle.dumps(item)
        with self._get_connection(atomic=True) as connection:
            connection.execute(self._queries[SqlOperation.insert_item].format(table_name=self._table_name), (serialized_item,))
            connection.execute(
                self._queries[SqlOperation.increment_unfinished_tasks].format(table_name=self._unfinished_tasks_table_name)
            )

    def get_nowait(self) -> T:
        with self._get_connection(atomic=True) as connection:
            row = connection.execute(self._queries[SqlOperation.fetch_item].format(table_name=self._table_name)).fetchone()
            row = cast(Optional[tuple[int, bytes]], row)
            if row is None:
                raise QueueEmpty
            item_id, item_value = row
            connection.execute(self._queries[SqlOperation.delete_item].format(table_name=self._table_name), (item_id,))
            item = pickle.loads(item_value)
            return cast(T, item)

    def __repr__(self) -> str:
        return f"DiskQueue<(file_path='{self.file_path}', queue_name='{self._queue_name}', maxsize={self._maxsize})>"
