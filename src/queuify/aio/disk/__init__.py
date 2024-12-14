import asyncio
import pickle
from typing import Optional, TypeVar, cast

from watchfiles import awatch

from queuify.const import TASK_DONE_TOO_MANY_TIMES_ERROR_MSG, TIMEOUT_NEGATIVE_ERROR_MSG
from queuify.disk.const import WATCHFILES_KWARGS
from queuify.disk.enums import SqlOperation
from queuify.disk.exceptions import QueueFileBroken
from queuify.exceptions import QueueEmpty, QueueFull

from .base import BaseAsyncDiskQueue

T = TypeVar("T")

__all__ = ("DiskQueue",)


class DiskQueue(BaseAsyncDiskQueue[T]):
    async def _has_unfinished_tasks(self) -> bool:
        """
        Check if there are any unfinished tasks in the queue.
        """
        await self._ensure_initialized()
        async with self._get_connection() as connection:
            async with connection.execute(
                self._queries[SqlOperation.count_unfinished_tasks].format(table_name=self._unfinished_tasks_table_name)
            ) as cursor:
                result = await cursor.fetchone()
        if result is not None:
            count = int(result[0])
            return count > 0
        else:
            raise QueueFileBroken

    async def task_done(self) -> None:
        await self._ensure_initialized()
        async with self._get_connection(commit=True) as connection:
            async with connection.execute(
                self._queries[SqlOperation.count_unfinished_tasks].format(table_name=self._unfinished_tasks_table_name)
            ) as cursor:
                result = await cursor.fetchone()
            if result is not None:
                if int(result[0]) <= 0:
                    raise ValueError(TASK_DONE_TOO_MANY_TIMES_ERROR_MSG)
                await connection.execute(
                    self._queries[SqlOperation.decrement_unfinished_tasks].format(table_name=self._unfinished_tasks_table_name)
                )
            else:
                raise QueueFileBroken

    async def join(self) -> None:
        if not await self._has_unfinished_tasks():
            return
        async for _ in awatch(self.file_path, **WATCHFILES_KWARGS):
            if not await self._has_unfinished_tasks():
                return

    async def qsize(self) -> int:
        await self._ensure_initialized()
        async with self._get_connection() as connection:
            async with connection.execute(self._queries[SqlOperation.count_items].format(table_name=self._table_name)) as cursor:
                result = await cursor.fetchone()
        if result is not None:
            return int(result[0])
        else:
            raise QueueFileBroken

    async def empty(self) -> bool:
        return not await self.qsize()

    async def full(self) -> bool:
        if self._maxsize <= 0:
            return False
        else:
            return (await self.qsize()) >= self._maxsize

    def _get_timeout_event_and_task(
        self, timeout: Optional[float] = None
    ) -> tuple[Optional[asyncio.Event], Optional[asyncio.Task[None]]]:
        if not timeout:
            return None, None
        if timeout < 0:
            raise ValueError(TIMEOUT_NEGATIVE_ERROR_MSG)
        stop_event = asyncio.Event()

        async def stop_on_timeout() -> None:
            await asyncio.sleep(timeout)
            stop_event.set()

        stop_on_timeout_task = asyncio.create_task(stop_on_timeout())
        return stop_event, stop_on_timeout_task

    async def put(self, item: T, timeout: Optional[float] = None) -> None:
        try:
            return await self.put_nowait(item)
        except QueueFull:
            pass
        timeout_event, timeout_task = self._get_timeout_event_and_task(timeout)
        async for _ in awatch(self.file_path, stop_event=timeout_event, **WATCHFILES_KWARGS):
            try:
                return await self.put_nowait(item)
            except QueueFull:
                pass
            finally:
                if timeout_task:
                    await timeout_task
        else:
            raise QueueFull

    async def get(self, timeout: Optional[float] = None) -> T:
        item: Optional[T] = None
        while item is None:
            try:
                item = await self.get_nowait()
                break
            except QueueEmpty:
                pass
            timeout_event, timeout_task = self._get_timeout_event_and_task(timeout)
            async for _ in awatch(self.file_path, stop_event=timeout_event, **WATCHFILES_KWARGS):
                try:
                    item = await self.get_nowait()
                    break
                except QueueEmpty:
                    pass
                finally:
                    if timeout_task:
                        await timeout_task
            else:
                raise QueueEmpty
        return item

    async def put_nowait(self, item: T) -> None:
        if await self.full():
            raise QueueFull
        serialized_item = pickle.dumps(item)
        async with self._get_connection(atomic=True) as connection:
            await connection.execute(
                self._queries[SqlOperation.insert_item].format(table_name=self._table_name), (serialized_item,)
            )
            await connection.execute(
                self._queries[SqlOperation.increment_unfinished_tasks].format(table_name=self._unfinished_tasks_table_name)
            )

    async def get_nowait(self) -> T:
        await self._ensure_initialized()
        async with self._get_connection(atomic=True) as connection:
            async with connection.execute(self._queries[SqlOperation.fetch_item].format(table_name=self._table_name)) as cursor:
                row = await cursor.fetchone()
            if row is None:
                raise QueueEmpty
            item_id, item_value = row
            await connection.execute(self._queries[SqlOperation.delete_item].format(table_name=self._table_name), (item_id,))
            item = pickle.loads(item_value)
            return cast(T, item)

    def __repr__(self) -> str:
        return f"DiskQueue<(file_path='{self.file_path}', queue_name='{self._queue_name}', maxsize={self._maxsize})>"
