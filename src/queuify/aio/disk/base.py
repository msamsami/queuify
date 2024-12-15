from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, TypeVar

import aiosqlite

from queuify.aio.base import AsyncQueue
from queuify.disk._enums import SqlOperation
from queuify.disk.base import FilePath, _BaseDiskQueue

from ._utils import get_sql_query, initialize_queue

T = TypeVar("T")

__all__ = ("BaseAsyncDiskQueue",)


class BaseAsyncDiskQueue(_BaseDiskQueue, AsyncQueue[T]):
    def __init__(self, file_path: FilePath, queue_name: str, maxsize: int = 0, **connection_kwargs: Any) -> None:
        super().__init__(file_path, queue_name, maxsize)
        self._connection_kwargs = connection_kwargs

        self._initialized: bool = False
        self._init_lock: asyncio.Lock = asyncio.Lock()

    async def _ensure_initialized(self) -> None:
        if not self._initialized or not self._queries:
            async with self._init_lock:
                if not self._queries:
                    for operation in SqlOperation:
                        self._queries[operation] = await get_sql_query(operation)
                if not self._initialized:
                    connection_kwargs = self._connection_kwargs.copy()
                    _ = connection_kwargs.pop("database", None)
                    await initialize_queue(self.file_path, self.table_name, self.unfinished_tasks_table_name, connection_kwargs)
                    self._initialized = True

    @asynccontextmanager
    async def _get_connection(self, commit: bool = False, atomic: bool = False) -> AsyncGenerator[aiosqlite.Connection, None]:
        async with aiosqlite.connect(self.file_path, **self._connection_kwargs) as connection:
            try:
                if atomic:
                    await connection.execute("BEGIN")
                yield connection
                if commit or atomic:
                    await connection.commit()
            except Exception as e:
                if atomic:
                    await connection.rollback()
                raise e

    async def delete(self) -> None:
        async with self._get_connection(atomic=True) as connection:
            for table_name in self.table_name, self.unfinished_tasks_table_name:
                await connection.execute(f'DROP TABLE IF EXISTS "{table_name}";')

    async def __aenter__(self):
        await self._ensure_initialized()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass
