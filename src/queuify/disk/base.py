from __future__ import annotations

import sqlite3
import threading
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Awaitable, Generator, TypeVar, Union

from queuify.base import Queue

from ._enums import SqlOperation
from ._utils import get_sql_query, initialize_queue

T = TypeVar("T")
FilePath = Union[str, Path]

__all__ = ("BaseDiskQueue",)


class _BaseDiskQueue(metaclass=ABCMeta):
    namespace_prefix: str = "queuify_queue"

    def __init__(self, file_path: FilePath, queue_name: str, maxsize: int = 0) -> None:
        self.file_path = file_path
        self._queue_name = queue_name
        self._maxsize = maxsize

        self._table_name = f"{self.namespace_prefix}_{queue_name}"
        self._unfinished_tasks_table_name = f"{self._table_name}_unfinished_tasks"
        self._queries: dict[SqlOperation, str] = {}

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
    def queue_name(self) -> str:
        """
        Return the name of the queue.
        """
        return self._queue_name

    @property
    def table_name(self) -> str:
        """
        Return the SQLite table name for this queue.
        """
        return self._table_name

    @property
    def unfinished_tasks_table_name(self) -> str:
        """
        Return the SQLite table name for the number of unfinished tasks in this queue.
        """
        return self._unfinished_tasks_table_name

    def __hash__(self):
        return hash(self.table_name)


class BaseDiskQueue(_BaseDiskQueue, Queue[T]):
    def __init__(self, file_path: FilePath, queue_name: str, maxsize: int = 0, **connection_kwargs: Any) -> None:
        super().__init__(file_path, queue_name, maxsize)
        self._connection_kwargs = connection_kwargs

        self._initialized: bool = False
        self._init_lock: threading.Lock = threading.Lock()
        self._ensure_initialized()

    def _ensure_initialized(self) -> None:
        if not self._initialized or not self._queries:
            with self._init_lock:
                if not self._queries:
                    for operation in SqlOperation:
                        self._queries[operation] = get_sql_query(operation)
                if not self._initialized:
                    connection_kwargs = self._connection_kwargs.copy()
                    _ = connection_kwargs.pop("database", None)
                    initialize_queue(self.file_path, self.table_name, self.unfinished_tasks_table_name, connection_kwargs)
                    self._initialized = True

    @contextmanager
    def _get_connection(self, commit: bool = False, atomic: bool = False) -> Generator[sqlite3.Connection, None, None]:
        with sqlite3.connect(self.file_path, **self._connection_kwargs) as connection:
            try:
                if atomic:
                    connection.execute("BEGIN")
                yield connection
                if commit or atomic:
                    connection.commit()
            except Exception as e:
                if atomic:
                    connection.rollback()
                raise e

    def delete(self) -> None:
        with self._get_connection(atomic=True) as connection:
            for table_name in self.table_name, self.unfinished_tasks_table_name:
                connection.execute(f'DROP TABLE IF EXISTS "{table_name}";')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
