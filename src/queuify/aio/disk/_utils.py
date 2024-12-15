from __future__ import annotations

import pathlib
from typing import Any, Optional, cast

import aiofiles
import aiosqlite

from queuify.disk._enums import SqlOperation
from queuify.disk.base import FilePath
from queuify.disk.const import QUEUE_TABLE_COLUMNS, UNFINISHED_TASKS_TABLE_COLUMNS
from queuify.disk.exceptions import QueueFileBroken


async def get_sql_query(operation: SqlOperation, table_name: Optional[str] = None) -> str:
    """Get the SQL query template for the given operation (asynchronous).

    Args:
        operation (SqlOperation): Name of the operation to get the SQL query for.
        table_name (Optional[str]): Name of the table to put in the SQL query. Defaults to None.

    Returns:
        str: The SQL query string for the given operation.
    """
    path = (pathlib.Path(__file__).resolve().parents[2] / "disk" / "queries") / f"{operation.value.lower()}.sql"
    async with aiofiles.open(path, "r") as f:
        query = await f.read()
    if table_name:
        query = query.format(table_name=table_name)
    return query


async def initialize_queue(
    file_path: FilePath,
    queue_table_name: str,
    unfinished_tasks_table_name: str,
    connection_kwargs: Optional[dict[str, Any]] = None,
) -> None:
    """Initialize the queue tables in the specified SQLite database file (asynchronous).

    Args:
        file_path (FilePath): Path to the queue file.
        queue_table_name (str): Name of the SQLite table name for the queue.
        unfinished_tasks_table_name (str): Name of the SQLite table name for the number of unfinished tasks in this queue.
        connection_kwargs (Optional[dict[str, Any]]): Additional keyword arguments for the `aiosqlite` connection. Defaults to None.

    Raises:
        QueueFileBroken: If an existing table's schema does not match the expected structure, indicating possible file corruption.
    """
    async with aiosqlite.connect(file_path, **(connection_kwargs or {})) as connection:
        try:
            for table_name, expected_columns, create_operation in zip(
                [queue_table_name, unfinished_tasks_table_name],
                [QUEUE_TABLE_COLUMNS, UNFINISHED_TASKS_TABLE_COLUMNS],
                [SqlOperation.create_queue_table, SqlOperation.create_unfinished_tasks_table],
            ):
                cursor = await connection.execute(f"PRAGMA table_info('{table_name}')")
                existing_columns = cast(list[tuple[int, str, str, int, Optional[str], int]], await cursor.fetchall())

                if existing_columns:
                    if len(existing_columns) != len(expected_columns):
                        raise QueueFileBroken
                    for col_id, col_name, col_type, _, _, pk in existing_columns:
                        if (col_name, col_type.upper(), pk) != expected_columns[col_id]:
                            raise QueueFileBroken
                else:
                    create_query = await get_sql_query(create_operation, table_name)
                    await connection.executescript(create_query)
            await connection.commit()
        except Exception:
            await connection.rollback()
            raise
