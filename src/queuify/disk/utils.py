from __future__ import annotations

import pathlib
import sqlite3
from typing import TYPE_CHECKING, Any, Optional, cast

from .const import QUEUE_TABLE_COLUMNS, UNFINISHED_TASKS_TABLE_COLUMNS
from .enums import SqlOperation
from .exceptions import QueueFileBroken

if TYPE_CHECKING:
    from .base import FilePath


def get_sql_query(operation: SqlOperation, table_name: Optional[str] = None) -> str:
    """Get the SQL query template for the given operation.

    Args:
        operation (SqlOperation): Name of the operation to get the SQL query for.
        table_name (Optional[str]): Name of the table to put in the SQL query. Defaults to None.

    Returns:
        str: The SQL query string for the given operation.
    """
    path = (pathlib.Path(__file__).resolve().parent / "queries") / f"{operation.value.lower()}.sql"
    with open(path, "r") as f:
        query = f.read()
    if table_name:
        query = query.format(table_name=table_name)
    return query


def initialize_queue(
    file_path: "FilePath", queue_table_name: str, unfinished_tasks_table_name: str, connection_kwargs: dict[str, Any]
) -> None:
    """Initialize the queue tables in the specified SQLite database file.

    Args:
        file_path (FilePath): Path to the queue file.
        queue_table_name (str): Name of the SQLite table name for the queue.
        unfinished_tasks_table_name (str): Name of the SQLite table name for the number of unfinished tasks in this queue.
        connection_kwargs (dict[str, Any]): Additional keyword arguments for the `sqlite3` connection.

    Raises:
        QueueFileBroken: If an existing table's schema does not match the expected structure, indicating possible file corruption.
    """
    with sqlite3.connect(file_path, **connection_kwargs) as connection:
        try:
            connection.execute("BEGIN")

            for table_name, expected_columns, create_operation in zip(
                [queue_table_name, unfinished_tasks_table_name],
                [QUEUE_TABLE_COLUMNS, UNFINISHED_TASKS_TABLE_COLUMNS],
                [SqlOperation.create_queue_table, SqlOperation.create_unfinished_tasks_table],
            ):
                cursor = connection.execute(f"PRAGMA table_info('{table_name}')")
                existing_columns = cast(list[tuple[int, str, str, int, Optional[str], int]], cursor.fetchall())

                if existing_columns:
                    if len(existing_columns) != len(expected_columns):
                        raise QueueFileBroken
                    for col_id, col_name, col_type, _, _, pk in existing_columns:
                        if (col_name, col_type.upper(), pk) != expected_columns[col_id]:
                            raise QueueFileBroken
                else:
                    create_query = get_sql_query(create_operation, table_name)
                    connection.executescript(create_query)

            connection.commit()
        except Exception:
            connection.rollback()
            raise
