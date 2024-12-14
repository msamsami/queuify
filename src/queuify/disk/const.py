from typing import Any

WATCHFILES_KWARGS: dict[str, Any] = {"debounce": 10, "step": 10, "force_polling": True, "poll_delay_ms": 5}

QUEUE_TABLE_COLUMNS: list[tuple[str, str, int]] = [("value", "BLOB", 0)]

UNFINISHED_TASKS_TABLE_COLUMNS: list[tuple[str, str, int]] = [("count", "INTEGER", 0)]
