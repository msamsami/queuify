from enum import Enum


class SqlOperation(str, Enum):
    create_queue_table = "create_queue_table"
    count_items = "count_items"
    insert_item = "insert_item"
    fetch_item = "fetch_item"
    delete_item = "delete_item"
    create_unfinished_tasks_table = "create_unfinished_tasks_table"
    count_unfinished_tasks = "count_unfinished_tasks"
    increment_unfinished_tasks = "increment_unfinished_tasks"
    decrement_unfinished_tasks = "decrement_unfinished_tasks"
