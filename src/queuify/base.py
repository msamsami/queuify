from __future__ import annotations

from abc import ABCMeta, abstractmethod
from typing import Any, Generic, TypeVar

T = TypeVar("T")

__all__ = ("Queue",)


class Queue(Generic[T], metaclass=ABCMeta):
    @abstractmethod
    def __init__(self, maxsize: int, *args: Any, **kwargs: Any) -> None:
        pass

    @abstractmethod
    def task_done(self) -> None:
        """Indicate that a formerly enqueued task is complete.

        Used by queue consumers. For each get() used to fetch a task,
        a subsequent call to task_done() tells the queue that the processing
        on the task is complete.

        If a join() is currently blocking, it will resume when all items have
        been processed (meaning that a task_done() call was received for every
        item that had been put() into the queue).

        Raises ValueError if called more times than there were items placed in
        the queue.
        """
        pass

    @abstractmethod
    def join(self) -> None:
        """Block until all items in the queue have been gotten and processed.

        The count of unfinished tasks goes up whenever an item is added to the
        queue. The count goes down whenever a consumer calls task_done() to
        indicate that the item was retrieved and all work on it is complete.

        When the count of unfinished tasks drops to zero, join() unblocks.
        """
        pass

    @abstractmethod
    def qsize(self) -> int:
        """Return the approximate size of the queue.

        Note: qsize() > 0 doesn't guarantee that a subsequent get()
        will not raise :class:`QueueEmpty`.
        """
        pass

    @abstractmethod
    def empty(self) -> bool:
        """
        Return True if the queue is empty, False otherwise (not reliable!).
        """
        pass

    @abstractmethod
    def full(self) -> bool:
        """Return True if there are maxsize items in the queue (not reliable!).

        Note: If the Queue was initialized with maxsize=0 (the default),
        then full() is never True.
        """
        pass

    @abstractmethod
    def put(self, item: T) -> None:
        """Put an item into the queue.

        If the queue is full, wait until a free slot is available before adding item.
        """
        pass

    @abstractmethod
    def get(self) -> T:
        """Remove and return an item from the queue.

        If queue is empty, wait until an item is available.
        """
        pass

    @abstractmethod
    def put_nowait(self, item: T) -> None:
        """Put an item into the queue without blocking.

        If no free slot is immediately available, raise :class:`QueueFull`.
        """
        pass

    @abstractmethod
    def get_nowait(self) -> T:
        """Remove and return an item from the queue without blocking.

        If no item is immediately available, raise :class:`QueueEmpty`.
        """
        pass

    @property
    @abstractmethod
    def maxsize(self) -> int:
        """
        Number of items allowed in the queue. 0 means unlimited.
        """
        pass

    def __bool__(self):
        return True
