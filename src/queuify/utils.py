import signal
from contextlib import ContextDecorator
from typing import Any, Optional

__all__ = ("timeout",)


def _raise_timeout_error(signum: int, frame: Any) -> None:
    raise TimeoutError


class timeout(ContextDecorator):
    def __init__(self, delay: Optional[float] = None) -> None:
        self._delay = delay

    def __enter__(self) -> "timeout":
        if self._delay:
            self._replace_alarm_handler()
            signal.setitimer(signal.ITIMER_REAL, self._delay)
        return self

    def __exit__(self, exc_type: Optional[type], exc_value: Optional[BaseException], traceback: Optional[Any]) -> None:
        if self._delay:
            self._restore_alarm_handler()
            signal.alarm(0)

    def _replace_alarm_handler(self) -> None:
        self._old_alarm_handler = signal.signal(signal.SIGALRM, _raise_timeout_error)

    def _restore_alarm_handler(self) -> None:
        signal.signal(signal.SIGALRM, self._old_alarm_handler)
