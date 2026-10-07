"""Decorators for cross-cutting concerns: execution timing and retry
logic. Applied to pipeline stage `.run()` methods so timing/retry behavior
doesn't have to be hand-written inside every stage.
"""

import functools
import logging
import time

logger = logging.getLogger(__name__)


def log_execution_time(func):
    """Logs how long the wrapped function took. In a real Spark job this
    is what tells you 'the aggregate stage took 4 minutes, the load stage
    took 30 seconds' — the first place to look when tuning performance."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        logger.info("%s completed in %.2fs", func.__qualname__, elapsed)
        return result
    return wrapper


def retry(n: int = 3, delay_seconds: float = 2.0):
    """Decorator factory — note the extra layer of nesting, since this
    decorator itself takes arguments. Retries the wrapped function up to
    `n` times on any exception, with a flat delay between attempts.
    Useful for transient failures (a JDBC connection blip), not for
    deterministic errors like a schema mismatch — those will just fail
    the same way every time."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, n + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    last_exc = exc
                    logger.warning(
                        "%s failed on attempt %d/%d: %s",
                        func.__qualname__, attempt, n, exc,
                    )
                    if attempt < n:
                        time.sleep(delay_seconds)
            raise last_exc
        return wrapper
    return decorator