"""Real bounded asyncio producer/consumer tasks, convenient from a notebook.

`await run_live(engine, teaching_fixture())` works under Jupyter's running event
loop. Only the CLI calls asyncio.run. The default synthetic source's event clock
advances two seconds per delivery, so a few real seconds show several windows.
"""
from __future__ import annotations

import asyncio
from datetime import timedelta
import inspect
import math
import time
from typing import Any, Callable, Iterable

from .engine import StreamEngine
from .events import Delivery, Event, PRICE_CENTS, PRODUCT_BY_PRICE, T0


def _live_event(sequence: int) -> Event:
    # Every thirteenth arrival is a retry of the immediately preceding event.
    source = sequence - 1 if sequence % 13 == 12 else sequence
    timestamp = T0 + timedelta(seconds=source * 2 - (100 if source % 19 == 18 else 0))
    if source % 4 == 3:
        return Event(f"live-{source:06d}", "review", timestamp, 5,
                     rating=1 if source % 5 else 5, text="לא עובד בכלל" if source % 5 else "מוצר מעולה")
    cents = PRICE_CENTS[source % len(PRICE_CENTS)]
    return Event(f"live-{source:06d}", "order", timestamp, PRODUCT_BY_PRICE[cents],
                 amount_cents=cents, order_id=20_000 + source)


def _positive_finite(value: float, name: str, allow_zero: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{name} must be finite")
    if value < 0 or (value == 0 and not allow_zero):
        raise ValueError(f"{name} must be {'nonnegative' if allow_zero else 'positive'}")
    return float(value)


async def run_live(engine: StreamEngine, deliveries: Iterable[Delivery] | None = None, *,
                   duration_s: float | None = 4.0, interval_s: float = 0.05,
                   processing_delay_s: float = 0.0, queue_maxsize: int = 8,
                   finalize: bool = False, on_snapshot: Callable[[dict[str, Any]], Any] | None = None) -> dict[str, Any]:
    """Produce, consume, and gracefully drain a bounded queue.

    duration_s limits admission by the producer. Already queued records drain
    afterward, so elapsed time may exceed the admission period with a slow sink.
    A finite fixture may finish sooner; duration_s=None is allowed only for a
    finite iterable. Every accepted queue item retains its original offset.
    Cancellation cancels both tasks; the committed DB remains restartable.
    `finalize=True` is an explicit declaration that this demo has ended, not an
    inference from timer expiry. Default timer expiry leaves windows open.
    The callback receives a fresh DB snapshot plus transient `queue` metrics:
    depth, maxsize, produced, consumed, and backpressure_waits. Depth counts
    waiting data records, excluding the processed record and end marker. These
    metrics are not persisted and are absent from engine.snapshot().
    """
    if isinstance(queue_maxsize, bool) or not isinstance(queue_maxsize, int) or queue_maxsize < 1:
        raise ValueError("queue_maxsize must be a positive integer")
    if duration_s is not None:
        duration_s = _positive_finite(duration_s, "duration_s")
    elif deliveries is None:
        raise ValueError("an unbounded source requires a finite duration_s")
    interval_s = _positive_finite(interval_s, "interval_s", True)
    processing_delay_s = _positive_finite(processing_delay_s, "processing_delay_s", True)
    queue: asyncio.Queue[Delivery | object] = asyncio.Queue(maxsize=queue_maxsize)
    end = object()
    started = time.perf_counter()
    deadline = started + duration_s if duration_s is not None else None
    state: dict[str, Any] = {
        "produced": 0, "consumed": 0, "queue_maxsize": queue_maxsize,
        "max_queue_depth": 0, "backpressure_waits": 0, "producer_wait_s": 0.0,
        "stop_reason": "duration", "event_time_clock": "fixture" if deliveries is not None else "synthetic_2s_per_delivery",
    }
    source = iter(deliveries) if deliveries is not None else None
    starting_offset = engine.next_offset

    async def producer() -> None:
        sequence = starting_offset
        while True:
            remaining = None if deadline is None else deadline - time.perf_counter()
            if remaining is not None and remaining <= 0:
                break
            if source is None:
                item = Delivery(sequence, _live_event(sequence))
            else:
                try:
                    item = next(source)
                except StopIteration:
                    state["stop_reason"] = "end_of_input"
                    break
            blocked = queue.full()
            if blocked:
                state["backpressure_waits"] += 1
            waiting_since = time.perf_counter()
            try:
                if remaining is None:
                    await queue.put(item)
                else:
                    # Keep admission and its counter in the same task. A
                    # wait_for child task can wake the consumer before the
                    # producer gets to increment `produced`.
                    async with asyncio.timeout(remaining):
                        await queue.put(item)
            except TimeoutError:
                state["producer_wait_s"] += time.perf_counter() - waiting_since
                break
            state["producer_wait_s"] += time.perf_counter() - waiting_since
            state["produced"] += 1
            state["max_queue_depth"] = max(state["max_queue_depth"], queue.qsize())
            sequence += 1
            if interval_s:
                pause = interval_s if deadline is None else min(interval_s, max(0.0, deadline - time.perf_counter()))
                await asyncio.sleep(pause)
        # The consumer drains admitted records before seeing this sentinel.
        # No finally-block put: cancellation must never block on a full queue.
        await queue.put(end)

    async def consumer() -> None:
        while True:
            item = await queue.get()
            try:
                if item is end:
                    return
                if processing_delay_s:
                    await asyncio.sleep(processing_delay_s)
                engine.ingest(item.event, offset=item.offset)
                state["consumed"] += 1
                if on_snapshot is not None:
                    snapshot = engine.snapshot()
                    snapshot["queue"] = {
                        "depth": state["produced"] - state["consumed"],
                        "maxsize": queue_maxsize,
                        "produced": state["produced"],
                        "consumed": state["consumed"],
                        "backpressure_waits": state["backpressure_waits"],
                    }
                    callback = on_snapshot(snapshot)
                    if inspect.isawaitable(callback):
                        await callback
            finally:
                queue.task_done()

    async with asyncio.TaskGroup() as group:
        group.create_task(producer(), name="streaming-lab-producer")
        group.create_task(consumer(), name="streaming-lab-consumer")
    await queue.join()
    state["snapshot"] = engine.finalize_demo() if finalize else engine.snapshot()
    state["elapsed_s"] = round(time.perf_counter() - started, 4)
    state["producer_wait_s"] = round(state["producer_wait_s"], 4)
    return state
