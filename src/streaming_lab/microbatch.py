"""Continuous orders collected into real asynchronous processing batches.

A producer and consumer run concurrently through a bounded asyncio.Queue. The
consumer collects a batch until its size limit OR its collection timer fires.
These are processing buffers, not event-time windows: StreamEngine still places
each event into its own event-time window and commits EACH record separately.
An exception halfway through a batch can therefore leave a committed prefix.
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


def _positive_integer(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{name} must be a positive integer")


def _seconds(value: float, name: str, *, zero_allowed: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{name} must be finite")
    if value < 0 or (value == 0 and not zero_allowed):
        raise ValueError(f"{name} must be {'nonnegative' if zero_allowed else 'positive'}")
    return float(value)


def _continuous_order(offset: int) -> Event:
    cents = PRICE_CENTS[offset % len(PRICE_CENTS)]
    return Event(
        f"continuous-order-{offset:06d}", "order", T0 + timedelta(seconds=offset * 2),
        PRODUCT_BY_PRICE[cents], amount_cents=cents, order_id=30_000 + offset,
    )


async def run_microbatch(
    engine: StreamEngine,
    deliveries: Iterable[Delivery] | None = None,
    *,
    duration_s: float | None = 4.0,
    interval_s: float = 0.03,
    max_batch_size: int = 5,
    max_wait_s: float = 0.2,
    processing_delay_s: float = 0.0,
    queue_maxsize: int = 8,
    finalize: bool = False,
    on_snapshot: Callable[[dict[str, Any]], Any] | None = None,
) -> dict[str, Any]:
    """Continuously admit records and process small batches without chunking a list.

    With no deliveries argument, emit synthetic orders until duration_s expires.
    Supply a finite Delivery iterable and duration_s=None to consume it completely.
    interval_s paces individual arrivals, never prebuilt batches. processing_delay_s
    is an optional delay PER RECORD, useful for demonstrating backpressure.

    max_wait_s starts when the consumer collects the FIRST record of the current
    batch. A timer flushes a nonempty batch even if no next event arrives. This is
    a collection-time limit; queueing, a slow sink, callback work, and event-loop
    scheduling can add latency. The queue and current batch have separate bounds:
    at most queue_maxsize queued records plus max_batch_size in the current batch.

    The producer explicitly signals its stop. Any remaining partial batch flushes
    with reason final_drain; this does not close event-time windows. finalize=True
    is a separate explicit declaration of finite demo completion.

    Each event uses engine.ingest's OWN SQLite transaction. No atomic batch or
    cross-system exactly-once behavior is claimed. Cancellation/error cancels both
    tasks; restart from durable source offsets to replay any uncommitted records.
    Callbacks run once after each completed batch, with transient batch and queue
    metadata added to a fresh snapshot. Synchronous and async callbacks are valid.
    """
    _positive_integer(max_batch_size, "max_batch_size")
    _positive_integer(queue_maxsize, "queue_maxsize")
    max_wait_s = _seconds(max_wait_s, "max_wait_s")
    interval_s = _seconds(interval_s, "interval_s", zero_allowed=True)
    processing_delay_s = _seconds(processing_delay_s, "processing_delay_s", zero_allowed=True)
    if duration_s is not None:
        duration_s = _seconds(duration_s, "duration_s")
    elif deliveries is None:
        raise ValueError("an unbounded source requires a finite duration_s")

    queue: asyncio.Queue[Delivery | object] = asyncio.Queue(maxsize=queue_maxsize)
    end = object()
    started = time.perf_counter()
    deadline = started + duration_s if duration_s is not None else None
    source = iter(deliveries) if deliveries is not None else None
    state: dict[str, Any] = {
        "produced": 0, "consumed": 0, "batches": 0,
        "queue_maxsize": queue_maxsize, "max_queue_depth": 0,
        "max_batch_size": max_batch_size, "max_wait_s": max_wait_s,
        "max_buffered_records": 0, "backpressure_waits": 0, "producer_wait_s": 0.0,
        "batch_sizes": [], "batch_wait_s": [], "batch_details": [],
        "flush_counts": {"max_size": 0, "max_wait": 0, "final_drain": 0},
        "final_partial_batch_size": 0, "stop_reason": "duration",
        "event_time_clock": "fixture" if source is not None else "synthetic_2s_per_order",
    }

    async def producer() -> None:
        offset = engine.next_offset
        while True:
            remaining = None if deadline is None else deadline - time.perf_counter()
            if remaining is not None and remaining <= 0:
                break
            if source is None:
                item = Delivery(offset, _continuous_order(offset))
            else:
                try:
                    item = next(source)
                except StopIteration:
                    state["stop_reason"] = "end_of_input"
                    break
            if queue.full():
                state["backpressure_waits"] += 1
            waiting_since = time.perf_counter()
            try:
                if remaining is None:
                    await queue.put(item)
                else:
                    # Same-task admission keeps produced ahead of consumed.
                    async with asyncio.timeout(remaining):
                        await queue.put(item)
            except TimeoutError:
                state["producer_wait_s"] += time.perf_counter() - waiting_since
                break
            state["producer_wait_s"] += time.perf_counter() - waiting_since
            state["produced"] += 1
            state["max_queue_depth"] = max(state["max_queue_depth"], queue.qsize())
            state["max_buffered_records"] = max(
                state["max_buffered_records"], state["produced"] - state["consumed"])
            offset += 1
            if interval_s:
                pause = interval_s if deadline is None else min(interval_s, max(0.0, deadline - time.perf_counter()))
                await asyncio.sleep(pause)
        # Never put this marker in a finally block: cancellation must not wait
        # on a full queue after the consumer has already stopped.
        await queue.put(end)

    async def consumer() -> None:
        while True:
            first = await queue.get()
            if first is end:
                queue.task_done()
                return
            batch = [first]
            collection_started = time.perf_counter()
            collect_until = collection_started + max_wait_s
            flush_reason = "max_size"
            source_finished = False
            try:
                while len(batch) < max_batch_size:
                    remaining = collect_until - time.perf_counter()
                    if remaining <= 0:
                        flush_reason = "max_wait"
                        break
                    try:
                        async with asyncio.timeout(remaining):
                            item = await queue.get()
                    except TimeoutError:
                        flush_reason = "max_wait"
                        break
                    if item is end:
                        queue.task_done()
                        source_finished = True
                        flush_reason = "final_drain"
                        break
                    batch.append(item)
                collection_wait = time.perf_counter() - collection_started
                processing_started = time.perf_counter()
                for item in batch:
                    if processing_delay_s:
                        await asyncio.sleep(processing_delay_s)
                    engine.ingest(item.event, offset=item.offset)
                    state["consumed"] += 1
                state["batches"] += 1
                details = {
                    "index": state["batches"], "size": len(batch),
                    "flush_reason": flush_reason,
                    "collection_wait_s": round(collection_wait, 6),
                    "processing_s": round(time.perf_counter() - processing_started, 6),
                    "first_offset": batch[0].offset, "last_offset": batch[-1].offset,
                }
                state["batch_sizes"].append(len(batch))
                state["batch_wait_s"].append(details["collection_wait_s"])
                state["batch_details"].append(details)
                state["flush_counts"][flush_reason] += 1
                if flush_reason == "final_drain":
                    state["final_partial_batch_size"] = len(batch)
                if on_snapshot is not None:
                    snapshot = engine.snapshot()
                    snapshot["batch"] = dict(details)
                    snapshot["queue"] = {
                        "depth": state["produced"] - state["consumed"],
                        "maxsize": queue_maxsize,
                        "produced": state["produced"], "consumed": state["consumed"],
                        "backpressure_waits": state["backpressure_waits"],
                    }
                    callback = on_snapshot(snapshot)
                    if inspect.isawaitable(callback):
                        await callback
            finally:
                for _ in batch:
                    queue.task_done()
            if source_finished:
                return

    async with asyncio.TaskGroup() as group:
        group.create_task(producer(), name="streaming-lab-microbatch-producer")
        group.create_task(consumer(), name="streaming-lab-microbatch-consumer")
    await queue.join()
    state["snapshot"] = engine.finalize_demo() if finalize else engine.snapshot()
    state["elapsed_s"] = round(time.perf_counter() - started, 6)
    state["producer_wait_s"] = round(state["producer_wait_s"], 6)
    return state
