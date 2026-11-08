"""Optional real Kafka transport for the same durable SQLite teaching engine.

One topic, one partition, one consumer process and one DB per consumer group.
Kafka commits and SQLite transactions are separate: process locally, commit the
next Kafka offset, and tolerate replay if the process stops between those steps.
No confluent_kafka import is needed for the ordinary local lab or unit tests.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import random
import sqlite3
import sys
import time
from typing import Any, Callable
import uuid

from .engine import StreamEngine
from .events import Event, PRICE_CENTS, PRODUCT_BY_PRICE, parse_time

DEFAULT_BOOTSTRAP = "localhost:19092"
TOPICS = {"order": "streaming-lab-orders", "review": "streaming-lab-reviews"}
CRASH_EXIT_CODE = 86


class KafkaLabError(RuntimeError):
    """A transport error that must not be reported as a successful lab run."""


class CrashBeforeBrokerCommit(KafkaLabError):
    """Deliberate teaching failure after durable ingest, before broker commit."""


def _kafka() -> Any:
    try:
        import confluent_kafka
    except ImportError as exc:
        raise KafkaLabError(
            "Kafka is optional. Install it with: uv sync --extra kafka "
            "(confluent-kafka==2.16.1). Then use uv run --extra kafka ..."
        ) from exc
    return confluent_kafka


def _admin(bootstrap: str, timeout: float) -> Any:
    _kafka()
    from confluent_kafka.admin import AdminClient
    return AdminClient({
        "bootstrap.servers": bootstrap,
        "socket.timeout.ms": max(1000, int(timeout * 1000)),
    })


def _topic_metadata(admin: Any, topic: str, timeout: float) -> Any:
    # Request all topics: requesting one unknown topic must not auto-create it.
    metadata = admin.list_topics(timeout=timeout)
    topic_metadata = metadata.topics.get(topic)
    if topic_metadata is None or topic_metadata.error is not None:
        raise KafkaLabError(f"Topic {topic!r} is unavailable. Run the 'topics' command first.")
    if set(topic_metadata.partitions) != {0}:
        raise KafkaLabError(
            f"Topic {topic!r} must have exactly one partition (0) for this lab. "
            "A multi-partition engine needs separate source checkpoints."
        )
    return metadata


def create_topics(bootstrap: str, names: list[str], *, timeout: float = 15) -> dict[str, Any]:
    """Create missing topics explicitly; inspect existing ones, never delete."""
    kafka = _kafka()
    from confluent_kafka.admin import NewTopic
    admin = _admin(bootstrap, timeout)
    metadata = admin.list_topics(timeout=timeout)
    missing = [name for name in dict.fromkeys(names) if name not in metadata.topics]
    created: list[str] = []
    if missing:
        futures = admin.create_topics(
            [NewTopic(name, num_partitions=1, replication_factor=1,
                      config={"cleanup.policy": "delete"}) for name in missing],
            request_timeout=timeout, operation_timeout=timeout,
        )
        for name, future in futures.items():
            try:
                future.result(timeout=timeout + 1)
                created.append(name)
            except kafka.KafkaException as exc:
                # Another explicit provisioning command may have won the race.
                if not exc.args or exc.args[0].code() != kafka.KafkaError.TOPIC_ALREADY_EXISTS:
                    raise KafkaLabError(f"Topic creation failed for {name}: {exc}") from exc
    for name in names:
        _topic_metadata(admin, name, timeout)
    return {"bootstrap": bootstrap, "topics": names, "created": created,
            "partitions_per_topic": 1, "replication_factor_for_new_topics": 1}


def make_event(kind: str, number: int, *, run_id: str, start_time: datetime,
               interval: float = 0.05, burst: bool = False, seed: int = 7) -> Event:
    """Deterministic payload/ID for a run; number starts at zero.

    Repeating run_id, start_time, seed and interval repeats logical event IDs
    and payloads. Intentional retries below reuse the original serialized bytes.
    """
    if kind not in TOPICS or number < 0:
        raise ValueError("kind must be order/review and number must be >= 0")
    rnd = random.Random(f"{seed}:{number}")
    event_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"streaming-lab:{run_id}:{kind}:{number}"))
    event_time = parse_time(start_time) + timedelta(seconds=number * interval)
    if kind == "order":
        cents = rnd.choice(PRICE_CENTS)
        return Event(event_id, kind, event_time, PRODUCT_BY_PRICE[cents],
                     amount_cents=cents, order_id=number + 1)
    rating = 1 if burst else rnd.randint(1, 5)
    return Event(event_id, kind, event_time, 5 if burst else rnd.randint(1, 6),
                 rating=rating, text="משלוח מאוחר, המוצר הגיע פגום" if rating <= 2 else "המוצר הגיע בזמן")


def encode_event(event: Event) -> tuple[bytes, bytes]:
    """An order retries under its ID; reviews group by product identity."""
    key = event.event_id if event.kind == "order" else str(event.product_id)
    return key.encode("utf-8"), json.dumps(
        event.to_dict(), ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


@dataclass
class DeliveryTracker:
    """Bounded callback error storage, including failures delivered by flush."""
    acknowledged: int = 0
    failed: int = 0
    first_errors: list[str] = field(default_factory=list)

    def callback(self, error: Any, _message: Any) -> None:
        if error is None:
            self.acknowledged += 1
        else:
            self.failed += 1
            if len(self.first_errors) < 5:
                self.first_errors.append(str(error))

    def check(self) -> None:
        if self.failed:
            raise KafkaLabError(f"{self.failed} delivery failure(s): {'; '.join(self.first_errors)}")


def flush_checked(producer: Any, tracker: DeliveryTracker, timeout: float) -> None:
    remaining = producer.flush(timeout)
    tracker.check()
    if remaining:
        raise KafkaLabError(f"Producer flush timed out with {remaining} unconfirmed message(s).")


def _enqueue(producer: Any, tracker: DeliveryTracker, topic: str, key: bytes,
             value: bytes, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while True:
        tracker.check()
        try:
            producer.produce(topic, key=key, value=value, on_delivery=tracker.callback)
            producer.poll(0)
            tracker.check()
            return
        except BufferError as exc:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise KafkaLabError("Producer queue stayed full until the enqueue deadline.") from exc
            producer.poll(min(0.1, remaining))


def produce(bootstrap: str, topic: str, *, kind: str = "order", count: int | None = 20,
            interval: float = 0.05, timeout: float = 15, duplicate_every: int = 0,
            run_id: str | None = None, start_time: datetime | None = None,
            burst: bool = False, seed: int = 7) -> dict[str, Any]:
    """Count logical events; intentional duplicates add physical Kafka records."""
    if count is not None and count <= 0:
        raise ValueError("count must be positive, or None for an explicit continuous run")
    kafka = _kafka()
    _topic_metadata(_admin(bootstrap, timeout), topic, timeout)
    producer = kafka.Producer({
        "bootstrap.servers": bootstrap, "client.id": "streaming-lab-producer",
        "enable.idempotence": True, "acks": "all",
        "message.timeout.ms": max(1000, int(timeout * 1000)),
        "socket.timeout.ms": max(1000, int(timeout * 1000)),
        "queue.buffering.max.messages": 1000, "queue.buffering.max.kbytes": 1024,
    })
    tracker = DeliveryTracker()
    run_id = run_id or str(uuid.uuid4())
    start_time = parse_time(start_time or datetime.now(timezone.utc))
    logical = physical = 0
    stopped = "count" if count is not None else "interrupt"
    primary_error: BaseException | None = None
    try:
        while count is None or logical < count:
            event = make_event(kind, logical, run_id=run_id, start_time=start_time,
                               interval=interval, burst=burst, seed=seed)
            key, value = encode_event(event)
            _enqueue(producer, tracker, topic, key, value, timeout)
            physical += 1
            logical += 1
            if duplicate_every and logical % duplicate_every == 0:
                _enqueue(producer, tracker, topic, key, value, timeout)
                physical += 1
            until = time.monotonic() + interval
            while time.monotonic() < until:
                producer.poll(min(0.1, max(0, until - time.monotonic())))
                tracker.check()
    except KeyboardInterrupt:
        stopped = "interrupt"
    except BaseException as exc:
        primary_error = exc
    try:
        flush_checked(producer, tracker, timeout)
    except Exception as exc:
        if primary_error is not None:
            raise KafkaLabError(f"{primary_error}; final delivery check also failed: {exc}") from exc
        raise
    if primary_error is not None:
        raise primary_error
    return {"topic": topic, "bootstrap": bootstrap, "run_id": run_id,
            "start_time": start_time.isoformat(), "logical_events": logical,
            "physical_records": physical, "acknowledged": tracker.acknowledged,
            "failed": tracker.failed, "stop_reason": stopped}


def bind_database(db: Path, *, bootstrap: str, cluster_id: str | None,
                  topic: str, group: str, checkpoint: int) -> None:
    """A DB belongs to one broker/topic/partition/group, not an arbitrary run."""
    binding = (",".join(sorted(part.strip() for part in bootstrap.split(","))),
               cluster_id or "", topic, 0, group)
    with sqlite3.connect(db, timeout=5) as connection:
        connection.execute("""CREATE TABLE IF NOT EXISTS kafka_source_binding (
            singleton INTEGER PRIMARY KEY CHECK(singleton=1), bootstrap TEXT NOT NULL,
            cluster_id TEXT NOT NULL, topic TEXT NOT NULL, partition_id INTEGER NOT NULL,
            consumer_group TEXT NOT NULL)""")
        existing = connection.execute("""SELECT bootstrap,cluster_id,topic,partition_id,consumer_group
            FROM kafka_source_binding WHERE singleton=1""").fetchone()
        if existing is None:
            if checkpoint != -1:
                raise KafkaLabError("This nonempty DB has no Kafka source binding. Use a fresh DB path.")
            connection.execute("INSERT INTO kafka_source_binding VALUES(1,?,?,?,?,?)", binding)
        elif existing != binding:
            raise KafkaLabError(
                "DB source binding differs from this broker/topic/group. "
                "Use the original group and DB together, or a new group with its own new DB."
            )


def choose_start_offset(*, checkpoint: int, committed: int, low: int, high: int) -> int:
    """Validate the broker's resume point without silently skipping local work."""
    local_next = checkpoint + 1
    if low > local_next:
        raise KafkaLabError("Kafka retention removed records needed by this DB; a complete replay is unavailable.")
    if high < local_next:
        raise KafkaLabError("SQLite is ahead of this topic log. The topic may have been recreated.")
    start = committed if committed >= 0 else low
    if start > local_next:
        raise KafkaLabError("The group's Kafka commit is ahead of SQLite. Restore the matching DB or use a new group + DB.")
    if start < low:
        # It is safe to start at low only if SQLite already covers the missing range.
        # Reject rather than introduce an implicit offset reset in this teaching lab.
        raise KafkaLabError("The group's committed offset has expired. This lab does not reset offsets automatically.")
    return start


def decode_payload(value: bytes | None) -> dict[str, Any]:
    """Bad transport payloads become durable local dead letters, not poison loops."""
    try:
        if value is None:
            raise ValueError("Kafka tombstones are outside this lab's event schema")
        payload = json.loads(value.decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("JSON event must be an object")
        return payload
    except (UnicodeDecodeError, ValueError) as exc:
        return {"kafka_decode_error": str(exc),
                "raw_utf8_preview": value[:4096].decode("utf-8", errors="replace") if value else None}


def process_message(engine: StreamEngine, consumer: Any, message: Any, *, topic: str,
                    topic_partition: Callable[..., Any], crash_before_broker_commit: bool = False) -> dict[str, Any]:
    """The ordering of these two operations is the central recovery contract."""
    if message.topic() != topic or message.partition() != 0:
        raise KafkaLabError("Unexpected source partition; each DB follows exactly one topic partition.")
    if message.error() is not None:
        raise KafkaLabError(f"Kafka read failed: {message.error()}")
    result = engine.ingest(decode_payload(message.value()), offset=message.offset())
    if crash_before_broker_commit:
        raise CrashBeforeBrokerCommit(
            f"Injected stop after SQLite offset {message.offset()} committed, before Kafka commit. "
            "Restart with the SAME group and DB and without --crash-before-broker-commit."
        )
    try:
        committed = consumer.commit(
            offsets=[topic_partition(topic, 0, message.offset() + 1)], asynchronous=False
        )
        for partition in committed or []:
            if partition.error is not None:
                raise KafkaLabError(f"Offset commit failed: {partition.error}")
    except Exception as exc:
        raise KafkaLabError(
            f"SQLite contains offset {message.offset()}, but Kafka commit failed: {exc}. "
            "Restart with the same group and DB; replay is safe."
        ) from exc
    return result


def consume(bootstrap: str, topic: str, group: str, db: Path, *, count: int | None = 20,
            seconds: float | None = None, timeout: float = 30,
            crash_before_broker_commit: bool = False) -> dict[str, Any]:
    """Count mode fails on timeout; seconds mode is a bounded observation."""
    if count is not None and count <= 0:
        raise ValueError("count must be positive")
    kafka = _kafka()
    metadata = _topic_metadata(_admin(bootstrap, timeout), topic, timeout)
    db = Path(db)
    db.parent.mkdir(parents=True, exist_ok=True)
    consumer = kafka.Consumer({
        "bootstrap.servers": bootstrap, "group.id": group,
        "client.id": f"streaming-lab-{group}",
        "enable.auto.commit": False, "enable.auto.offset.store": False,
        "auto.offset.reset": "error", "allow.auto.create.topics": False,
        "partition.assignment.strategy": "range",
        "socket.timeout.ms": max(1000, int(timeout * 1000)),
    })
    consumed = 0
    replayed = 0
    assigned = False
    started = time.monotonic()
    deadline = started + (seconds if seconds is not None else timeout) if count is not None or seconds is not None else None
    stop_reason = "count" if count is not None else "seconds" if seconds is not None else "interrupt"
    try:
        with StreamEngine(db) as engine:
            bind_database(db, bootstrap=bootstrap, cluster_id=getattr(metadata, "cluster_id", None),
                          topic=topic, group=group, checkpoint=engine.snapshot()["checkpoint"])

            def on_assign(client: Any, partitions: list[Any]) -> None:
                nonlocal assigned
                if not partitions:
                    return
                if len(partitions) != 1 or partitions[0].topic != topic or partitions[0].partition != 0:
                    raise KafkaLabError("This lab expects one assigned topic partition.")
                partition = partitions[0]
                low, high = client.get_watermark_offsets(partition, timeout=timeout)
                stored = client.committed(partitions, timeout=timeout)[0]
                if stored.error is not None:
                    raise KafkaLabError(f"Cannot inspect committed offset: {stored.error}")
                partition.offset = choose_start_offset(
                    checkpoint=engine.snapshot()["checkpoint"], committed=stored.offset, low=low, high=high)
                client.assign(partitions)
                assigned = True

            consumer.subscribe([topic], on_assign=on_assign)
            try:
                while count is None or consumed < count:
                    remaining = deadline - time.monotonic() if deadline is not None else 0.25
                    if deadline is not None and remaining <= 0:
                        if not assigned:
                            raise KafkaLabError("No partition was assigned before the deadline. Check broker/group membership.")
                        if count is not None:
                            raise KafkaLabError(f"Consumer timed out after {consumed}/{count} records; no implicit offset reset.")
                        break
                    message = consumer.poll(min(0.25, max(0, remaining)))
                    if message is None:
                        continue
                    result = process_message(
                        engine, consumer, message, topic=topic, topic_partition=kafka.TopicPartition,
                        crash_before_broker_commit=crash_before_broker_commit,
                    )
                    consumed += 1
                    replayed += result["status"] == "replayed"
            except KeyboardInterrupt:
                stop_reason = "interrupt"
            snapshot = engine.snapshot()
    finally:
        # Auto commit is disabled; closing after a failure never acknowledges it.
        consumer.close()
    return {"topic": topic, "partition": 0, "group": group, "db": str(db.resolve()),
            "consumed": consumed, "replayed": replayed, "stop_reason": stop_reason,
            "elapsed_s": round(time.monotonic() - started, 3), "snapshot": snapshot}


def _positive(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def _nonnegative(value: str) -> int:
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError("must be >= 0")
    return number


def _duration(value: str) -> float:
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("must be a finite number > 0")
    return number


def _interval(value: str) -> float:
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise argparse.ArgumentTypeError("must be a finite number >= 0")
    return number


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)
    topics = commands.add_parser("topics", help="explicitly create/verify one-partition topics")
    topics.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    topics.add_argument("--topics", nargs="+", default=list(TOPICS.values()))
    topics.add_argument("--timeout", type=_duration, default=15)
    sender = commands.add_parser("produce", help="send synthetic events with checked delivery")
    sender.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    sender.add_argument("--topic")
    sender.add_argument("--kind", choices=TOPICS, default="order")
    producer_mode = sender.add_mutually_exclusive_group()
    producer_mode.add_argument("--count", type=_positive)
    producer_mode.add_argument("--continuous", action="store_true")
    sender.add_argument("--interval", type=_interval, default=0.05)
    sender.add_argument("--timeout", type=_duration, default=15)
    sender.add_argument("--duplicate-every", type=_nonnegative, default=0)
    sender.add_argument("--run-id", help="reuse with the same start time to reproduce logical IDs")
    sender.add_argument("--start-time", help="timezone-aware ISO-8601 event time")
    sender.add_argument("--seed", type=int, default=7)
    sender.add_argument("--burst", action="store_true", help="all reviews are negative for product 5")
    reader = commands.add_parser("consume", help="durable SQLite ingest, then manual broker commit")
    reader.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    reader.add_argument("--topic", choices=None, default=TOPICS["order"])
    reader.add_argument("--group", required=True)
    reader.add_argument("--db", required=True, type=Path)
    consumer_mode = reader.add_mutually_exclusive_group()
    consumer_mode.add_argument("--count", type=_positive)
    consumer_mode.add_argument("--seconds", type=_duration)
    consumer_mode.add_argument("--continuous", action="store_true")
    reader.add_argument("--timeout", type=_duration, default=30)
    reader.add_argument("--crash-before-broker-commit", action="store_true")
    return root


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.command == "topics":
            report = create_topics(args.bootstrap, args.topics, timeout=args.timeout)
        elif args.command == "produce":
            if args.burst and args.kind != "review":
                raise ValueError("--burst is for --kind review")
            report = produce(
                args.bootstrap, args.topic or TOPICS[args.kind], kind=args.kind,
                count=None if args.continuous else args.count or 20,
                interval=args.interval, timeout=args.timeout, duplicate_every=args.duplicate_every,
                run_id=args.run_id, start_time=parse_time(args.start_time) if args.start_time else None,
                burst=args.burst, seed=args.seed,
            )
        else:
            report = consume(
                args.bootstrap, args.topic, args.group, args.db,
                count=None if args.continuous or args.seconds is not None else args.count or 20,
                seconds=args.seconds, timeout=args.timeout,
                crash_before_broker_commit=args.crash_before_broker_commit,
            )
        print(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False))
        return 0
    except CrashBeforeBrokerCommit as exc:
        print(f"INJECTED CRASH: {exc}", file=sys.stderr)
        return CRASH_EXIT_CODE
    except Exception as exc:
        print(f"Kafka lab failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
