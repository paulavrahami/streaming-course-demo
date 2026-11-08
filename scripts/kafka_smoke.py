#!/usr/bin/env python3
"""Real Kafka smoke test with bounded commands and resources owned by this run.

Default: starts a separate Compose project, then removes only that project's
containers and volume. --bootstrap uses an existing disposable broker and
removes only this run's unique topics and consumer groups. Never resets a group,
formats a broker, or deletes another lab's DB. Requires the optional kafka extra.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import uuid

from streaming_lab import StreamEngine
from streaming_lab.kafka_demo import CRASH_EXIT_CODE, KafkaLabError, create_topics

ROOT = Path(__file__).resolve().parents[1]


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def run_command(command, *, env=None, timeout=90, expected_code=0):
    completed = subprocess.run(command, cwd=ROOT, env=env, capture_output=True,
                               text=True, encoding="utf-8", errors="replace", timeout=timeout)
    if completed.returncode != expected_code:
        raise RuntimeError(
            f"Command failed ({completed.returncode}, expected {expected_code}): {command}\n"
            f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )
    return completed


def available_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def cli(*arguments, expected_code=0, timeout=75):
    environment = os.environ.copy()
    environment["PYTHONUTF8"] = "1"
    result = run_command([sys.executable, "-m", "streaming_lab.kafka_demo", *map(str, arguments)],
                         env=environment, timeout=timeout, expected_code=expected_code)
    return json.loads(result.stdout) if expected_code == 0 else result.stderr


def read_state(path):
    with StreamEngine(path) as engine:
        return engine.snapshot()


def business_state(report):
    snapshot = report["snapshot"]
    return {key: snapshot[key] for key in (
        "checkpoint", "stats", "totals", "open_windows", "open_review_windows",
        "revenue_windows", "review_windows", "alerts", "late_events", "dead_letters",
    )}


def clean_external_resources(bootstrap, topics, groups):
    """Delete exact run-scoped names after every consumer subprocess has exited."""
    from confluent_kafka import KafkaError, KafkaException
    from confluent_kafka.admin import AdminClient
    admin = AdminClient({"bootstrap.servers": bootstrap, "socket.timeout.ms": 10000})
    errors = []
    if groups:
        futures = admin.delete_consumer_groups(groups, request_timeout=10)
        for name, future in futures.items():
            try:
                future.result(timeout=12)
            except KafkaException as exc:
                if not exc.args or exc.args[0].code() != KafkaError.GROUP_ID_NOT_FOUND:
                    errors.append(f"group {name}: {exc}")
            except Exception as exc:
                errors.append(f"group {name}: {exc}")
    if topics:
        futures = admin.delete_topics(topics, operation_timeout=10, request_timeout=10)
        for name, future in futures.items():
            try:
                future.result(timeout=12)
            except KafkaException as exc:
                if not exc.args or exc.args[0].code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                    errors.append(f"topic {name}: {exc}")
            except Exception as exc:
                errors.append(f"topic {name}: {exc}")
    if errors:
        raise RuntimeError("Smoke cleanup incomplete; only these run-owned names were targeted: "
                           + "; ".join(errors))


def exercise(bootstrap, topics, groups, directory):
    orders, reviews = topics
    baseline_group, crash_group, independent_group, review_group = groups
    baseline_db = directory / "baseline.sqlite"
    crash_db = directory / "crash.sqlite"
    independent_db = directory / "independent.sqlite"
    review_db = directory / "reviews.sqlite"
    start = "2026-11-01T10:00:00+00:00"
    completed = []

    def mark(name):
        completed.append(name)
        print(f"PASS {name}", file=sys.stderr, flush=True)

    again = create_topics(bootstrap, topics)
    check(again["created"] == [], "Repeated topic provisioning unexpectedly created a topic")
    mark("explicit_topic_creation_is_repeatable")

    produced = cli("produce", "--bootstrap", bootstrap, "--topic", orders,
                   "--count", 20, "--duplicate-every", 5, "--interval", 0,
                   "--run-id", f"{orders}-events", "--start-time", start)
    check(produced["logical_events"] == 20 and produced["physical_records"] == 24,
          "Expected 20 logical orders and 4 intentional duplicates")
    check(produced["acknowledged"] == 24 and produced["failed"] == 0,
          "Producer did not acknowledge every physical record")
    mark("producer_delivery_acknowledgements")

    baseline = cli("consume", "--bootstrap", bootstrap, "--topic", orders,
                   "--group", baseline_group, "--db", baseline_db, "--count", 24, "--timeout", 40)
    check(baseline["snapshot"]["totals"]["orders"] == 20, "Logical duplicates changed the order count")
    check(baseline["snapshot"]["stats"]["duplicates"] == 4, "Expected four semantic duplicates")
    check(baseline["snapshot"]["checkpoint"] == 23, "Unexpected persisted source checkpoint")
    check(not baseline["snapshot"]["finalized"], "Idle broker stream must not be finalized")
    check(len(baseline["snapshot"]["revenue_windows"]) == 0, "An idle stream closed event-time windows")
    mark("sqlite_effect_and_manual_offset_commit")

    resumed = cli("consume", "--bootstrap", bootstrap, "--topic", orders,
                  "--group", baseline_group, "--db", baseline_db, "--seconds", 5, "--timeout", 15)
    check(resumed["consumed"] == 0 and business_state(resumed) == business_state(baseline),
          "Same-group restart reread committed records or changed state")
    mark("same_group_restart_resumes_after_last_commit")

    failure = cli("consume", "--bootstrap", bootstrap, "--topic", orders,
                  "--group", crash_group, "--db", crash_db, "--count", 24, "--timeout", 40,
                  "--crash-before-broker-commit", expected_code=CRASH_EXIT_CODE)
    check("INJECTED CRASH" in failure, "Expected deliberate crash exit")
    check(read_state(crash_db)["checkpoint"] == 0, "Crash lost the committed SQLite effect")
    recovered = cli("consume", "--bootstrap", bootstrap, "--topic", orders,
                    "--group", crash_group, "--db", crash_db, "--count", 24, "--timeout", 40)
    check(recovered["replayed"] == 1, "The uncommitted Kafka record was not replayed")
    check(business_state(recovered) == business_state(baseline), "Crash recovery changed business results")
    mark("post_sqlite_pre_kafka_crash_recovers_without_double_counting")

    independent = cli("consume", "--bootstrap", bootstrap, "--topic", orders,
                      "--group", independent_group, "--db", independent_db, "--count", 24, "--timeout", 40)
    check(business_state(independent) == business_state(baseline), "Independent group did not replay full history")
    mark("independent_group_with_own_database")

    lost_db_error = cli("consume", "--bootstrap", bootstrap, "--topic", orders,
                        "--group", baseline_group, "--db", directory / "wrong-empty.sqlite",
                        "--count", 1, "--timeout", 15, expected_code=1)
    check("ahead of SQLite" in lost_db_error, "Missing DB was not rejected before skipping past history")
    mark("broker_commit_ahead_of_database_is_rejected")

    timeout_started = time.monotonic()
    timeout_error = cli("consume", "--bootstrap", bootstrap, "--topic", orders,
                        "--group", baseline_group, "--db", baseline_db,
                        "--count", 1, "--timeout", 5, expected_code=1, timeout=30)
    check("timed out" in timeout_error or "deadline" in timeout_error, "Idle count mode did not report timeout")
    check(time.monotonic() - timeout_started < 30, "Count mode did not terminate within its external bound")
    mark("count_mode_is_bounded_and_fails_when_records_are_missing")

    review_produced = cli("produce", "--bootstrap", bootstrap, "--topic", reviews,
                         "--kind", "review", "--burst", "--count", 9, "--duplicate-every", 3,
                         "--interval", 0, "--run-id", f"{reviews}-events", "--start-time", start)
    check(review_produced["acknowledged"] == 12, "Review producer acknowledgements missing")
    review_result = cli("consume", "--bootstrap", bootstrap, "--topic", reviews,
                       "--group", review_group, "--db", review_db, "--count", 12, "--timeout", 40)
    check(review_result["snapshot"]["totals"]["reviews"] == 9, "Review replay inflated count")
    check(review_result["snapshot"]["totals"]["negative_reviews"] == 9, "Negative review count mismatch")
    check(len(review_result["snapshot"]["alerts"]) == 1, "Expected one durable alert per product/window")
    mark("negative_review_alert_is_durable_and_deduplicated")
    return {"status": "passed", "bootstrap": bootstrap, "checks": completed,
            "order_totals": baseline["snapshot"]["totals"],
            "review_totals": review_result["snapshot"]["totals"],
            "review_alerts": len(review_result["snapshot"]["alerts"])}


def main():
    options = argparse.ArgumentParser(description=__doc__)
    options.add_argument("--bootstrap", help="use an existing disposable broker instead of starting Compose")
    args = options.parse_args()
    try:
        import confluent_kafka  # noqa: F401
    except ImportError:
        options.error("Install the client first: uv sync --extra kafka; run with uv run --extra kafka python ...")
    if args.bootstrap is None and shutil.which("docker") is None:
        options.error("Docker is unavailable. Install Docker Desktop/Engine, or provide --bootstrap for a disposable broker.")
    unique = uuid.uuid4().hex[:12]
    project = f"streaming-smoke-{unique}"
    topics = [f"{project}-orders", f"{project}-reviews"]
    groups = [f"{project}-{suffix}" for suffix in ("baseline", "crash", "independent", "reviews")]
    environment = os.environ.copy()
    environment["PYTHONUTF8"] = "1"
    compose = ["docker", "compose", "--project-name", project, "--file", str(ROOT / "compose.yaml")]
    bootstrap = args.bootstrap
    started_compose = False
    reserved_topics = []
    primary_error = None
    report = None
    try:
        if bootstrap is None:
            kafka_port, console_port = available_port(), available_port()
            while console_port == kafka_port:
                console_port = available_port()
            environment["KAFKA_HOST_PORT"] = str(kafka_port)
            environment["CONSOLE_HOST_PORT"] = str(console_port)
            # Compose publishes this listener on IPv4 loopback only. Its
            # advertised external address must use the same numeric host.
            bootstrap = f"127.0.0.1:{kafka_port}"
            started_compose = True
            run_command([*compose, "up", "--detach", "--wait", "--wait-timeout", "120"],
                        env=environment, timeout=180)
        from confluent_kafka.admin import AdminClient
        metadata = AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=15)
        check(not any(name in metadata.topics for name in topics), "Run-scoped topics unexpectedly already exist")
        # These names were observed absent, are unique, and belong only to this run.
        reserved_topics = topics.copy()
        created = create_topics(bootstrap, topics)
        check(set(created["created"]) == set(topics), "Expected explicit creation of both fresh topics")
        with tempfile.TemporaryDirectory(prefix=f"{project}-") as temporary:
            report = exercise(bootstrap, topics, groups, Path(temporary))
    except Exception as exc:
        primary_error = exc
        if started_compose:
            try:
                logs = run_command([*compose, "logs", "--no-color", "--tail", "80"],
                                   env=environment, timeout=15)
                print(logs.stdout, file=sys.stderr)
            except Exception as log_error:
                print(f"Could not collect Compose logs: {log_error}", file=sys.stderr)
    finally:
        try:
            if started_compose:
                run_command([*compose, "down", "--volumes", "--remove-orphans", "--timeout", "10"],
                            env=environment, timeout=60)
            elif bootstrap and reserved_topics:
                clean_external_resources(bootstrap, reserved_topics, groups)
        except Exception as cleanup_error:
            print(f"Cleanup failed: {cleanup_error}", file=sys.stderr)
            primary_error = primary_error or cleanup_error
    if primary_error:
        print(f"Kafka smoke FAILED: {primary_error}", file=sys.stderr)
        return 1
    report["cleanup"] = "removed this run's dedicated resources"
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
