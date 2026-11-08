"""`python -m streaming_lab` or the installed `streaming-lab` command."""
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
import sqlite3

from .engine import InjectedCrash, StreamEngine
from .events import batch_truth, generate_order_fixture, teaching_fixture
from .live import run_live
from .reporting import canonical_results, format_summary, fresh_run_directory


def _config(args: argparse.Namespace) -> dict[str, int]:
    return {"window_seconds": args.window, "grace_seconds": args.grace,
            "out_of_order_seconds": args.out_of_order, "alert_threshold": args.alert_threshold}


def _db(args: argparse.Namespace, prefix: str) -> Path:
    return Path(args.db) if args.db else fresh_run_directory(prefix=prefix) / "stream.db"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Short, reproducible streaming lab. Data lives in a fresh lab_runs/ directory by default.")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("fixture", "live", "recovery"):
        command = sub.add_parser(name)
        command.add_argument("--window", type=int, default=60)
        command.add_argument("--grace", type=int, default=10)
        command.add_argument("--out-of-order", type=int, default=0)
        command.add_argument("--alert-threshold", type=int, default=3)
        if name != "recovery":
            command.add_argument("--db", help="Existing compatible DB to resume, or a new DB path")
    fixture = sub.choices["fixture"]
    fixture.add_argument("--orders", type=int, default=300)
    fixture.add_argument("--teaching", action="store_true", help="17 mixed arrivals with hand-checkable expected values")
    fixture.add_argument("--verbose", action="store_true")
    live = sub.choices["live"]
    live.add_argument("--seconds", type=float, default=4.0)
    live.add_argument("--interval", type=float, default=0.05)
    live.add_argument("--processing-delay", type=float, default=0.0)
    live.add_argument("--queue-size", type=int, default=8)
    live.add_argument("--finalize", action="store_true", help="Explicitly declare bounded-demo end-of-input; otherwise windows stay open")
    recovery = sub.choices["recovery"]
    recovery.add_argument("--db-dir", default="lab_runs", help="Parent directory; a fresh unique child will be created")
    recovery.add_argument("--crash-offset", type=int, default=8)
    inspect_parser = sub.add_parser("inspect")
    inspect_parser.add_argument("--db", required=True)
    inspect_parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    if args.command == "inspect":
        path = Path(args.db)
        if not path.is_file():
            parser.error("--db must point to an existing streaming DB")
        con = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
        try:
            config = json.loads(con.execute("SELECT config_json FROM stream_meta WHERE singleton=1").fetchone()[0])
        finally:
            con.close()
        with StreamEngine(path, **config) as engine:
            snapshot = engine.snapshot()
        print(json.dumps(snapshot, ensure_ascii=False, indent=2) if args.json else format_summary(snapshot))
        return 0
    if args.command == "fixture":
        data = teaching_fixture() if args.teaching else generate_order_fixture(args.orders)
        path = _db(args, "fixture-")
        with StreamEngine(path, **_config(args)) as engine:
            for item in data:
                result = engine.ingest(item.event, offset=item.offset)
                if args.verbose:
                    print(json.dumps(result, ensure_ascii=False))
            snapshot = engine.finalize_demo()
        print(f"DB: {path.resolve()}")
        print(format_summary(snapshot, truth=batch_truth(data)))
        return 0
    if args.command == "live":
        path = _db(args, "live-")
        with StreamEngine(path, **_config(args)) as engine:
            result = asyncio.run(run_live(engine, duration_s=args.seconds, interval_s=args.interval,
                                          processing_delay_s=args.processing_delay, queue_maxsize=args.queue_size,
                                          finalize=args.finalize))
        print(f"DB: {path.resolve()}")
        print("Real asyncio tasks; synthetic event time advances 2 seconds per delivery.")
        print(json.dumps({key: value for key, value in result.items() if key != "snapshot"}, ensure_ascii=False, indent=2))
        print(format_summary(result["snapshot"]))
        return 0
    data = teaching_fixture()
    if not 0 <= args.crash_offset < len(data):
        parser.error(f"--crash-offset must be between 0 and {len(data)-1}")
    directory = fresh_run_directory(args.db_dir, prefix="recovery-")
    outputs = {}
    for stage in ("baseline", "before_commit", "after_commit"):
        path = directory / f"{stage}.db"
        with StreamEngine(path, **_config(args)) as engine:
            try:
                for item in data:
                    engine.ingest(item.event, offset=item.offset,
                                  crash_at=stage if stage != "baseline" and item.offset == args.crash_offset else None)
            except InjectedCrash as exc:
                print(str(exc))
        # Reopening the DB proves recovery uses durable state, not a Python object.
        with StreamEngine(path, **_config(args)) as engine:
            checkpoint = engine.checkpoint
            for item in data:
                engine.ingest(item.event, offset=item.offset)
            outputs[stage] = canonical_results(engine.finalize_demo())
        print(f"{stage}: restarted at checkpoint {checkpoint}")
    equal = outputs["baseline"] == outputs["before_commit"] == outputs["after_commit"]
    print(f"DB directory: {directory.resolve()}")
    print(f"Recovery matches baseline: {equal}")
    print(format_summary(outputs["baseline"], truth=batch_truth(data)))
    return 0 if equal else 1


if __name__ == "__main__":
    raise SystemExit(main())
