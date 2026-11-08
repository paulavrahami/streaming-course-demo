# Validation record

Validated locally and in GitHub Actions on 2026-11-08 using **CPython 3.13.12**. All input is synthetic. This file distinguishes executed checks from remaining environment limitations.

## Local checks executed successfully

| Check | Result |
|---|---|
| Locked uv environment with the Kafka extra | Installed successfully |
| Core engine, events, live queue, micro-batches and recovery | 43 tests passed |
| Kafka producer, consumer, commit and resume contracts | 21 tests passed |
| Entire notebook, executed sequentially with actual IPython and top-level await | 20 code cells, zero cell errors, 44 assertions passed |
| Real Kafka integration using Apache Kafka 4.4.0 and OpenJDK 21.0.13 | All 9 smoke scenarios passed |
| Standalone JSON, CSV and HTML exports from the notebook | Created from the successful run |

The notebook used in-process IPython execution, including rich display output and asynchronous cells. This executes the actual cell source in order; it is not a static notebook inspection. It does **not** validate the separate Jupyter kernel transport.

## GitHub Actions verification

[Workflow run 42453821564](https://github.com/paulavrahami/streaming-course-demo/actions/runs/42453821564) completed successfully on 2026-11-08 for code commit `80755923cd49521ecea51a55093519b182bbfc00`.

| Job | Verified result |
|---|---|
| Ubuntu notebook | 64 unit tests passed; all 20 notebook code cells executed in a separate Jupyter kernel with zero cell errors |
| Windows notebook | 64 unit tests passed; all 20 notebook code cells executed in a separate Jupyter kernel with zero cell errors |
| Linux broker | All 9 real Kafka smoke scenarios passed using the pinned Redpanda Compose deployment; test resources were cleaned up |

Both notebook jobs used Python 3.13.12 and the default TCP kernel transport. The run includes the executed notebooks and execution summaries as downloadable artifacts. These checks validate native Windows execution, but do not exercise the interactive Cursor UI or Docker Desktop on a teaching computer.

The first remote run exposed two portability problems, fixed before the successful run: a SQLite connection needed explicit closure to release its file on Windows, and the disposable broker needed consistent IPv4 loopback addresses for bootstrap and advertised metadata. The successful run verified both fixes.

## Reproducible observations

The 12-order comparison produced 12 individual display updates and 3 grouped updates. The observed batch sizes were 5, 5 and 2. Both modes persisted the same canonical business state and **1,266 cents** of revenue. A separate slow-source experiment released records on the collection timer before a group filled.

The 300-order fixture contains 317 deliveries with 17 logical duplicates. With grace values 0, 10 and 120, it accepted 279, 286 and 300 orders respectively. Accepted revenue plus preserved late revenue matched the complete 6,124,170-cent input total in each case.

The real broker smoke test produced 24 Kafka order records representing 20 logical orders and 367,630 cents. It ignored four intentional duplicates. The review stream produced 12 Kafka records representing 9 logical negative reviews and emitted one durable alert. The test covered delivery acknowledgements, manual offset commit, restart, independent groups, a deliberately mismatched database, bounded timeout and an injected process exit between SQLite commit and Kafka commit.

The test created unique topics and groups and removed those resources when finished. The disposable broker process was then stopped. Its downloaded distribution is not part of the course project.

## Remaining environment limitations

| Environment or path | Status |
|---|---|
| Interactive Cursor notebook UI on Windows | Instructions provided; native Windows code and notebook execution passed in GitHub Actions, but the editor UI was not exercised |
| Docker Desktop on Windows | Not executed; the pinned Redpanda Compose deployment passed on the Linux GitHub runner |
| Separate Jupyter kernel protocol in this executor | Blocked before the first cell by an executor restriction |

The Jupyter limitation was isolated: ordinary Python TCP loopback communication worked, but native network-interface enumeration returned `Operation not permitted`. The installed libzmq calls that enumeration during local TCP binding, even for the literal address 127.0.0.1, and aborts. UNIX-domain socket binding is also restricted. No dependency, privilege or access-control workaround was applied. This does not imply that the notebook failed in a cell or that a normal Windows Jupyter installation has the same restriction.

`scripts/run_notebook.py` runs the notebook with a real kernel using the active project interpreter. The GitHub workflow successfully performed this check on Windows and Linux. Run it on the teaching computer before class to confirm that computer's environment as well.

## Evidence included

- `examples/verified-run/results.json`: actual notebook metrics and assertions.
- `examples/verified-run/report.html`: readable results from the same run.
- `examples/verified-run/*.csv`: actual exported tables, including the individual/group comparison.
- `examples/validation_summary.json`: machine-readable summary with the execution mode identified.
- `examples/kafka_smoke_result.json`: real broker checks and business totals.

Runtime counts and collection durations are observations from the validation machine, not timing guarantees for another computer. Source-controlled fixtures and money totals are deterministic.
