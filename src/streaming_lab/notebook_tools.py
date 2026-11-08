"""Readable Hebrew notebook outputs and portable exports; no network access."""
from __future__ import annotations

import csv
import html
import json
from pathlib import Path
import re
from typing import Any, Iterable


STYLE = """
body {font-family:Arial,sans-serif;color:#15283e;background:#fff;max-width:1150px;margin:36px auto;padding:0 24px;line-height:1.65}
h1,h2 {color:#176c68} table {border-collapse:collapse;width:100%;margin:12px 0 24px;font-size:16px}
th,td {border:1px solid #d5dfe2;padding:10px 12px;text-align:right;vertical-align:top}
th {background:#176c68;color:white} tr:nth-child(even) td {background:#f5f8f8}
code,pre {direction:ltr;text-align:left} .note {color:#526575} .pass {color:#176c68}
"""


def table_html(rows: Iterable[dict[str, Any]], columns: list[tuple[str, str]] | None = None,
               *, limit: int | None = None, caption: str = "") -> str:
    rows = list(rows)
    if limit is not None:
        rows = rows[:limit]
    if columns is None:
        columns = [(key, key) for key in rows[0]] if rows else []
    header = "".join(f"<th>{html.escape(label)}</th>" for _, label in columns)
    body = []
    for row in rows:
        cells = []
        for key, _ in columns:
            value = row.get(key, "")
            if isinstance(value, (dict, list)):
                value = json.dumps(value, ensure_ascii=False)
            text = "" if value is None else str(value)
            direction = "rtl" if re.search(r"[\u0590-\u05ff]", text) else "ltr"
            cells.append(f'<td><span dir="{direction}" style="unicode-bidi:isolate">{html.escape(text)}</span></td>')
        body.append("<tr>" + "".join(cells) + "</tr>")
    if not body:
        body = [f'<tr><td colspan="{max(1,len(columns))}">אין רשומות להצגה</td></tr>']
    title = f"<h3>{html.escape(caption)}</h3>" if caption else ""
    return f'<div dir="rtl">{title}<table><thead><tr>{header}</tr></thead><tbody>{"".join(body)}</tbody></table></div>'


def show_table(rows: Iterable[dict[str, Any]], columns: list[tuple[str, str]] | None = None,
               *, limit: int | None = None, caption: str = "") -> None:
    from IPython.display import HTML, display
    display(HTML(f"<style>{STYLE}</style>" + table_html(rows, columns, limit=limit, caption=caption)))


def save_csv(path: Path, rows: list[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(value, ensure_ascii=False) if isinstance(value, (dict,list)) else value
                             for key, value in row.items()})
    return path


def save_report(run_dir: Path, *, comparison: list[dict[str, Any]], trace: list[dict[str, Any]],
                teaching: dict[str, Any], orders: dict[str, Any], reviews: dict[str, Any],
                live: dict[str, Any], checks: list[dict[str, Any]],
                microbatch_comparison: list[dict[str, Any]] | None = None,
                microbatch_trace: list[dict[str, Any]] | None = None) -> dict[str, Path]:
    """Export exactly the supplied run's data, never a hard-coded report."""
    run_dir.mkdir(parents=True, exist_ok=True)
    data = {
        "grace_comparison": comparison,
        "teaching_trace": trace,
        "revenue_windows": orders["revenue_windows"],
        "late_events": orders["late_events"],
        "dead_letters": teaching["dead_letters"],
        "review_alerts": reviews["alerts"],
        "checks": checks,
    }
    if microbatch_comparison is not None:
        data["microbatch_comparison"] = microbatch_comparison
    if microbatch_trace is not None:
        data["microbatch_trace"] = microbatch_trace
    files = {name: save_csv(run_dir / f"{name}.csv", rows) for name, rows in data.items()}
    metrics = {
        "teaching": {"totals": teaching["totals"], "stats": teaching["stats"]},
        "orders": {"totals": orders["totals"], "stats": orders["stats"]},
        "reviews": {"totals": reviews["totals"], "stats": reviews["stats"]},
        "grace_comparison": comparison,
        "live": {key:value for key,value in live.items() if key != "snapshot"},
        "checks": checks,
        "microbatch_comparison": microbatch_comparison or [],
        "microbatch_trace": microbatch_trace or [],
    }
    metrics_path = run_dir / "results.json"
    metrics_path.write_text(json.dumps(metrics, ensure_ascii=False, indent=2), encoding="utf-8")
    files["results"] = metrics_path
    sections = ["<h1>תוצאות מעבדת Streaming</h1>",
                '<p class="note">נתונים סינתטיים לצורכי לימוד. כל התוצאות בדוח נוצרו בהרצת המחברת הנוכחית.</p>',
                "<h2>השוואת מדיניות האיחור</h2>" + table_html(comparison),
                "<h2>רשומות בודדות וקבוצות קטנות</h2>" + table_html(microbatch_comparison or []),
                "<h2>מתי הופעלה כל קבוצה</h2>" + table_html(microbatch_trace or []),
                "<h2>חלונות הכנסות</h2>" + table_html(orders["revenue_windows"],
                    [("window_start","תחילת החלון"),("orders","הזמנות"),("revenue_cents","הכנסה באגורות"),("closure_reason","סיבת סגירה")]),
                "<h2>אירועים מאוחרים שנשמרו</h2>" + table_html(orders["late_events"],
                    [("event_id","מזהה"),("event_time","זמן האירוע"),("amount_cents","סכום באגורות")]),
                "<h2>התראות ביקורות</h2>" + table_html(reviews["alerts"]),
                "<h2>בדיקות המחברת</h2>" + table_html(checks),
                "<h2>קובצי תוצאות</h2><ul>" + "".join(f'<li><a href="{path.name}">{path.name}</a></li>' for path in files.values()) + "</ul>"]
    report = run_dir / "report.html"
    report.write_text('<!doctype html><html lang="he" dir="rtl"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Streaming Lab Results</title><style>' + STYLE + '</style></head><body>' + "\n".join(sections) + "</body></html>", encoding="utf-8")
    files["report"] = report
    return files
