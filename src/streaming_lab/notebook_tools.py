"""Readable Hebrew notebook outputs and portable exports; no network access."""
from __future__ import annotations

import csv
import html
import json
from pathlib import Path
import re
from typing import Any, Iterable


STYLE = """
.streaming-output {
  color-scheme:dark; direction:rtl; box-sizing:border-box;
  font-family:Arial,"Segoe UI",sans-serif; font-size:15px; line-height:1.6;
  color:#e6edf7; background:#101c2c; border:1px solid #365169;
  border-radius:12px; margin:12px 0 22px; max-width:100%; min-width:0;
  overflow:hidden; box-shadow:0 6px 20px #07101b24;
}
.streaming-output .sl-scroll {
  overflow-x:auto; max-width:100%; scrollbar-color:#527b95 #101c2c;
}
.streaming-output .sl-scroll:focus-visible {outline:3px solid #7dd3fc; outline-offset:-3px}
.streaming-output table {
  border-collapse:separate; border-spacing:0; width:100%; margin:0;
  font-family:inherit; font-size:inherit; line-height:inherit;
  color:#e6edf7; background:#13253a; direction:rtl;
}
.streaming-output caption {
  caption-side:top; text-align:right; padding:14px 16px;
  font-size:17px; font-weight:700; color:#f1f7ff; background:#101c2c;
}
.streaming-output th, .streaming-output td {
  box-sizing:border-box; padding:12px 16px; text-align:right; vertical-align:top;
  border:0; border-bottom:1px solid #365169; border-inline-end:1px solid #365169;
  overflow-wrap:anywhere; max-width:32rem; font-variant-numeric:tabular-nums;
}
.streaming-output th {background:#155e63; color:#f0fdfa; font-weight:700}
.streaming-output td {background:#13253a; color:#e6edf7}
.streaming-output tbody tr:nth-child(even) td {background:#1c3046; color:#e6edf7}
.streaming-output tbody tr:hover td {background:#29425b; color:#f1f7ff}
.streaming-output tr > :last-child {border-inline-end:0}
.streaming-output tbody tr:last-child td {border-bottom:0}
.streaming-output .sl-value {unicode-bidi:isolate; white-space:normal}
.streaming-output .sl-badge {
  display:inline-block; border:1px solid; border-radius:6px; padding:2px 9px;
  font-weight:700; font-size:0.92em; white-space:nowrap; unicode-bidi:isolate;
}
.streaming-output .sl-success {background:#103d34; color:#a7f3d0; border-color:#2c8064}
.streaming-output .sl-warning {background:#49371b; color:#fde68a; border-color:#977337}
.streaming-output .sl-late {background:#492b3a; color:#fecdd3; border-color:#a75a75}
.streaming-output .sl-danger {background:#4c272c; color:#fecaca; border-color:#a85a64}
.streaming-output .sl-info {background:#253653; color:#bfdbfe; border-color:#587cad}
.streaming-output .sl-empty {color:#b8c9db; padding:20px 16px}
@media (max-width:640px) {
  .streaming-output {font-size:14px}
  .streaming-output th, .streaming-output td {padding:10px 12px}
}
"""

# Page layout belongs only to exported reports. Notebook outputs must not
# restyle the editor, surrounding markdown, or another cell's tables.
REPORT_STYLE = """
body.streaming-report {margin:0; background:#0b1422; color:#e6edf7; color-scheme:dark;
  font-family:Arial,"Segoe UI",sans-serif; line-height:1.7}
.streaming-report main {box-sizing:border-box; max-width:1200px; margin:0 auto; padding:32px 24px 48px}
.streaming-report .report-header {background:#13253a; border:1px solid #365169;
  border-top:4px solid #3fc7b2; border-radius:12px; padding:24px; margin-bottom:28px}
.streaming-report h1 {font-size:30px; color:#f1f7ff; margin:0 0 10px}
.streaming-report h2 {font-size:21px; color:#a7f3d0; margin:30px 0 12px;
  padding-inline-start:12px; border-inline-start:4px solid #3fc7b2}
.streaming-report .note {color:#bacbdb; margin:0}
.streaming-report a {color:#7dd3fc; text-underline-offset:3px}
.streaming-report a:hover {color:#c7edff}
.streaming-report code, .streaming-report pre {direction:ltr; text-align:left}
.streaming-report li {margin:6px 0; overflow-wrap:anywhere}
@media (max-width:640px) {
  .streaming-report main {padding:18px 12px 32px}
  .streaming-report .report-header {padding:18px}
  .streaming-report h1 {font-size:25px}
}
"""


def table_html(rows: Iterable[dict[str, Any]], columns: list[tuple[str, str]] | None = None,
               *, limit: int | None = None, caption: str = "", include_style: bool = True) -> str:
    """Return a scoped, self-contained table, including for live display updates."""
    rows = list(rows)
    if limit is not None:
        rows = rows[:limit]
    if columns is None:
        columns = [(key, key) for key in rows[0]] if rows else []
    header = "".join(f'<th scope="col"><bdi>{html.escape(label)}</bdi></th>' for _, label in columns)
    badges = {"accepted": "success", "duplicate": "warning", "late": "late",
              "invalid": "danger", "replayed": "info", "עבר": "success"}
    body = []
    for row in rows:
        cells = []
        for key, _ in columns:
            value = row.get(key, "")
            structured = isinstance(value, (dict, list))
            if structured:
                value = json.dumps(value, ensure_ascii=False)
            text = "" if value is None else str(value)
            path_like = re.match(r"^(?:[A-Za-z]:[\\/]|/|\\\\)", text)
            direction = "rtl" if not (structured or path_like) and re.search(r"[\u0590-\u05ff]", text) else "ltr"
            value_class = "sl-value"
            if text in badges:
                value_class += f" sl-badge sl-{badges[text]}"
            cells.append(f'<td><span dir="{direction}" class="{value_class}">{html.escape(text)}</span></td>')
        body.append("<tr>" + "".join(cells) + "</tr>")
    if not body:
        body = [f'<tr><td class="sl-empty" colspan="{max(1,len(columns))}">אין רשומות להצגה</td></tr>']
    title = f"<caption>{html.escape(caption)}</caption>" if caption else ""
    label = html.escape(caption or "טבלת תוצאות", quote=True)
    minimum_width = max(320, len(columns) * 130)
    style = f"<style>{STYLE}</style>" if include_style else ""
    return (style + '<div class="streaming-output" dir="rtl">'
            f'<div class="sl-scroll" role="region" aria-label="{label}" tabindex="0">'
            f'<table style="min-width:{minimum_width}px">{title}<thead><tr>{header}</tr></thead>'
            f'<tbody>{"".join(body)}</tbody></table></div></div>')


def show_table(rows: Iterable[dict[str, Any]], columns: list[tuple[str, str]] | None = None,
               *, limit: int | None = None, caption: str = "") -> None:
    from IPython.display import HTML, display
    display(HTML(table_html(rows, columns, limit=limit, caption=caption)))


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
    sections = ['<header class="report-header"><h1>תוצאות מעבדת Streaming</h1>',
                '<p class="note">נתונים סינתטיים לצורכי לימוד. כל התוצאות בדוח נוצרו בהרצת המחברת הנוכחית.</p></header>',
                "<h2>השוואת מדיניות האיחור</h2>" + table_html(comparison, include_style=False),
                "<h2>רשומות בודדות וקבוצות קטנות</h2>" + table_html(microbatch_comparison or [], include_style=False),
                "<h2>מתי הופעלה כל קבוצה</h2>" + table_html(microbatch_trace or [], include_style=False),
                "<h2>חלונות הכנסות</h2>" + table_html(orders["revenue_windows"],
                    [("window_start","תחילת החלון"),("orders","הזמנות"),("revenue_cents","הכנסה באגורות"),("closure_reason","סיבת סגירה")], include_style=False),
                "<h2>אירועים מאוחרים שנשמרו</h2>" + table_html(orders["late_events"],
                    [("event_id","מזהה"),("event_time","זמן האירוע"),("amount_cents","סכום באגורות")], include_style=False),
                "<h2>התראות ביקורות</h2>" + table_html(reviews["alerts"], include_style=False),
                "<h2>בדיקות המחברת</h2>" + table_html(checks, include_style=False),
                "<h2>קובצי תוצאות</h2><ul>" + "".join(f'<li><a href="{path.name}">{path.name}</a></li>' for path in files.values()) + "</ul>"]
    report = run_dir / "report.html"
    report.write_text('<!doctype html><html lang="he" dir="rtl"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Streaming Lab Results</title><style>' + STYLE + REPORT_STYLE + '</style></head><body class="streaming-report"><main>' + "\n".join(sections) + "</main></body></html>", encoding="utf-8")
    files["report"] = report
    return files
