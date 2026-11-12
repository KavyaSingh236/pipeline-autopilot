"""Weekly informational recovery summaries. No approval requests or per-incident email."""
from __future__ import annotations

import html
import os
from collections import Counter
from datetime import datetime

import resend
import structlog

log = structlog.get_logger(__name__)


def send_weekly_report(start: datetime, end: datetime, stats: list[dict], ai_summary: str = "") -> None:
    api_key = os.getenv("RESEND_API_KEY", "")
    recipient = os.getenv("ALERT_EMAIL", "")
    if not api_key or not recipient:
        raise RuntimeError("RESEND_API_KEY and ALERT_EMAIL must be configured")
    resend.api_key = api_key
    counts = Counter()
    errors = Counter()
    for entry in stats:
        n = int(entry["total"])
        counts[entry["status"]] += n
        errors[entry["error_type"]] += n
    total = sum(counts.values())
    recovered = counts["auto_fixed"]
    contained = counts["contained"]
    quarantined = counts["quarantined"]
    failed = counts["recovery_failed"]
    # Historical pending/approved records are counted, not rewritten.
    rows = "".join(
        f"<tr><td style='padding:8px'>{html.escape(str(k))}</td>"
        f"<td style='padding:8px;text-align:right'>{v}</td></tr>"
        for k, v in errors.most_common(8)
    ) or "<tr><td colspan='2'>No incidents recorded this week.</td></tr>"
    title = f"Pipeline Autopilot | Weekly recovery report | {start:%d %b}–{end:%d %b %Y}"
    body = f"""
    <div style='background:#0a0a0a;color:#f5f5f5;font-family:Arial,sans-serif;padding:24px'>
      <h2>Pipeline Autopilot · Weekly autonomous recovery</h2>
      <p>{start:%d %b %Y} to {end:%d %b %Y} (UTC)</p>
      <p><b>{total}</b> incidents · <b>{recovered}</b> auto-fixed ·
         <b>{failed}</b> recovery failed ·
         <b>{contained + quarantined}</b> historical protected incidents</p>
      <p>Unsuccessful recovery does not imply that missing upstream records were restored.</p>
      <h3>Summary</h3><p>{html.escape(ai_summary) if ai_summary else "Automated recovery statistics for the past seven days."}</p>
      <h3>Most frequent issues</h3>
      <table style='border-collapse:collapse;color:#f5f5f5'>{rows}</table>
      <p style='color:#aaa'>Informational report only. All recovery decisions are
         automated; no action or approval is requested.</p>
    </div>"""
    result = resend.Emails.send({
        "from": os.getenv("RESEND_FROM", "Pipeline Autopilot <onboarding@resend.dev>"),
        "to": [r.strip() for r in recipient.split(",") if r.strip()],
        "subject": title,
        "html": body,
    })
    if not result:
        raise RuntimeError("Resend returned no confirmation")
    log.info("weekly_report_sent", period=start.isoformat(), incidents=total)
