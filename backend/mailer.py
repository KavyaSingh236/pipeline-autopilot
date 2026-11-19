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
    success_pct = (100 * recovered / total) if total else None
    bar_colors = ["#8054f7", "#4e8af7", "#10bf96", "#f2a34a", "#b4a0fa"]
    chart = "".join(
        f"<tr><td style='padding:9px 4px;font-size:13px'>{html.escape(str(k).replace('_',' ').title())}</td>"
        f"<td style='width:54%;padding:9px'><div style='height:14px;background:#f1edfa;border-radius:9px'>"
        f"<div style='height:14px;width:{100*v/max(1,max(errors.values())):.1f}%;background:{bar_colors[i%5]};border-radius:9px'></div></div></td>"
        f"<td style='text-align:right;font-weight:bold'>{v:,}</td></tr>"
        for i,(k,v) in enumerate(errors.most_common(6))
    ) or "<tr><td>No incidents recorded in this reporting week.</td></tr>"
    body = f"""
    <div style='background:#f5f2ff;padding:32px 14px;font-family:Arial,Helvetica,sans-serif;color:#19184e'>
      <div style='max-width:690px;margin:auto;background:white;border-radius:22px;overflow:hidden;border:1px solid #e8e0ff'>
        <div style='padding:28px 32px;background:linear-gradient(110deg,#34247b,#8054f7);color:white'>
          <div style='font-size:15px;font-weight:bold;letter-spacing:1px'>✦ PIPELINE AUTOPILOT</div>
          <h1 style='margin:14px 0 8px;font-size:29px'>Weekly Performance Report</h1>
          <div style='opacity:.9'>{start:%d %b %Y} – {end:%d %b %Y} · UTC</div>
        </div>
        <div style='padding:28px 32px'>
          <h2 style='font-size:19px'>Executive Summary</h2>
          <p style='line-height:1.65;color:#5a5674'>{html.escape(ai_summary) if ai_summary else 'Weekly autonomous ETL monitoring and recovery activity, based on recorded incidents.'}</p>
          <table style='width:100%;border-spacing:9px;table-layout:fixed'><tr>
            <td style='padding:17px;background:#f1edff;border-radius:13px'><div style='font-size:12px'>Incidents</div><strong style='font-size:28px'>{total:,}</strong></td>
            <td style='padding:17px;background:#e9faf4;border-radius:13px'><div style='font-size:12px'>Auto-recovered</div><strong style='font-size:28px'>{recovered:,}</strong></td>
            <td style='padding:17px;background:#fff4e9;border-radius:13px'><div style='font-size:12px'>Recovery failed</div><strong style='font-size:28px'>{failed:,}</strong></td>
          </tr></table>
          <h2 style='font-size:19px;margin-top:28px'>Incident Type Distribution</h2>
          <table style='width:100%;border-collapse:collapse'>{chart}</table>
          <h2 style='font-size:19px;margin-top:28px'>Operational Insights</h2>
          <p style='line-height:1.65;color:#5a5674'>Observed auto-recovery share: {f'{success_pct:.1f}%' if success_pct is not None else 'not available'}. This describes recorded incident outcomes, not guaranteed pipeline uptime.</p>
          <p style='line-height:1.65;color:#5a5674'>Review recurring incident categories and quarantined rows to improve data completeness. Historical protected outcomes: {contained + quarantined:,}.</p>
          <h2 style='font-size:19px;margin-top:28px'>Recommendations</h2>
          <p style='line-height:1.65;color:#5a5674'>Continue automated validation and monitor the most frequent error categories. Detailed diagnosis and verification evidence remain available in the Audit Log.</p>
          <div style='border-top:1px solid #e8e0ff;margin-top:25px;padding-top:16px;font-size:12px;color:#807b9d'>Informational weekly report · No manual approvals requested · Figures are database aggregates</div>
        </div>
      </div>
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
