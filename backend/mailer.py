"""Critical-failure email via Resend. Only called for failures that need a human."""
from __future__ import annotations
import os
from datetime import datetime, timezone
import structlog

log = structlog.get_logger(__name__)


def send_alert(pipeline_name, pipeline_id, error_type, description, root_cause, proposed_fix) -> None:
    api_key, to = os.getenv("RESEND_API_KEY", ""), os.getenv("ALERT_EMAIL", "")
    if not api_key or not to:
        log.info("email_skipped_no_config")
        return
    url = f"{os.getenv('APP_URL', 'http://localhost:3000')}/pipeline/{pipeline_id}"
    when = datetime.now(timezone.utc).strftime("%d %b %Y, %H:%M:%S UTC")
    rows = "".join(
        f'<tr style="border-bottom:1px solid rgba(255,255,255,0.1)"><td style="padding:10px 0;font-size:11px;'
        f'color:rgba(255,255,255,0.4);width:140px">{k}</td><td style="padding:10px 0;font-size:13px;color:{c}">{v}</td></tr>'
        for k, v, c in [("Error Type", error_type, "#FF0055"), ("Description", description, "#f5f5f5"),
                        ("AI Root Cause", root_cause or "n/a", "#f5f5f5"),
                        ("Proposed Fix", proposed_fix, "#00FF66"), ("Detected At", when, "#f5f5f5")])
    html = f"""<div style="font-family:monospace;background:#050505;color:#f5f5f5;padding:32px;max-width:600px">
      <div style="border-left:4px solid #FF0055;padding-left:16px;margin-bottom:24px">
        <div style="font-size:11px;letter-spacing:3px;color:#FF0055">🔴 CRITICAL · HUMAN APPROVAL REQUIRED</div>
        <div style="font-size:22px;font-weight:700;color:#fff">{pipeline_name}</div></div>
      <table style="width:100%;border-collapse:collapse;margin-bottom:24px">{rows}</table>
      <a href="{url}" style="background:#00FF66;color:#000;padding:12px 24px;text-decoration:none;font-size:12px;font-weight:700">→ Review &amp; Approve Fix</a>
      <div style="font-size:11px;color:rgba(255,255,255,0.3);margin-top:24px">Pipeline Autopilot · automated alert</div></div>"""
    try:
        import resend
        resend.api_key = api_key
        resend.Emails.send({"from": "Pipeline Autopilot <onboarding@resend.dev>", "to": to,
                            "subject": f"🔴 Critical pipeline failure — {pipeline_name}", "html": html})
        log.info("alert_email_sent", pipeline=pipeline_id)
    except Exception as e:
        log.error("email_send_failed", error=str(e))
