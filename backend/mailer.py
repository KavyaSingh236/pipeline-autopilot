from __future__ import annotations

import os
from datetime import datetime, timezone

import structlog

log = structlog.get_logger(__name__)


def send_alert(
    pipeline_name,
    pipeline_id,
    audit_id,
    error_type,
    description,
    root_cause,
    proposed_fix,
    recommendation,
    downstream_impact,
    risk_if_approved,
    risk_if_rejected,
) -> None:
    api_key = os.getenv("RESEND_API_KEY", "")
    recipient = os.getenv("ALERT_EMAIL", "")

    if not api_key or not recipient:
        log.info("email_skipped_no_config")
        return

    app_url = os.getenv(
        "APP_URL",
        "http://localhost:3000",
    ).rstrip("/")

    review_url = (
        f"{app_url}/pipeline/{pipeline_id}"
    )

    detected_at = datetime.now(
        timezone.utc
    ).strftime(
        "%d %b %Y, %H:%M:%S UTC"
    )

    rows = "".join(
        (
            '<tr style="border-bottom:1px solid rgba(255,255,255,0.1)">'
            '<td style="padding:10px 0;font-size:11px;'
            'color:rgba(255,255,255,0.4);width:160px">'
            f"{label}"
            "</td>"
            '<td style="padding:10px 0;font-size:13px;color:#f5f5f5">'
            f"{value or 'n/a'}"
            "</td>"
            "</tr>"
        )
        for label, value in [
            ("Error Type", error_type),
            ("Description", description),
            ("AI Root Cause", root_cause),
            ("AI Recommendation", recommendation),
            ("Proposed Fix", proposed_fix),
            ("Downstream Impact", downstream_impact),
            ("Risk if Approved", risk_if_approved),
            ("Risk if Rejected", risk_if_rejected),
            ("Detected At", detected_at),
        ]
    )

    html = f"""
    <div style="font-family:monospace;background:#050505;color:#f5f5f5;padding:32px;max-width:700px">
      <div style="border-left:4px solid #FF0055;padding-left:16px;margin-bottom:24px">
        <div style="font-size:11px;letter-spacing:3px;color:#FF0055">
          CRITICAL · HUMAN APPROVAL REQUIRED
        </div>
        <div style="font-size:22px;font-weight:700;color:#fff;margin-top:8px">
          {pipeline_name}
        </div>
      </div>

      <table style="width:100%;border-collapse:collapse;margin-bottom:28px">
        {rows}
      </table>

      <a
        href="{review_url}"
        style="display:inline-block;background:#00FF66;color:#000;padding:13px 24px;text-decoration:none;font-size:12px;font-weight:700"
      >
        REVIEW INCIDENT
      </a>

      <div style="font-size:10px;color:rgba(255,255,255,0.3);margin-top:24px">
        Pipeline Autopilot · Audit ID {audit_id}
      </div>
    </div>
    """

    try:
        import resend

        resend.api_key = api_key

        resend.Emails.send(
            {
                "from": "Pipeline Autopilot <onboarding@resend.dev>",
                "to": recipient,
                "subject": (
                    f"Critical pipeline failure — {pipeline_name}"
                ),
                "html": html,
            }
        )

        log.info(
            "alert_email_sent",
            pipeline=pipeline_id,
        )

    except Exception as exc:
        log.error(
            "email_send_failed",
            error=str(exc),
        )
