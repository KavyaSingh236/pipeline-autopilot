"""Pipeline orchestration simulator."""
from __future__ import annotations
import asyncio
import random
import uuid
import smtplib
import os
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import structlog

from db import get_pool, PIPELINES
from error_classifier import classify_error, ERROR_PLAYBOOK
from ws_manager import manager

log = structlog.get_logger(__name__)

GMAIL_USER = os.getenv("GMAIL_USER", "")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")
ALERT_EMAIL = os.getenv("ALERT_EMAIL", "")
APP_URL = os.getenv("APP_URL", "http://localhost:3000")

_TICK_SECONDS = 12
_FAILURE_CHANCE = 0.30
_task: asyncio.Task | None = None
_email_alerts_enabled: bool = False


def get_alerts_enabled() -> bool:
    return _email_alerts_enabled


def set_alerts_enabled(value: bool) -> None:
    global _email_alerts_enabled
    _email_alerts_enabled = value
    log.info("email_alerts_toggled", enabled=value)


def _send_email(pipeline_name, pipeline_id, error_type, description, proposed_fix, severity):
    if not GMAIL_USER or not GMAIL_APP_PASSWORD or not ALERT_EMAIL:
        return
    severity_emoji = "🔴" if severity == "critical" else "⚠️"
    pipeline_url = f"{APP_URL}/pipeline/{pipeline_id}"
    detected_at = datetime.now(timezone.utc).strftime("%d %b %Y, %H:%M:%S UTC")
    subject = f"{severity_emoji} Pipeline Alert — {pipeline_name} Failed"
    html = f"""
    <div style="font-family:monospace;background:#050505;color:#f5f5f5;padding:32px;max-width:600px;">
      <div style="border-left:4px solid {'#FF0055' if severity == 'critical' else '#FFCC00'};padding-left:16px;margin-bottom:24px;">
        <div style="font-size:11px;letter-spacing:3px;text-transform:uppercase;color:{'#FF0055' if severity == 'critical' else '#FFCC00'};margin-bottom:8px;">
          {severity_emoji} Pipeline Failure · {severity.upper()}
        </div>
        <div style="font-size:22px;font-weight:700;color:#ffffff;">{pipeline_name}</div>
      </div>
      <table style="width:100%;border-collapse:collapse;margin-bottom:24px;">
        <tr style="border-bottom:1px solid rgba(255,255,255,0.1);">
          <td style="padding:10px 0;font-size:11px;color:rgba(255,255,255,0.4);width:140px;">Error Type</td>
          <td style="padding:10px 0;font-size:13px;color:#FF0055;">{error_type}</td>
        </tr>
        <tr style="border-bottom:1px solid rgba(255,255,255,0.1);">
          <td style="padding:10px 0;font-size:11px;color:rgba(255,255,255,0.4);">Description</td>
          <td style="padding:10px 0;font-size:13px;color:#f5f5f5;">{description}</td>
        </tr>
        <tr style="border-bottom:1px solid rgba(255,255,255,0.1);">
          <td style="padding:10px 0;font-size:11px;color:rgba(255,255,255,0.4);">Proposed Fix</td>
          <td style="padding:10px 0;font-size:13px;color:#00FF66;">{proposed_fix}</td>
        </tr>
        <tr>
          <td style="padding:10px 0;font-size:11px;color:rgba(255,255,255,0.4);">Detected At</td>
          <td style="padding:10px 0;font-size:13px;color:#f5f5f5;">{detected_at}</td>
        </tr>
      </table>
      <a href="{pipeline_url}" style="display:inline-block;background:#00FF66;color:#000;padding:12px 24px;text-decoration:none;font-size:12px;font-weight:700;margin-bottom:24px;">
        → Review &amp; Approve Fix
      </a>
      <div style="font-size:11px;color:rgba(255,255,255,0.3);border-top:1px solid rgba(255,255,255,0.1);padding-top:16px;">
        Pipeline Autopilot · This is an automated alert. Do not reply.
      </div>
    </div>
    """
    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = GMAIL_USER
        msg["To"] = ALERT_EMAIL
        msg.attach(MIMEText(html, "html"))
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_USER, ALERT_EMAIL, msg.as_string())
        log.info("alert_email_sent", pipeline=pipeline_id)
    except Exception as e:
        log.error("email_send_failed", error=str(e))


async def _has_open_failure(conn, pipeline_id: str) -> bool:
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT count(*) FROM public.audit_log WHERE pipeline_id=%s AND status='pending_approval'",
            (pipeline_id,)
        )
        row = await cur.fetchone()
        return bool(row and row[0])


async def _record_run(conn, dag_id: str, status: str, rows: int, quarantined: int) -> str:
    run_id = f"manual__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{random.randint(100,999)}"
    async with conn.cursor() as cur:
        await cur.execute("""
            INSERT INTO public.pipeline_runs (dag_id, run_id, status, started_at, finished_at, rows_processed, rows_quarantined)
            VALUES (%s,%s,%s, now() - interval '4 seconds', now(), %s, %s)
        """, (dag_id, run_id, status, rows, quarantined))
    return run_id


async def _open_failure(conn, pipeline_id: str, error_type: str) -> dict:
    info = classify_error(error_type)
    async with conn.cursor() as cur:
        await cur.execute("""
            INSERT INTO public.audit_log (pipeline_id, error_type, description, proposed_fix, auto_fixable, status)
            VALUES (%s,%s,%s,%s,%s,'pending_approval') RETURNING id
        """, (pipeline_id, error_type, info["description"], info["proposed_fix"], info["auto_fixable"]))
        row = await cur.fetchone()
        audit_id = str(row[0])
    return {"audit_id": audit_id, **info}


async def _set_status(conn, pipeline_id: str, status: str, next_run: bool = True) -> None:
    async with conn.cursor() as cur:
        if next_run:
            await cur.execute(
                "UPDATE public.pipelines SET status=%s, next_run=now() + interval '1 hour' WHERE id=%s",
                (status, pipeline_id))
        else:
            await cur.execute("UPDATE public.pipelines SET status=%s WHERE id=%s", (status, pipeline_id))


async def run_once(pipeline: dict) -> None:
    pool = await get_pool()
    async with pool.connection() as conn:
        if await _has_open_failure(conn, pipeline["id"]):
            return

        inject = random.random() < _FAILURE_CHANCE
        if inject:
            error_type = random.choice(list(ERROR_PLAYBOOK.keys()))
            quarantined = random.randint(20, 150) if error_type == "null_threshold_exceeded" else 0
            rows = random.randint(200, 900)
            await _record_run(conn, pipeline["dag_id"], "failed", rows, quarantined)
            failure = await _open_failure(conn, pipeline["id"], error_type)
            new_status = "critical" if not failure["auto_fixable"] else "warning"
            await _set_status(conn, pipeline["id"], new_status, next_run=False)
            await conn.commit()
            log.warning("pipeline_failed", pipeline=pipeline["id"], error=error_type)
            await manager.broadcast("pipeline_failed", {
                "pipeline_id": pipeline["id"], "status": new_status, **failure,
            })
            if _email_alerts_enabled:
                asyncio.get_event_loop().run_in_executor(
                    None, _send_email,
                    pipeline["name"], pipeline["id"],
                    error_type, failure["description"],
                    failure["proposed_fix"], new_status
                )
        else:
            rows = random.randint(800, 1200)
            run_id = await _record_run(conn, pipeline["dag_id"], "success", rows, 0)
            await _set_status(conn, pipeline["id"], "healthy")
            await conn.commit()
            await manager.broadcast("pipeline_success", {
                "pipeline_id": pipeline["id"], "status": "healthy",
                "rows_processed": rows, "run_id": run_id,
            })


async def approve_fix(pipeline_id: str, audit_id: str, approved_by: str) -> dict:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT * FROM public.audit_log WHERE id=%s", (uuid.UUID(audit_id),))
            audit = await cur.fetchone()
            if audit is None:
                raise ValueError("audit entry not found")
            await cur.execute("SELECT * FROM public.pipelines WHERE id=%s", (pipeline_id,))
            pipe = await cur.fetchone()
            quarantined = random.randint(20, 150) if audit[2] == "null_threshold_exceeded" else 0
            rows = random.randint(850, 1200)
            run_id = await _record_run(conn, pipe[1], "success", rows, quarantined)
            await cur.execute("""
                UPDATE public.audit_log SET status='approved', approved_by=%s,
                outcome='Fix applied · pipeline healed and rerun', resolved_at=now() WHERE id=%s
            """, (approved_by, uuid.UUID(audit_id)))
            await _set_status(conn, pipeline_id, "healthy")
        await conn.commit()
    await manager.broadcast("fix_approved", {
        "pipeline_id": pipeline_id, "status": "healthy", "approved_by": approved_by,
        "rows_processed": rows, "rows_quarantined": quarantined, "run_id": run_id,
    })
    log.info("fix_approved", pipeline=pipeline_id, by=approved_by)
    return {"status": "healthy", "run_id": run_id, "rows_processed": rows, "rows_quarantined": quarantined}


async def reject_fix(pipeline_id: str, audit_id: str, rejected_by: str, reason: str = "") -> dict:
    pool = await get_pool()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            outcome = f"Rejected · escalated to on-call. {reason}".strip()
            await cur.execute("""
                UPDATE public.audit_log SET status='rejected', approved_by=%s,
                outcome=%s, resolved_at=now() WHERE id=%s
            """, (rejected_by, outcome, uuid.UUID(audit_id)))
            await _set_status(conn, pipeline_id, "critical", next_run=False)
        await conn.commit()
    await manager.broadcast("fix_rejected", {
        "pipeline_id": pipeline_id, "status": "critical", "rejected_by": rejected_by,
    })
    log.info("fix_rejected", pipeline=pipeline_id, by=rejected_by)
    return {"status": "critical", "escalated": True}


async def _loop() -> None:
    idx = 0
    while True:
        try:
            await asyncio.sleep(_TICK_SECONDS)
            pipeline = PIPELINES[idx % len(PIPELINES)]
            idx += 1
            await run_once(pipeline)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            log.error("orchestrator_tick_error", error=str(exc))


def start() -> None:
    global _task
    if _task is None:
        _task = asyncio.create_task(_loop())
        log.info("orchestrator_started")


async def stop() -> None:
    global _task
    if _task is not None:
        _task.cancel()
        _task = None
