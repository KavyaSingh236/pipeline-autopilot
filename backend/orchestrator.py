from __future__ import annotations

import asyncio
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

import httpx
import structlog

import agents
import data_source as ds
from db import PIPELINES, get_pool
from error_classifier import CRITICAL_TYPES, classify_error
from mailer import send_alert
from ws_manager import manager

log = structlog.get_logger(__name__)

TICK_SECONDS = int(os.getenv("TICK_SECONDS", "30"))
INITIAL_DELAY_SECONDS = int(os.getenv("INITIAL_DELAY_SECONDS", "5"))
FAILURE_CHANCE = float(os.getenv("FAILURE_CHANCE", "0.6"))

_task: asyncio.Task | None = None
_client: httpx.AsyncClient | None = None

_alerts_enabled = os.getenv("ALERTS_DEFAULT", "false").lower() == "true"

_last_good: list[dict] = []
_baseline = 0
_cache: tuple[list[dict], float] = ([], 0.0)

_held: dict[str, list[dict]] = {}

_rng = random.Random()

_diag = {
    "ticks": 0,
    "last_tick": None,
    "bigquery_ok": None,
    "last_error": None,
    "rows_in_batch": 0,
}


def diagnostics() -> dict:
    return _diag


def get_alerts_enabled() -> bool:
    return _alerts_enabled


def set_alerts_enabled(value: bool) -> None:
    global _alerts_enabled
    _alerts_enabled = value
    log.info("email_alerts_toggled", enabled=value)


async def _failure_number(conn) -> int:
    row = await conn.fetchrow(
        "SELECT failure_count FROM public.orchestrator_state WHERE id=1"
    )

    if row is None:
        await conn.execute(
            "INSERT INTO public.orchestrator_state (id, failure_count) VALUES (1, 0)"
        )
        return 1

    return int(row["failure_count"]) + 1


async def _advance_failure_number(conn) -> None:
    await conn.execute(
        """
        UPDATE public.orchestrator_state
        SET failure_count = CASE
            WHEN failure_count >= 7 THEN 0
            ELSE failure_count + 1
        END
        WHERE id=1
        """
    )


async def _clean_batch() -> list[dict]:
    global _cache

    rows, timestamp = _cache

    if rows and time.time() - timestamp < 3600:
        return rows

    try:
        rows = await asyncio.to_thread(ds.fetch_batch_sync)

        if rows:
            _cache = (rows, time.time())
            _diag.update(
                bigquery_ok=True,
                last_error=None,
                rows_in_batch=len(rows),
            )
            return rows

        _diag.update(
            bigquery_ok=False,
            last_error="BigQuery returned 0 rows",
        )

    except Exception as exc:
        _diag.update(
            bigquery_ok=False,
            last_error=f"{type(exc).__name__}: {str(exc)[:300]}",
        )
        log.error("bigquery_fetch_failed", error=str(exc))

    return _last_good or _cache[0]


async def _record_run(
    conn,
    dag_id: str,
    status: str,
    rows: int,
    quarantined: int,
) -> str:
    run_id = (
        f"scheduled__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
        f"_{random.randint(100, 999)}"
    )

    await conn.execute(
        """
        INSERT INTO public.pipeline_runs
        (dag_id, run_id, status, started_at, finished_at, rows_processed, rows_quarantined)
        VALUES ($1, $2, $3, now() - interval '4 seconds', now(), $4, $5)
        """,
        dag_id,
        run_id,
        status,
        rows,
        quarantined,
    )

    return run_id


async def _set_status(
    conn,
    pipeline_id: str,
    status: str,
    next_run: bool = True,
) -> None:
    query = (
        "UPDATE public.pipelines SET status=$2"
        + (
            ", next_run=now() + interval '1 hour'"
            if next_run
            else ""
        )
        + " WHERE id=$1"
    )

    await conn.execute(query, pipeline_id, status)


async def _has_open_failure(conn, pipeline_id: str) -> bool:
    return bool(
        await conn.fetchval(
            """
            SELECT count(*)
            FROM public.audit_log
            WHERE pipeline_id=$1
              AND status IN ('pending_approval', 'rejected')
            """,
            pipeline_id,
        )
    )


async def _commit_good(conn, rows: list[dict]) -> None:
    global _last_good, _baseline

    await ds.load(conn, rows)

    _last_good = [dict(row) for row in rows]
    _baseline = len(rows)


async def run_once(
    pipeline: dict,
    force: str | None = None,
) -> None:
    global _last_good, _baseline

    pool = await get_pool()

    async with pool.acquire() as conn:
        if await _has_open_failure(conn, pipeline["id"]):
            return

        clean = await _clean_batch()

        if not clean:
            return

        if not _last_good:
            _last_good = [dict(row) for row in clean]
            _baseline = len(clean)

        forced = force in ds.ALLOWED_ACTION

        if not forced and _rng.random() >= FAILURE_CHANCE:
            await _commit_good(conn, clean)

            run_id = await _record_run(
                conn,
                pipeline["dag_id"],
                "success",
                len(clean),
                0,
            )

            await _set_status(conn, pipeline["id"], "healthy")

            await manager.broadcast(
                "pipeline_success",
                {
                    "pipeline_id": pipeline["id"],
                    "status": "healthy",
                    "rows_processed": len(clean),
                    "run_id": run_id,
                },
            )

            return

        failure_number = await _failure_number(conn)

        if forced:
            severity = "critical" if force in CRITICAL_TYPES else "routine"

            failure_plan = {
                "error_type": force,
                "fraction": None,
                "scenario": "Operator-triggered demonstration failure.",
                "model": "demo-trigger",
            }
        else:
            severity = "critical" if failure_number % 8 == 0 else "routine"

            failure_plan = await agents.generate_failure(
                _client,
                pipeline,
                len(clean),
                severity,
                failure_number,
            )

        error_type = failure_plan["error_type"]

        if severity == "critical":
            error_type = "row_count_anomaly"
        elif error_type in CRITICAL_TYPES:
            error_type = agents.ROUTINE_ERROR_TYPES[
                (failure_number - 1) % len(agents.ROUTINE_ERROR_TYPES)
            ]

        await _handle_failure(
            conn,
            pipeline,
            clean,
            error_type,
            failure_plan,
            not forced,
        )


async def _handle_failure(
    conn,
    pipeline: dict,
    clean: list[dict],
    error_type: str,
    failure_plan: dict,
    count_toward_cycle: bool,
) -> None:
    fraction = failure_plan.get("fraction")
    scenario = failure_plan.get("scenario", "")

    bad = ds.inject(
        error_type,
        clean,
        _rng,
        fraction,
    )

    findings = ds.validate(
        bad,
        _baseline or len(clean),
    )

    if not findings:
        await _commit_good(conn, clean)
        return

    actual_type = findings[0]["type"]

    if error_type in CRITICAL_TYPES:
        actual_type = "row_count_anomaly"

    if error_type not in CRITICAL_TYPES and actual_type in CRITICAL_TYPES:
        actual_type = error_type

    info = classify_error(actual_type)
    pipeline_id = pipeline["id"]

    await _set_status(
        conn,
        pipeline_id,
        "warning",
        next_run=False,
    )

    await manager.broadcast(
        "failure_detected",
        {
            "pipeline_id": pipeline_id,
            "status": "warning",
            "error_type": actual_type,
        },
    )

    error_log, error_model = await agents.write_error_log(
        _client,
        pipeline,
        actual_type,
        findings,
        len(bad),
        scenario,
    )

    diagnosis = await agents.remediation_agent(
        _client,
        actual_type,
        error_log,
        findings,
    )

    critical = actual_type in CRITICAL_TYPES

    base = {
        "pipeline_id": pipeline_id,
        "error_type": actual_type,
        "description": info["description"],
        "error_log": error_log,
        "root_cause": diagnosis["root_cause"],
        "action": diagnosis["action"],
        "explanation": diagnosis["explanation"],
        "model": diagnosis["model"],
        "confidence": diagnosis["confidence"],
    }

    if not critical:
        fixed, quarantined = ds.apply_fix(
            diagnosis["action"],
            bad,
            _last_good or clean,
        )

        remaining = ds.validate(
            fixed,
            _baseline or len(clean),
        )

        fallback_used = False

        if remaining:
            fallback_used = True

            fixed, quarantined = ds.apply_fix(
                "reload_last_good_batch",
                bad,
                _last_good or clean,
            )

            remaining = ds.validate(
                fixed,
                _baseline or len(clean),
            )

        if not remaining:
            await _commit_good(conn, fixed)

            outcome = (
                f"Auto-healed · {len(fixed)} rows loaded, "
                f"{quarantined} quarantined"
            )

            if fallback_used:
                outcome = (
                    "Agent 2 remediation failed revalidation; "
                    "safe last-known-good recovery applied · "
                    f"{len(fixed)} rows loaded"
                )

            await conn.execute(
                """
                INSERT INTO public.audit_log
                (
                    pipeline_id,
                    error_type,
                    description,
                    proposed_fix,
                    auto_fixable,
                    status,
                    approved_by,
                    outcome,
                    root_cause,
                    action,
                    explanation,
                    error_log,
                    model,
                    confidence,
                    rows_affected,
                    resolved_at
                )
                VALUES
                (
                    $1,$2,$3,$4,TRUE,'auto_fixed',
                    'agent-2 (auto)',$5,$6,$7,$8,$9,$10,$11,$12,now()
                )
                """,
                pipeline_id,
                actual_type,
                base["description"],
                f"{diagnosis['action']} · {info['proposed_fix']}",
                outcome,
                base["root_cause"],
                base["action"],
                base["explanation"],
                error_log,
                f"{base['model']} / Agent 1: {error_model}",
                base["confidence"],
                quarantined,
            )

            run_id = await _record_run(
                conn,
                pipeline["dag_id"],
                "success",
                len(fixed),
                quarantined,
            )

            await _set_status(
                conn,
                pipeline_id,
                "healthy",
            )

            if count_toward_cycle:
                await _advance_failure_number(conn)

            await manager.broadcast(
                "auto_healed",
                {
                    "pipeline_id": pipeline_id,
                    "status": "healthy",
                    "error_type": actual_type,
                    "rows_processed": len(fixed),
                    "run_id": run_id,
                },
            )

            return

        critical = True

    audit_id = await conn.fetchval(
        """
        INSERT INTO public.audit_log
        (
            pipeline_id,
            error_type,
            description,
            proposed_fix,
            auto_fixable,
            status,
            root_cause,
            action,
            explanation,
            error_log,
            model,
            confidence,
            recommendation,
            downstream_impact,
            risk_if_approved,
            risk_if_rejected,
            alternatives
        )
        VALUES
        (
            $1,$2,$3,$4,FALSE,'pending_approval',
            $5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15
        )
        RETURNING id
        """,
        pipeline_id,
        actual_type,
        base["description"],
        info["proposed_fix"],
        base["root_cause"],
        base["action"],
        base["explanation"],
        error_log,
        f"{base['model']} / Agent 1: {error_model}",
        base["confidence"],
        diagnosis["recommendation"],
        diagnosis["downstream_impact"],
        diagnosis["risk_if_approved"],
        diagnosis["risk_if_rejected"],
        json.dumps(diagnosis["alternatives"]),
    )

    audit_key = str(audit_id)

    _held[audit_key] = bad

    await _record_run(
        conn,
        pipeline["dag_id"],
        "failed",
        len(bad),
        0,
    )

    await _set_status(
        conn,
        pipeline_id,
        "critical",
        next_run=False,
    )

    if count_toward_cycle:
        await _advance_failure_number(conn)

    await manager.broadcast(
        "pipeline_failed",
        {
            "pipeline_id": pipeline_id,
            "status": "critical",
            "audit_id": audit_key,
            "error_type": actual_type,
            "description": info["description"],
        },
    )

    if _alerts_enabled:
        asyncio.create_task(
            asyncio.to_thread(
                send_alert,
                pipeline["name"],
                pipeline_id,
                audit_key,
                actual_type,
                info["description"],
                diagnosis["root_cause"],
                diagnosis["action"],
                diagnosis["recommendation"],
                diagnosis["downstream_impact"],
                diagnosis["risk_if_approved"],
                diagnosis["risk_if_rejected"],
            )
        )


async def approve_fix(
    pipeline_id: str,
    audit_id: str,
    approved_by: str,
) -> dict:
    pool = await get_pool()

    async with pool.acquire() as conn:
        audit = await conn.fetchrow(
            """
            SELECT *
            FROM public.audit_log
            WHERE id=$1
              AND pipeline_id=$2
              AND status='pending_approval'
            """,
            uuid.UUID(audit_id),
            pipeline_id,
        )

        pipe = await conn.fetchrow(
            "SELECT * FROM public.pipelines WHERE id=$1",
            pipeline_id,
        )

        if audit is None or pipe is None:
            raise ValueError("pending approval not found")

        rows = _last_good or await _clean_batch()

        if not rows:
            raise ValueError("no last-known-good batch is available")

        await _commit_good(conn, rows)

        run_id = await _record_run(
            conn,
            pipe["dag_id"],
            "success",
            len(rows),
            0,
        )

        await conn.execute(
            """
            UPDATE public.audit_log
            SET
                status='approved',
                approved_by=$2,
                outcome='Fix approved · reloaded last good batch and reran',
                resolved_at=now()
            WHERE id=$1
            """,
            uuid.UUID(audit_id),
            approved_by,
        )

        await _set_status(
            conn,
            pipeline_id,
            "healthy",
        )

    await manager.broadcast(
        "fix_approved",
        {
            "pipeline_id": pipeline_id,
            "status": "healthy",
            "approved_by": approved_by,
            "rows_processed": len(rows),
            "run_id": run_id,
        },
    )

    return {
        "status": "healthy",
        "run_id": run_id,
        "rows_processed": len(rows),
        "rows_quarantined": 0,
    }


async def reject_fix(
    pipeline_id: str,
    audit_id: str,
    rejected_by: str,
    reason: str = "",
) -> dict:
    pool = await get_pool()

    async with pool.acquire() as conn:
        audit = await conn.fetchrow(
            """
            SELECT id
            FROM public.audit_log
            WHERE id=$1
              AND pipeline_id=$2
              AND status='pending_approval'
            """,
            uuid.UUID(audit_id),
            pipeline_id,
        )

        if audit is None:
            raise ValueError("pending approval not found")

        outcome = "Rejected · awaiting manual fix by engineer"

        if reason.strip():
            outcome += f" · Reason: {reason.strip()[:300]}"

        await conn.execute(
            """
            UPDATE public.audit_log
            SET status='rejected',
                approved_by=$2,
                outcome=$3
            WHERE id=$1
            """,
            uuid.UUID(audit_id),
            rejected_by,
            outcome,
        )

        await _set_status(
            conn,
            pipeline_id,
            "critical",
            next_run=False,
        )

    await manager.broadcast(
        "fix_rejected",
        {
            "pipeline_id": pipeline_id,
            "status": "critical",
            "rejected_by": rejected_by,
        },
    )

    return {
        "status": "critical",
        "awaiting_manual_fix": True,
    }


async def manual_fix(
    pipeline_id: str,
    audit_id: str,
    fixed_by: str,
    action: str | None,
    instruction: str | None,
) -> dict:
    if not action and instruction:
        action = await agents.interpret_instruction(
            _client,
            instruction,
        )

    if action not in ds.MANUAL_ACTIONS:
        raise ValueError(
            "could not map that to a safe action — pick one of the suggested options"
        )

    pool = await get_pool()

    async with pool.acquire() as conn:
        audit = await conn.fetchrow(
            """
            SELECT *
            FROM public.audit_log
            WHERE id=$1
              AND pipeline_id=$2
              AND status='rejected'
            """,
            uuid.UUID(audit_id),
            pipeline_id,
        )

        pipe = await conn.fetchrow(
            "SELECT * FROM public.pipelines WHERE id=$1",
            pipeline_id,
        )

        if audit is None or pipe is None:
            raise ValueError("rejected incident not found")

        bad = _held.pop(audit_id, None)

        if bad is None:
            bad = _last_good or await _clean_batch()

        if not bad:
            raise ValueError("no batch is available for manual remediation")

        rows, quarantined = ds.apply_fix(
            action,
            bad,
            _last_good or bad,
        )

        if action == "reload_last_good_batch":
            await _commit_good(conn, rows)
        elif rows:
            await ds.load(conn, rows)
            global _last_good, _baseline
            _last_good = [dict(row) for row in rows]
            _baseline = len(rows)

        run_id = await _record_run(
            conn,
            pipe["dag_id"],
            "success",
            len(rows),
            quarantined,
        )

        label = ds.MANUAL_ACTIONS[action]

        if instruction:
            label += f' (instruction: "{instruction.strip()[:300]}")'

        await conn.execute(
            """
            UPDATE public.audit_log
            SET
                status='manually_fixed',
                approved_by=$2,
                action=$3,
                outcome=$4,
                resolved_at=now()
            WHERE id=$1
            """,
            uuid.UUID(audit_id),
            fixed_by,
            action,
            f"Manual fix · {label}",
        )

        await _set_status(
            conn,
            pipeline_id,
            "healthy",
        )

    await manager.broadcast(
        "fix_approved",
        {
            "pipeline_id": pipeline_id,
            "status": "healthy",
            "approved_by": fixed_by,
            "rows_processed": len(rows),
            "run_id": run_id,
        },
    )

    return {
        "status": "healthy",
        "action": action,
        "run_id": run_id,
        "rows_processed": len(rows),
    }


async def trigger_demo(
    error_type: str = "row_count_anomaly",
) -> dict:
    if error_type not in ds.ALLOWED_ACTION:
        raise ValueError("unsupported demo error type")

    pool = await get_pool()

    selected = None

    async with pool.acquire() as conn:
        for pipeline in PIPELINES:
            if not await _has_open_failure(conn, pipeline["id"]):
                selected = pipeline
                break

    if selected is None:
        return {
            "triggered": None,
            "reason": "all pipelines already have an open critical failure",
        }

    await run_once(
        selected,
        force=error_type,
    )

    return {
        "triggered": error_type,
        "pipeline": selected["id"],
    }


async def _loop() -> None:
    index = 0

    await asyncio.sleep(INITIAL_DELAY_SECONDS)

    while True:
        try:
            _diag["ticks"] += 1
            _diag["last_tick"] = datetime.now(timezone.utc).isoformat()

            pipeline = PIPELINES[index % len(PIPELINES)]

            await run_once(pipeline)

            index += 1

            await asyncio.sleep(TICK_SECONDS)

        except asyncio.CancelledError:
            break

        except Exception as exc:
            _diag["last_error"] = (
                f"tick: {type(exc).__name__}: {str(exc)[:300]}"
            )
            log.error(
                "orchestrator_tick_error",
                error=str(exc),
            )
            await asyncio.sleep(5)


def start() -> None:
    global _task, _client

    if _task is None:
        _client = httpx.AsyncClient(
            follow_redirects=True,
            timeout=30,
        )

        _task = asyncio.create_task(_loop())

        log.info("orchestrator_started")


async def stop() -> None:
    global _task, _client

    if _task is not None:
        _task.cancel()
        try:
            await _task
        except asyncio.CancelledError:
            pass
        _task = None

    if _client is not None:
        await _client.aclose()
        _client = None
