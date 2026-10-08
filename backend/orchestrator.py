"""Orchestrator.
Real Google Trends batch (BigQuery) -> Agent 1 injects + logs an ingestion error -> Agent 2 diagnoses and heals it.
Errors come from a shuffled deck of 8: 7 routine (auto-healed, audit-log only) + 1 critical
(human approval + email). So exactly 1 in 8 failures raises an alert.
"""
from __future__ import annotations
import asyncio, json, os, random, time, uuid
from datetime import datetime, timezone

import httpx
import structlog

import agents
import data_source as ds
from db import get_pool, PIPELINES
from error_classifier import classify_error, CRITICAL_TYPES
from mailer import send_alert
from ws_manager import manager

log = structlog.get_logger(__name__)

TICK_SECONDS = int(os.getenv("TICK_SECONDS", "30"))
FAILURE_CHANCE = float(os.getenv("FAILURE_CHANCE", "0.6"))
ROUTINE_DECK = ["schema_mismatch", "null_threshold_exceeded", "data_type_mismatch", "duplicate_records",
                "invalid_range", "schema_mismatch", "null_threshold_exceeded"]

_task: asyncio.Task | None = None
_client: httpx.AsyncClient | None = None
_alerts_enabled = os.getenv("ALERTS_DEFAULT", "false").lower() == "true"
_deck: list[str] = []
_last_good: list[dict] = []
_baseline = 0
_cache: tuple[list[dict], float] = ([], 0.0)
_held: dict[str, list[dict]] = {}   # corrupted batches awaiting a human decision
_rng = random.Random()
_diag = {"ticks": 0, "last_tick": None, "bigquery_ok": None, "last_error": None, "rows_in_batch": 0}


def diagnostics() -> dict:
    return _diag


def get_alerts_enabled() -> bool:
    return _alerts_enabled


def set_alerts_enabled(value: bool) -> None:
    global _alerts_enabled
    _alerts_enabled = value
    log.info("email_alerts_toggled", enabled=value)


def _next_error() -> str:
    global _deck
    if not _deck:
        _deck = ROUTINE_DECK + ["row_count_anomaly"]
        _rng.shuffle(_deck)
    return _deck.pop()


async def _clean_batch() -> list[dict]:
    global _cache
    rows, ts = _cache
    if rows and time.time() - ts < 3600:
        return rows
    try:
        rows = await asyncio.to_thread(ds.fetch_batch_sync)
        if rows:
            _cache = (rows, time.time())
            _diag.update(bigquery_ok=True, last_error=None, rows_in_batch=len(rows))
            return rows
        _diag.update(bigquery_ok=False, last_error="BigQuery returned 0 rows")
    except Exception as e:
        _diag.update(bigquery_ok=False, last_error=f"{type(e).__name__}: {str(e)[:300]}")
        log.error("bigquery_fetch_failed", error=str(e))
    return _last_good or _cache[0]


async def _record_run(conn, dag_id: str, status: str, rows: int, quarantined: int) -> str:
    run_id = f"scheduled__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{random.randint(100, 999)}"
    await conn.execute(
        """INSERT INTO public.pipeline_runs (dag_id, run_id, status, started_at, finished_at, rows_processed, rows_quarantined)
           VALUES ($1,$2,$3, now() - interval '4 seconds', now(), $4, $5)""",
        dag_id, run_id, status, rows, quarantined)
    return run_id


async def _set_status(conn, pipeline_id: str, status: str, next_run: bool = True) -> None:
    q = "UPDATE public.pipelines SET status=$2" + (", next_run=now() + interval '1 hour'" if next_run else "") + " WHERE id=$1"
    await conn.execute(q, pipeline_id, status)


async def _has_open_failure(conn, pipeline_id: str) -> bool:
    return bool(await conn.fetchval(
        "SELECT count(*) FROM public.audit_log WHERE pipeline_id=$1 AND status IN ('pending_approval','rejected')", pipeline_id))


async def _commit_good(conn, rows: list[dict]) -> None:
    global _last_good, _baseline
    await ds.load(conn, rows)
    _last_good, _baseline = [dict(r) for r in rows], len(rows)


async def run_once(pipeline: dict, force: str | None = None) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        if await _has_open_failure(conn, pipeline["id"]):
            return
        clean = await _clean_batch()
        if not clean:
            return
        error_type = force if force in ds.ALLOWED_ACTION else (_next_error() if _rng.random() < FAILURE_CHANCE else None)
        if error_type is None:
            await _commit_good(conn, clean)
            run_id = await _record_run(conn, pipeline["dag_id"], "success", len(clean), 0)
            await _set_status(conn, pipeline["id"], "healthy")
            await manager.broadcast("pipeline_success", {"pipeline_id": pipeline["id"], "status": "healthy",
                                                         "rows_processed": len(clean), "run_id": run_id})
            return
        await _handle_failure(conn, pipeline, clean, error_type)


async def _handle_failure(conn, pipeline: dict, clean: list[dict], injected: str) -> None:
    fraction, scenario = await agents.plan_injection(_client, pipeline, injected, len(clean))   # Agent 1 decides
    bad = ds.inject(injected, clean, _rng, fraction)
    findings = ds.validate(bad, _baseline or len(clean))
    if not findings:  # injection didn't trip a check; treat as healthy run
        await _commit_good(conn, clean)
        return
    etype = findings[0]["type"]
    info = classify_error(etype)
    pid = pipeline["id"]

    await _set_status(conn, pid, "warning", next_run=False)
    await manager.broadcast("failure_detected", {"pipeline_id": pid, "status": "warning", "error_type": etype})

    error_log, _ = await agents.write_error_log(_client, pipeline, etype, findings, len(bad), scenario)       # Agent 1
    diag = await agents.remediation_agent(_client, etype, error_log, findings)                 # Agent 2
    base = dict(pipeline_id=pid, error_type=etype, description=info["description"], error_log=error_log,
                root_cause=diag["root_cause"], action=diag["action"], explanation=diag["explanation"],
                model=diag["model"], confidence=diag["confidence"])

    critical = etype in CRITICAL_TYPES
    reason = ""
    if not critical:
        fixed, quarantined = ds.apply_fix(diag["action"], bad, _last_good)
        if ds.validate(fixed, _baseline or len(clean)):   # fix must pass revalidation
            critical, reason = True, " Auto-fix failed revalidation — escalated."
        else:
            await _commit_good(conn, fixed)
            await conn.execute(
                """INSERT INTO public.audit_log (pipeline_id,error_type,description,proposed_fix,auto_fixable,status,
                   approved_by,outcome,root_cause,action,explanation,error_log,model,confidence,rows_affected,resolved_at)
                   VALUES ($1,$2,$3,$4,TRUE,'auto_fixed','agent-2 (auto)',$5,$6,$7,$8,$9,$10,$11,$12,now())""",
                pid, etype, base["description"], f"{diag['action']} · {info['proposed_fix']}",
                f"Auto-healed · {len(fixed)} rows loaded, {quarantined} quarantined",
                base["root_cause"], base["action"], base["explanation"], error_log, base["model"],
                base["confidence"], quarantined)
            run_id = await _record_run(conn, pipeline["dag_id"], "success", len(fixed), quarantined)
            await _set_status(conn, pid, "healthy")
            log.info("auto_healed", pipeline=pid, error=etype, action=diag["action"])
            await manager.broadcast("auto_healed", {"pipeline_id": pid, "status": "healthy", "error_type": etype,
                                                    "rows_processed": len(fixed), "run_id": run_id})
            return

    # critical path: human approval + email
    audit_id = await conn.fetchval(
        """INSERT INTO public.audit_log (pipeline_id,error_type,description,proposed_fix,auto_fixable,status,
           root_cause,action,explanation,error_log,model,confidence,recommendation,downstream_impact,
           risk_if_approved,risk_if_rejected,alternatives)
           VALUES ($1,$2,$3,$4,FALSE,'pending_approval',$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) RETURNING id""",
        pid, etype, base["description"], info["proposed_fix"], base["root_cause"], base["action"],
        base["explanation"] + reason, error_log, base["model"], base["confidence"], diag["recommendation"],
        diag["downstream_impact"], diag["risk_if_approved"], diag["risk_if_rejected"], json.dumps(diag["alternatives"]))
    _held[str(audit_id)] = bad
    await _record_run(conn, pipeline["dag_id"], "failed", len(bad), 0)
    await _set_status(conn, pid, "critical", next_run=False)
    log.warning("critical_failure", pipeline=pid, error=etype)
    await manager.broadcast("pipeline_failed", {"pipeline_id": pid, "status": "critical", "audit_id": str(audit_id),
                                                "error_type": etype, "description": info["description"]})
    if _alerts_enabled:
        asyncio.get_running_loop().run_in_executor(
            None, send_alert, pipeline["name"], pid, etype, info["description"], diag["root_cause"], info["proposed_fix"])


async def approve_fix(pipeline_id: str, audit_id: str, approved_by: str) -> dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        audit = await conn.fetchrow("SELECT * FROM public.audit_log WHERE id=$1", uuid.UUID(audit_id))
        pipe = await conn.fetchrow("SELECT * FROM public.pipelines WHERE id=$1", pipeline_id)
        if audit is None or pipe is None:
            raise ValueError("audit entry not found")
        rows = _last_good or await _clean_batch()
        await _commit_good(conn, rows)
        run_id = await _record_run(conn, pipe["dag_id"], "success", len(rows), 0)
        await conn.execute(
            """UPDATE public.audit_log SET status='approved', approved_by=$2,
               outcome='Fix approved · reloaded last good batch and reran', resolved_at=now() WHERE id=$1""",
            uuid.UUID(audit_id), approved_by)
        await _set_status(conn, pipeline_id, "healthy")
    await manager.broadcast("fix_approved", {"pipeline_id": pipeline_id, "status": "healthy",
                                             "approved_by": approved_by, "rows_processed": len(rows), "run_id": run_id})
    return {"status": "healthy", "run_id": run_id, "rows_processed": len(rows), "rows_quarantined": 0}


async def reject_fix(pipeline_id: str, audit_id: str, rejected_by: str, reason: str = "") -> dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        res = await conn.execute(
            """UPDATE public.audit_log SET status='rejected', approved_by=$2, outcome=$3 WHERE id=$1""",
            uuid.UUID(audit_id), rejected_by, "Rejected · awaiting manual fix by engineer")
        if res.endswith("0"):
            raise ValueError("audit entry not found")
        await _set_status(conn, pipeline_id, "critical", next_run=False)
    await manager.broadcast("fix_rejected", {"pipeline_id": pipeline_id, "status": "critical", "rejected_by": rejected_by})
    return {"status": "critical", "awaiting_manual_fix": True}


async def manual_fix(pipeline_id: str, audit_id: str, fixed_by: str, action: str | None, instruction: str | None) -> dict:
    """After rejecting the AI's fix, an engineer picks an alternative or types an instruction."""
    if not action and instruction:
        action = await agents.interpret_instruction(_client, instruction)
    if action not in ds.MANUAL_ACTIONS:
        raise ValueError("could not map that to a safe action — pick one of the suggested options")
    pool = await get_pool()
    async with pool.acquire() as conn:
        audit = await conn.fetchrow("SELECT * FROM public.audit_log WHERE id=$1", uuid.UUID(audit_id))
        pipe = await conn.fetchrow("SELECT * FROM public.pipelines WHERE id=$1", pipeline_id)
        if audit is None or pipe is None:
            raise ValueError("audit entry not found")
        bad = _held.pop(audit_id, None) or _last_good
        rows, quarantined = ds.apply_fix(action, bad, _last_good or bad)
        if action == "reload_last_good_batch":
            await _commit_good(conn, rows)
        elif rows:
            await ds.load(conn, rows)
        run_id = await _record_run(conn, pipe["dag_id"], "success", len(rows), quarantined)
        label = ds.MANUAL_ACTIONS[action] + (f' (instruction: "{instruction}")' if instruction else "")
        await conn.execute(
            """UPDATE public.audit_log SET status='manually_fixed', approved_by=$2, action=$3,
               outcome=$4, resolved_at=now() WHERE id=$1""",
            uuid.UUID(audit_id), fixed_by, action, f"Manual fix · {label}")
        await _set_status(conn, pipeline_id, "healthy")
    await manager.broadcast("fix_approved", {"pipeline_id": pipeline_id, "status": "healthy", "approved_by": fixed_by,
                                             "rows_processed": len(rows), "run_id": run_id})
    return {"status": "healthy", "action": action, "run_id": run_id, "rows_processed": len(rows)}


async def trigger_demo(error_type: str = "row_count_anomaly") -> dict:
    """Force an error right now (for live demos)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        for p in PIPELINES:
            if not await _has_open_failure(conn, p["id"]):
                await run_once(p, force=error_type)
                return {"triggered": error_type, "pipeline": p["id"]}
    return {"triggered": None, "reason": "all pipelines already have an open critical failure"}


async def _loop() -> None:
    idx = 0
    while True:
        try:
            await asyncio.sleep(TICK_SECONDS)
            _diag["ticks"] += 1
            _diag["last_tick"] = datetime.now(timezone.utc).isoformat()
            await run_once(PIPELINES[idx % len(PIPELINES)])
            idx += 1
        except asyncio.CancelledError:
            break
        except Exception as exc:
            _diag["last_error"] = f"tick: {type(exc).__name__}: {str(exc)[:300]}"
            log.error("orchestrator_tick_error", error=str(exc))


def start() -> None:
    global _task, _client
    if _task is None:
        _client = httpx.AsyncClient(follow_redirects=True)
        _task = asyncio.create_task(_loop())
        log.info("orchestrator_started")


async def stop() -> None:
    global _task
    if _task is not None:
        _task.cancel()
        _task = None
    if _client:
        await _client.aclose()
