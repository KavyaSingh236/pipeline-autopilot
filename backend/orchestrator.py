import asyncio
import os
from datetime import datetime, timezone
from typing import Any

import httpx
import structlog

import agents
import data_source as ds
from db import PIPELINES, get_pool
from error_classifier import CRITICAL_TYPES
from ws_manager import manager

try:
    import mailer
except Exception:
    mailer = None

log = structlog.get_logger()

TICK_SECONDS = int(os.getenv("TICK_SECONDS", "30"))
INITIAL_DELAY_SECONDS = int(os.getenv("INITIAL_DELAY_SECONDS", "5"))
FAILURE_CHANCE = float(os.getenv("FAILURE_CHANCE", "0.6"))

_client: httpx.AsyncClient | None = None
_scheduler_task: asyncio.Task | None = None

_last_good: list[dict[str, Any]] = []
_baseline = 0

_held: dict[str, list[dict[str, Any]]] = {}
_last_error: str | None = None
_ticks = 0
_last_tick: str | None = None
_rows_in_batch = 0
_bigquery_ok: bool | None = None

DEMO_PIPELINE = "trends-monitor"

ROUTINE_TYPES = [
    "schema_mismatch",
    "null_threshold_exceeded",
    "data_type_mismatch",
    "duplicate_records",
    "invalid_range",
]

FAILURE_CYCLE = [
    "routine",
    "routine",
    "routine",
    "routine",
    "routine",
    "routine",
    "routine",
    "critical",
]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _row_count(rows: Any) -> int:
    try:
        return len(rows)
    except Exception:
        return 0


def _pipeline_name(pipeline_id: str) -> str:
    for p in PIPELINES:
        if p.get("id") == pipeline_id:
            return p.get("name", pipeline_id)
    return pipeline_id


def _pipeline_config(pipeline_id: str) -> dict[str, Any]:
    for p in PIPELINES:
        if p.get("id") == pipeline_id:
            return p
    return {"id": pipeline_id, "name": pipeline_id}


def _failure_class(number: int) -> str:
    return "critical" if number == 8 else "routine"


def _allowed_types() -> list[str]:
    return list(ROUTINE_TYPES) + ["row_count_anomaly"]


async def _get_failure_number(conn) -> int:
    row = await conn.fetchrow(
        """
        SELECT failure_count
        FROM public.orchestrator_state
        WHERE id = 1
        """
    )

    if not row:
        await conn.execute(
            """
            INSERT INTO public.orchestrator_state(id, failure_count)
            VALUES (1, 0)
            ON CONFLICT (id) DO NOTHING
            """
        )
        return 1

    current = int(row["failure_count"])
    return current + 1 if current < 8 else 1


async def _advance_failure_number(conn) -> None:
    await conn.execute(
        """
        UPDATE public.orchestrator_state
        SET failure_count = CASE
            WHEN failure_count >= 8 THEN 0
            ELSE failure_count + 1
        END
        WHERE id = 1
        """
    )


async def _ensure_state(conn) -> None:
    await conn.execute(
        """
        INSERT INTO public.orchestrator_state(id, failure_count)
        VALUES (1, 0)
        ON CONFLICT (id) DO NOTHING
        """
    )


async def _get_latest_good(conn, pipeline_id: str) -> list[dict[str, Any]]:
    try:
        rows = await conn.fetch(
            """
            SELECT row_data
            FROM public.pipeline_good_batches
            WHERE pipeline_id = $1
            ORDER BY created_at DESC
            LIMIT 1
            """,
            pipeline_id,
        )
        if rows:
            data = rows[0]["row_data"]
            if isinstance(data, list):
                return data
    except Exception:
        pass

    return []


async def _commit_good(
    conn,
    rows: list[dict[str, Any]],
    pipeline_id: str = DEMO_PIPELINE,
) -> None:
    global _last_good, _baseline

    clean_rows = [_jsonable(dict(row)) for row in rows]

    try:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS public.pipeline_good_batches (
                pipeline_id TEXT PRIMARY KEY,
                row_data JSONB NOT NULL,
                row_count INTEGER NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )

        await conn.execute(
            """
            INSERT INTO public.pipeline_good_batches(
                pipeline_id,
                row_data,
                row_count,
                created_at
            )
            VALUES ($1, $2::jsonb, $3, NOW())
            ON CONFLICT (pipeline_id)
            DO UPDATE SET
                row_data = EXCLUDED.row_data,
                row_count = EXCLUDED.row_count,
                created_at = NOW()
            """,
            pipeline_id,
            __import__("json").dumps(clean_rows),
            len(clean_rows),
        )
    except Exception as exc:
        log.warning("good_batch_persist_failed", error=str(exc))

    _last_good = clean_rows
    _baseline = len(clean_rows)


async def _load_last_good(
    conn,
    pipeline_id: str,
) -> list[dict[str, Any]]:
    rows = await _get_latest_good(conn, pipeline_id)

    if rows:
        return rows

    if pipeline_id == DEMO_PIPELINE and _last_good:
        return [dict(row) for row in _last_good]

    return []


async def _has_open_failure(conn, pipeline_id: str) -> bool:
    try:
        row = await conn.fetchrow(
            """
            SELECT 1
            FROM public.audit_log
            WHERE pipeline_id = $1
              AND status IN ('pending_approval', 'open', 'detected')
            ORDER BY created_at DESC
            LIMIT 1
            """,
            pipeline_id,
        )
        return bool(row)
    except Exception:
        return False


async def _create_audit(
    conn,
    *,
    pipeline_id: str,
    status: str,
    error_type: str,
    root_cause: str | None = None,
    action: str | None = None,
    explanation: str | None = None,
    error_log: str | None = None,
    model: str | None = None,
    confidence: float | None = None,
    rows_affected: int | None = None,
    recommendation: str | None = None,
    downstream_impact: str | None = None,
    risk_if_approved: str | None = None,
    risk_if_rejected: str | None = None,
    alternatives: Any = None,
) -> str:
    alternatives_value = alternatives

    if alternatives_value is not None and not isinstance(
        alternatives_value,
        str,
    ):
        import json

        alternatives_value = json.dumps(
            _jsonable(alternatives_value)
        )

    row = await conn.fetchrow(
        """
        INSERT INTO public.audit_log(
            pipeline_id,
            status,
            error_type,
            root_cause,
            action,
            explanation,
            error_log,
            model,
            confidence,
            rows_affected,
            recommendation,
            downstream_impact,
            risk_if_approved,
            risk_if_rejected,
            alternatives,
            created_at
        )
        VALUES(
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,NOW()
        )
        RETURNING id
        """,
        pipeline_id,
        status,
        error_type,
        root_cause,
        action,
        explanation,
        error_log,
        model,
        confidence,
        rows_affected,
        recommendation,
        downstream_impact,
        risk_if_approved,
        risk_if_rejected,
        alternatives_value,
    )

    return str(row["id"])


async def _update_audit(
    conn,
    audit_id: str,
    *,
    status: str | None = None,
    action: str | None = None,
    explanation: str | None = None,
) -> None:
    sets = []
    values: list[Any] = []
    index = 1

    if status is not None:
        sets.append(f"status = ${index}")
        values.append(status)
        index += 1

    if action is not None:
        sets.append(f"action = ${index}")
        values.append(action)
        index += 1

    if explanation is not None:
        sets.append(f"explanation = ${index}")
        values.append(explanation)
        index += 1

    if not sets:
        return

    values.append(audit_id)

    await conn.execute(
        f"""
        UPDATE public.audit_log
        SET {", ".join(sets)}
        WHERE id = ${index}
        """,
        *values,
    )


async def _broadcast(event: dict[str, Any]) -> None:
    try:
        await manager.broadcast(_jsonable(event))
    except Exception as exc:
        log.warning("websocket_broadcast_failed", error=str(exc))


async def _send_alert(
    pipeline_id: str,
    audit_id: str,
    error_type: str,
) -> None:
    if mailer is None:
        return

    try:
        await mailer.send_critical_alert(
            pipeline_id=pipeline_id,
            audit_id=audit_id,
            error_type=error_type,
        )
    except Exception as exc:
        log.warning(
            "critical_email_failed",
            pipeline_id=pipeline_id,
            error=str(exc),
        )


async def _get_batch() -> list[dict[str, Any]]:
    global _bigquery_ok

    try:
        rows = await asyncio.to_thread(ds.fetch_batch_sync)
        _bigquery_ok = True
        return [dict(row) for row in rows]
    except Exception as exc:
        _bigquery_ok = False
        raise RuntimeError(
            f"BigQuery fetch failed: {exc}"
        ) from exc


async def _ask_agent_for_injection(
    pipeline_id: str,
    rows: list[dict[str, Any]],
    allowed_class: str,
) -> dict[str, Any]:
    result = await agents.plan_injection(
        _client,
        pipeline_id,
        allowed_class,
        len(rows),
    )

    if not isinstance(result, dict):
        result = {}

    error_type = str(
        result.get("error_type")
        or ""
    ).strip()

    if allowed_class == "critical":
        error_type = "row_count_anomaly"
    elif error_type not in ROUTINE_TYPES:
        error_type = ROUTINE_TYPES[
            (len(rows) + len(pipeline_id)) % len(ROUTINE_TYPES)
        ]

    result["error_type"] = error_type
    result["failure_class"] = allowed_class

    return result


async def _inject_failure(
    rows: list[dict[str, Any]],
    plan: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    error_type = plan["error_type"]

    bad_rows, details = await ds.inject(
        rows,
        error_type,
        plan,
    )

    if not isinstance(details, dict):
        details = {"details": str(details)}

    details["error_type"] = error_type

    return (
        [dict(row) for row in bad_rows],
        details,
    )


async def _validate(
    rows: list[dict[str, Any]],
    expected_error: str | None = None,
) -> dict[str, Any]:
    result = await ds.validate(
        rows,
        expected_error,
    )

    if isinstance(result, dict):
        return result

    return {
        "valid": bool(result),
        "findings": [],
    }


async def _classify_with_agent(
    *,
    pipeline_id: str,
    error_type: str,
    validation: dict[str, Any],
    injection: dict[str, Any],
) -> dict[str, Any]:
    context = {
        "pipeline_id": pipeline_id,
        "error_type": error_type,
        "validation": _jsonable(validation),
        "injection": _jsonable(injection),
    }

    try:
        result = await agents.diagnose_failure(
            _client,
            context,
        )
    except Exception as exc:
        result = {
            "root_cause": f"Automated diagnosis fallback: {exc}",
            "recommendation": (
                "Use the whitelisted remediation for this failure type."
            ),
            "confidence": 0.75,
        }

    if not isinstance(result, dict):
        result = {}

    return result


async def _apply_routine_fix(
    conn,
    *,
    pipeline_id: str,
    audit_id: str,
    rows: list[dict[str, Any]],
    error_type: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    action_map = getattr(
        ds,
        "ALLOWED_ACTION",
        {},
    )

    action = action_map.get(error_type)

    if not action:
        raise ValueError(
            f"No whitelisted automatic action for {error_type}"
        )

    fixed_rows = await ds.apply_fix(
        rows,
        error_type,
        action,
    )

    if fixed_rows is None:
        fixed_rows = rows

    fixed_rows = [dict(row) for row in fixed_rows]

    validation = await _validate(
        fixed_rows,
        None,
    )

    valid = bool(
        validation.get("valid", False)
    )

    if not valid:
        raise RuntimeError(
            f"Automatic remediation failed validation for {error_type}"
        )

    await _update_audit(
        conn,
        audit_id,
        status="auto_fixed",
        action=action,
        explanation=(
            f"Routine failure {error_type} was automatically "
            f"remediated using the whitelisted action {action}."
        ),
    )

    await _commit_good(
        conn,
        fixed_rows,
        pipeline_id,
    )

    return fixed_rows, validation


async def process_pipeline(
    pipeline_id: str,
    *,
    count_toward_cycle: bool = True,
    forced_error_type: str | None = None,
) -> dict[str, Any]:
    global _rows_in_batch
    global _last_error

    pool = await get_pool()

    async with pool.acquire() as conn:
        if await _has_open_failure(
            conn,
            pipeline_id,
        ):
            return {
                "pipeline_id": pipeline_id,
                "status": "pending_approval",
                "message": "Pipeline has an unresolved failure.",
            }

        rows = await _get_batch()

        _rows_in_batch = len(rows)

        if not rows:
            raise RuntimeError(
                "BigQuery returned no rows."
            )

        if pipeline_id == DEMO_PIPELINE and not _last_good:
            await _commit_good(
                conn,
                rows,
                pipeline_id,
            )

        if count_toward_cycle:
            failure_number = await _get_failure_number(
                conn
            )
            failure_class = _failure_class(
                failure_number
            )
        else:
            failure_number = 0
            failure_class = (
                "critical"
                if forced_error_type in CRITICAL_TYPES
                else "routine"
            )

        if forced_error_type:
            allowed_class = (
                "critical"
                if forced_error_type in CRITICAL_TYPES
                else "routine"
            )
        else:
            allowed_class = failure_class

        plan = await _ask_agent_for_injection(
            pipeline_id,
            rows,
            allowed_class,
        )

        if forced_error_type:
            plan["error_type"] = forced_error_type

        error_type = plan["error_type"]

        if failure_class == "critical":
            error_type = "row_count_anomaly"
            plan["error_type"] = error_type

        bad_rows, injection = await _inject_failure(
            rows,
            plan,
        )

        validation = await _validate(
            bad_rows,
            error_type,
        )

        diagnosis = await _classify_with_agent(
            pipeline_id=pipeline_id,
            error_type=error_type,
            validation=validation,
            injection=injection,
        )

        root_cause = str(
            diagnosis.get(
                "root_cause",
                f"Detected {error_type}.",
            )
        )

        recommendation = str(
            diagnosis.get(
                "recommendation",
                "Review the detected failure.",
            )
        )

        explanation = str(
            diagnosis.get(
                "explanation",
                recommendation,
            )
        )

        confidence = diagnosis.get(
            "confidence",
            0.85,
        )

        try:
            confidence = float(confidence)
        except Exception:
            confidence = 0.85

        rows_affected = int(
            validation.get(
                "rows_affected",
                abs(len(bad_rows) - len(rows)),
            )
            or 0
        )

        downstream_impact = str(
            diagnosis.get(
                "downstream_impact",
                (
                    "Potential downstream impact requires "
                    "engineer review."
                    if error_type in CRITICAL_TYPES
                    else "Low-risk impact limited to this batch."
                ),
            )
        )

        risk_if_approved = str(
            diagnosis.get(
                "risk_if_approved",
                (
                    "Potential propagation of incorrect row counts "
                    "to downstream consumers."
                    if error_type in CRITICAL_TYPES
                    else "Low risk when validation succeeds."
                ),
            )
        )

        risk_if_rejected = str(
            diagnosis.get(
                "risk_if_rejected",
                (
                    "Pipeline remains on the previous known-good "
                    "batch until manually resolved."
                ),
            )
        )

        alternatives = diagnosis.get(
            "alternatives",
            [
                {
                    "action": "reload_last_good_batch",
                    "label": "Reload last known-good batch",
                },
                {
                    "action": "accept_partial_batch",
                    "label": "Accept partial batch as-is",
                },
                {
                    "action": "skip_and_keep_previous",
                    "label": "Skip run and keep previous batch",
                },
            ],
        )

        status = (
            "pending_approval"
            if error_type in CRITICAL_TYPES
            else "detected"
        )

        audit_id = await _create_audit(
            conn,
            pipeline_id=pipeline_id,
            status=status,
            error_type=error_type,
            root_cause=root_cause,
            action=(
                "pending_human_approval"
                if error_type in CRITICAL_TYPES
                else getattr(
                    ds,
                    "ALLOWED_ACTION",
                    {},
                ).get(error_type)
            ),
            explanation=explanation,
            error_log=str(
                injection
            ),
            model=str(
                diagnosis.get(
                    "model",
                    "Groq",
                )
            ),
            confidence=confidence,
            rows_affected=rows_affected,
            recommendation=recommendation,
            downstream_impact=downstream_impact,
            risk_if_approved=risk_if_approved,
            risk_if_rejected=risk_if_rejected,
            alternatives=alternatives,
        )

        if count_toward_cycle:
            await _advance_failure_number(conn)

        if error_type in CRITICAL_TYPES:
            _held[audit_id] = bad_rows

            await _broadcast(
                {
                    "type": "critical_failure",
                    "pipeline_id": pipeline_id,
                    "audit_id": audit_id,
                    "error_type": error_type,
                    "status": "pending_approval",
                    "recommendation": recommendation,
                    "downstream_impact": downstream_impact,
                    "risk_if_approved": risk_if_approved,
                    "risk_if_rejected": risk_if_rejected,
                    "alternatives": alternatives,
                }
            )

            await _send_alert(
                pipeline_id,
                audit_id,
                error_type,
            )

            return {
                "pipeline_id": pipeline_id,
                "audit_id": audit_id,
                "status": "pending_approval",
                "error_type": error_type,
                "recommendation": recommendation,
                "downstream_impact": downstream_impact,
                "risk_if_approved": risk_if_approved,
                "risk_if_rejected": risk_if_rejected,
                "alternatives": alternatives,
            }

        fixed_rows, fixed_validation = await _apply_routine_fix(
            conn,
            pipeline_id=pipeline_id,
            audit_id=audit_id,
            rows=bad_rows,
            error_type=error_type,
        )

        await _broadcast(
            {
                "type": "auto_fixed",
                "pipeline_id": pipeline_id,
                "audit_id": audit_id,
                "error_type": error_type,
                "status": "auto_fixed",
                "action": getattr(
                    ds,
                    "ALLOWED_ACTION",
                    {},
                ).get(error_type),
                "rows_affected": rows_affected,
            }
        )

        return {
            "pipeline_id": pipeline_id,
            "audit_id": audit_id,
            "status": "auto_fixed",
            "error_type": error_type,
            "rows": len(fixed_rows),
            "validation": fixed_validation,
        }


async def approve_failure(
    pipeline_id: str,
    audit_id: str,
    approved_by: str,
) -> dict[str, Any]:
    pool = await get_pool()

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT *
            FROM public.audit_log
            WHERE id = $1
              AND pipeline_id = $2
            """,
            audit_id,
            pipeline_id,
        )

        if not row:
            raise ValueError(
                "Audit record not found."
            )

        if row["status"] != "pending_approval":
            raise ValueError(
                "This failure is no longer awaiting approval."
            )

        good_rows = await _load_last_good(
            conn,
            pipeline_id,
        )

        if not good_rows:
            raise RuntimeError(
                "No known-good batch is available for recovery."
            )

        await ds.load(
            conn,
            good_rows,
        )

        await _commit_good(
            conn,
            good_rows,
            pipeline_id,
        )

        await _update_audit(
            conn,
            audit_id,
            status="approved",
            action="reload_last_good_batch",
            explanation=(
                f"Critical failure approved by {approved_by}. "
                "The pipeline was restored to its last known-good batch."
            ),
        )

        _held.pop(audit_id, None)

        await _broadcast(
            {
                "type": "critical_resolved",
                "pipeline_id": pipeline_id,
                "audit_id": audit_id,
                "status": "approved",
                "approved_by": approved_by,
            }
        )

        return {
            "pipeline_id": pipeline_id,
            "audit_id": audit_id,
            "status": "approved",
            "action": "reload_last_good_batch",
        }


async def reject_failure(
    pipeline_id: str,
    audit_id: str,
    rejected_by: str,
) -> dict[str, Any]:
    pool = await get_pool()

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT *
            FROM public.audit_log
            WHERE id = $1
              AND pipeline_id = $2
            """,
            audit_id,
            pipeline_id,
        )

        if not row:
            raise ValueError(
                "Audit record not found."
            )

        if row["status"] != "pending_approval":
            raise ValueError(
                "This failure is no longer awaiting approval."
            )

        await _update_audit(
            conn,
            audit_id,
            status="rejected",
            action="manual_fix_required",
            explanation=(
                f"Critical recommendation rejected by {rejected_by}. "
                "Manual remediation is required."
            ),
        )

        await _broadcast(
            {
                "type": "critical_rejected",
                "pipeline_id": pipeline_id,
                "audit_id": audit_id,
                "status": "rejected",
                "rejected_by": rejected_by,
            }
        )

        return {
            "pipeline_id": pipeline_id,
            "audit_id": audit_id,
            "status": "rejected",
            "manual_fix_required": True,
        }


async def manual_fix(
    pipeline_id: str,
    audit_id: str,
    fixed_by: str,
    action: str | None,
    instruction: str | None,
) -> dict[str, Any]:
    global _last_good, _baseline

    if not action and instruction:
        action = await agents.interpret_instruction(
            _client,
            instruction,
        )

    manual_actions = getattr(
        ds,
        "MANUAL_ACTIONS",
        {},
    )

    if action not in manual_actions:
        raise ValueError(
            "Invalid manual action."
        )

    pool = await get_pool()

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT *
            FROM public.audit_log
            WHERE id = $1
              AND pipeline_id = $2
            """,
            audit_id,
            pipeline_id,
        )

        if not row:
            raise ValueError(
                "Audit record not found."
            )

        if row["status"] not in (
            "pending_approval",
            "rejected",
        ):
            raise ValueError(
                "This failure is not available for manual remediation."
            )

        if action == "reload_last_good_batch":
            rows = await _load_last_good(
                conn,
                pipeline_id,
            )

            if not rows:
                raise RuntimeError(
                    "No known-good batch is available."
                )

            await ds.load(
                conn,
                rows,
            )

            _last_good = [
                dict(item)
                for item in rows
            ]
            _baseline = len(rows)

            await _commit_good(
                conn,
                rows,
                pipeline_id,
            )

        elif action == "accept_partial_batch":
            rows = _held.get(
                audit_id,
                [],
            )

            if not rows:
                raise RuntimeError(
                    "The affected batch is no longer available "
                    "in memory. Reload the last known-good batch instead."
                )

            await ds.load(
                conn,
                rows,
            )

            _last_good = [
                dict(item)
                for item in rows
            ]
            _baseline = len(rows)

        elif action == "skip_and_keep_previous":
            rows = await _load_last_good(
                conn,
                pipeline_id,
            )

            if not rows:
                raise RuntimeError(
                    "No known-good batch is available."
                )

            await ds.load(
                conn,
                rows,
            )

            _last_good = [
                dict(item)
                for item in rows
            ]
            _baseline = len(rows)

        await _update_audit(
            conn,
            audit_id,
            status="manually_fixed",
            action=action,
            explanation=(
                f"Manual remediation completed by {fixed_by} "
                f"using {manual_actions[action]}."
            ),
        )

        _held.pop(
            audit_id,
            None,
        )

        await _broadcast(
            {
                "type": "manual_fix_completed",
                "pipeline_id": pipeline_id,
                "audit_id": audit_id,
                "status": "manually_fixed",
                "action": action,
                "fixed_by": fixed_by,
            }
        )

        return {
            "pipeline_id": pipeline_id,
            "audit_id": audit_id,
            "status": "manually_fixed",
            "action": action,
        }


async def run_pipeline_once(
    pipeline_id: str,
) -> dict[str, Any]:
    try:
        return await process_pipeline(
            pipeline_id,
            count_toward_cycle=True,
        )
    except Exception as exc:
        global _last_error
        _last_error = str(exc)

        log.exception(
            "pipeline_run_failed",
            pipeline_id=pipeline_id,
            error=str(exc),
        )

        return {
            "pipeline_id": pipeline_id,
            "status": "error",
            "error": str(exc),
        }


async def scheduler_tick() -> dict[str, Any]:
    global _ticks
    global _last_tick
    global _last_error

    _ticks += 1
    _last_tick = _now()

    results = []

    for pipeline in PIPELINES:
        pipeline_id = pipeline.get(
            "id",
        )

        if not pipeline_id:
            continue

        try:
            result = await run_pipeline_once(
                pipeline_id,
            )
            results.append(result)
        except Exception as exc:
            _last_error = str(exc)

            results.append(
                {
                    "pipeline_id": pipeline_id,
                    "status": "error",
                    "error": str(exc),
                }
            )

    return {
        "tick": _ticks,
        "timestamp": _last_tick,
        "results": results,
    }


async def scheduler_loop() -> None:
    await asyncio.sleep(
        INITIAL_DELAY_SECONDS
    )

    while True:
        try:
            await scheduler_tick()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            global _last_error
            _last_error = str(exc)

            log.exception(
                "scheduler_tick_failed",
                error=str(exc),
            )

        await asyncio.sleep(
            TICK_SECONDS
        )


async def start_scheduler() -> None:
    global _scheduler_task
    global _client

    if _client is None:
        _client = httpx.AsyncClient(
            timeout=60.0
        )

    if _scheduler_task is None or _scheduler_task.done():
        _scheduler_task = asyncio.create_task(
            scheduler_loop()
        )


async def stop_scheduler() -> None:
    global _scheduler_task
    global _client

    if _scheduler_task:
        _scheduler_task.cancel()

        try:
            await _scheduler_task
        except asyncio.CancelledError:
            pass

        _scheduler_task = None

    if _client:
        await _client.aclose()
        _client = None


async def force_failure(
    pipeline_id: str,
    error_type: str,
) -> dict[str, Any]:
    if error_type not in _allowed_types():
        raise ValueError(
            f"Unsupported error type: {error_type}"
        )

    return await process_pipeline(
        pipeline_id,
        count_toward_cycle=False,
        forced_error_type=error_type,
    )


async def get_debug() -> dict[str, Any]:
    return {
        "groq_key_set": bool(
            os.getenv("GROQ_API_KEY")
        ),
        "google_creds_set": bool(
            os.getenv("GOOGLE_CREDENTIALS_JSON")
        ),
        "alerts_enabled": bool(
            os.getenv("RESEND_API_KEY")
            and os.getenv("ALERT_EMAIL")
        ),
        "ticks": _ticks,
        "last_tick": _last_tick,
        "bigquery_ok": _bigquery_ok,
        "last_error": _last_error,
        "rows_in_batch": _rows_in_batch,
        "baseline_rows": _baseline,
        "held_critical_events": len(_held),
        "failure_cycle": "1-7 routine, 8 critical",
    }


async def initialize() -> None:
    pool = await get_pool()

    async with pool.acquire() as conn:
        await _ensure_state(
            conn
        )

        try:
            existing = await _load_last_good(
                conn,
                DEMO_PIPELINE,
            )

            if existing:
                global _last_good, _baseline
                _last_good = existing
                _baseline = len(existing)
        except Exception as exc:
            log.warning(
                "initial_good_batch_load_failed",
                error=str(exc),
            )


async def shutdown() -> None:
    await stop_scheduler()
