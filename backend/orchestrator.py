from __future__ import annotations

import json
import os

import structlog

from data_source import ALLOWED_ACTION, MANUAL_ACTIONS, FRACTION_BOUNDS, clamp_fraction
from error_classifier import ERROR_PLAYBOOK

log = structlog.get_logger(__name__)

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"

ROUTINE_ERROR_TYPES = [
    "schema_mismatch",
    "null_threshold_exceeded",
    "data_type_mismatch",
    "duplicate_records",
    "invalid_range",
]

CRITICAL_ERROR_TYPES = [
    "row_count_anomaly",
]

CONTEXT = (
    "Data source: real Google Trends top-25 search terms per US region from the "
    "BigQuery public dataset. Downstream systems include trending-terms dashboards, "
    "regional leader reports, and ad-demand forecasting features."
)


def model_name() -> str:
    return os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")


async def _chat(client, system: str, user: str) -> dict | None:
    key = os.getenv("GROQ_API_KEY", "")
    if not key or client is None:
        return None

    try:
        response = await client.post(
            GROQ_URL,
            timeout=25,
            headers={"Authorization": f"Bearer {key}"},
            json={
                "model": model_name(),
                "temperature": 0.3,
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
            },
        )
        response.raise_for_status()
        return json.loads(response.json()["choices"][0]["message"]["content"])
    except Exception as exc:
        log.warning("groq_call_failed", error=str(exc))
        return None


async def generate_failure(
    client,
    pipeline: dict,
    n_rows: int,
    severity: str,
    failure_number: int,
) -> dict:
    if severity == "critical":
        allowed = CRITICAL_ERROR_TYPES
    else:
        allowed = ROUTINE_ERROR_TYPES

    allowed_text = ", ".join(allowed)

    out = await _chat(
        client,
        (
            "You are Agent 1, the Data Ingestion Chaos Agent for a controlled ETL "
            "reliability test. You must generate a realistic failure against a copy "
            "of the real incoming batch. You may ONLY choose an error type from the "
            f"allowed list: {allowed_text}. The severity class is {severity}. "
            "Do not choose anything outside the list. For a critical slot, the "
            "error must be row_count_anomaly. For a routine slot, never choose "
            "row_count_anomaly. Choose a realistic corruption fraction within the "
            "allowed range for that error type and describe the upstream scenario. "
            "Return JSON only with error_type, fraction and scenario."
        ),
        (
            f"Pipeline: {pipeline['name']}\n"
            f"Batch rows: {n_rows}\n"
            f"Failure number in controlled cycle: {failure_number}\n"
            f"Severity: {severity}\n"
            f"Allowed error types: {allowed_text}"
        ),
    )

    fallback_type = (
        "row_count_anomaly"
        if severity == "critical"
        else ROUTINE_ERROR_TYPES[(failure_number - 1) % len(ROUTINE_ERROR_TYPES)]
    )

    error_type = fallback_type
    fraction = None
    scenario = ""

    if out:
        candidate = str(out.get("error_type", "")).strip()
        if candidate in allowed:
            error_type = candidate

        try:
            fraction = float(out.get("fraction"))
        except (TypeError, ValueError):
            fraction = None

        scenario = str(out.get("scenario", "")).strip()[:500]

    lo, hi = FRACTION_BOUNDS[error_type]

    if fraction is None:
        fraction = (lo + hi) / 2

    fraction = clamp_fraction(error_type, fraction)

    if not scenario:
        scenario = (
            "Upstream replay or transformation drift introduced a controlled "
            "data-quality fault into the incoming batch."
        )

    return {
        "error_type": error_type,
        "fraction": fraction,
        "scenario": scenario,
        "model": model_name() if out else "rule-based-fallback",
    }


async def write_error_log(
    client,
    pipeline: dict,
    error_type: str,
    findings: list[dict],
    n_rows: int,
    scenario: str,
) -> tuple[str, str]:
    evidence = "; ".join(str(item["detail"]) for item in findings)

    out = await _chat(
        client,
        (
            "You are Agent 1 writing the loader/validator error log for a real ETL "
            "incident. Write 3-5 realistic lines containing a timestamp, task name, "
            "check or exception name, and concrete evidence numbers. Do not suggest "
            "a fix. Return JSON with error_log."
        ),
        (
            f"Pipeline: {pipeline['name']} ({pipeline['dag_id']})\n"
            f"Error type: {error_type}\n"
            f"Scenario: {scenario}\n"
            f"Evidence: {evidence}\n"
            f"Batch size: {n_rows}"
        ),
    )

    if out and isinstance(out.get("error_log"), str) and out["error_log"].strip():
        return out["error_log"].strip(), model_name()

    return (
        f"[{pipeline['dag_id']}] ERROR {error_type}: {evidence}",
        "rule-based-fallback",
    )


FALLBACK_WHY = {
    "reload_last_good_batch": "Restores the last validated batch and avoids propagating the incident.",
    "accept_partial_batch": "Keeps the available rows flowing but may leave downstream data incomplete.",
    "skip_and_keep_previous": "Keeps the previous trusted state until a later run provides clean data.",
}


def _alternatives(raw, recommended: str) -> list[dict]:
    result = []
    seen = set()

    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue

            action = item.get("action")

            if action not in MANUAL_ACTIONS or action in seen:
                continue

            seen.add(action)

            result.append(
                {
                    "rank": len(result) + 1,
                    "action": action,
                    "label": MANUAL_ACTIONS[action],
                    "why": str(
                        item.get("why", "") or FALLBACK_WHY[action]
                    )[:250],
                }
            )

            if len(result) == 3:
                break

    ordered_actions = list(MANUAL_ACTIONS.keys())

    if recommended in ordered_actions:
        ordered_actions.remove(recommended)
        ordered_actions.insert(0, recommended)

    for action in ordered_actions:
        if action in seen:
            continue

        result.append(
            {
                "rank": len(result) + 1,
                "action": action,
                "label": MANUAL_ACTIONS[action],
                "why": FALLBACK_WHY[action],
            }
        )
        seen.add(action)

        if len(result) == 3:
            break

    for index, item in enumerate(result[:3], start=1):
        item["rank"] = index

    return result[:3]


async def remediation_agent(
    client,
    error_type: str,
    error_log: str,
    findings: list[dict],
) -> dict:
    auto_action = ALLOWED_ACTION[error_type]
    playbook = ERROR_PLAYBOOK[error_type]

    out = await _chat(
        client,
        (
            "You are Agent 2, the Remediation Agent. Diagnose the ETL incident using "
            "the error log and validator evidence. Choose one action from the "
            "available automatic actions. The system will enforce a hard playbook "
            "guardrail before executing it. For a critical row_count_anomaly, "
            "recommend whether the operator should approve or reject the proposed "
            "recovery. Also provide downstream impact, risk if approved, risk if "
            "rejected, and exactly three ranked human alternatives. Return JSON."
        ),
        (
            f"Error type: {error_type}\n"
            f"Error log:\n{error_log}\n\n"
            f"Validator evidence:\n{json.dumps(findings)}\n\n"
            f"Automatic playbook action: {auto_action}"
        ),
    )

    if not out:
        return {
            "root_cause": playbook["description"],
            "action": auto_action,
            "explanation": playbook["proposed_fix"],
            "risk": "high" if not playbook["auto_fixable"] else "low",
            "confidence": 0.7,
            "model": "rule-based-fallback",
            "recommendation": "approve",
            "downstream_impact": (
                "Trending-term dashboards and regional leader outputs may remain "
                "stale or incomplete until the incident is resolved."
            ),
            "risk_if_approved": (
                "The system uses the last validated batch, which may be slightly stale."
            ),
            "risk_if_rejected": (
                "The affected pipeline remains blocked from publishing the unsafe batch."
            ),
            "alternatives": _alternatives([], auto_action),
        }

    action = out.get("action")
    note = ""

    if action != auto_action:
        note = (
            f" Model proposed '{action}', but the playbook guardrail enforced "
            f"'{auto_action}'."
        )
        action = auto_action

    try:
        confidence = float(out.get("confidence", 0.7))
    except (TypeError, ValueError):
        confidence = 0.7

    confidence = max(0.0, min(1.0, confidence))

    recommendation = (
        "reject"
        if str(out.get("recommendation", "")).lower() == "reject"
        else "approve"
    )

    return {
        "root_cause": str(out.get("root_cause", ""))[:500] or playbook["description"],
        "action": action,
        "explanation": (
            str(out.get("explanation", ""))[:600] or playbook["proposed_fix"]
        ) + note,
        "risk": str(out.get("risk", "low")),
        "confidence": confidence,
        "model": model_name(),
        "recommendation": recommendation,
        "downstream_impact": str(
            out.get("downstream_impact", "")
        )[:500],
        "risk_if_approved": str(
            out.get("risk_if_approved", "")
        )[:400],
        "risk_if_rejected": str(
            out.get("risk_if_rejected", "")
        )[:400],
        "alternatives": _alternatives(
            out.get("alternatives"),
            auto_action,
        ),
    }


async def interpret_instruction(client, text: str) -> str | None:
    out = await _chat(
        client,
        (
            "Map the engineer's instruction to exactly one safe action. "
            "Never invent an action. Available actions are: "
            + "; ".join(
                f"{key} = {value}"
                for key, value in MANUAL_ACTIONS.items()
            )
            + ". Return JSON with action or null."
        ),
        text,
    )

    if out and out.get("action") in MANUAL_ACTIONS:
        return out["action"]

    value = text.lower()

    if any(
        word in value
        for word in ("reload", "restore", "last good", "backup", "rollback")
    ):
        return "reload_last_good_batch"

    if any(
        word in value
        for word in ("partial", "as-is", "as is", "accept")
    ):
        return "accept_partial_batch"

    if any(
        word in value
        for word in ("skip", "ignore", "keep previous", "leave")
    ):
        return "skip_and_keep_previous"

    return None
