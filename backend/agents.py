"""Groq agents.
Agent 1 (Ingestion/Chaos): decides how badly to corrupt the real Google Trends batch + writes the error log.
Agent 2 (Remediation): diagnoses, picks the fix, and for critical cases recommends approve/reject with alternatives.
Interpreter: turns a human's free-text fix instruction into one whitelisted action.
Deterministic fallbacks keep the demo alive if Groq is unavailable.
"""
from __future__ import annotations
import json, os
import structlog
from data_source import ALLOWED_ACTION, MANUAL_ACTIONS, FRACTION_BOUNDS, clamp_fraction
from error_classifier import ERROR_PLAYBOOK

log = structlog.get_logger(__name__)
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
CONTEXT = ("Data: Google Trends top-25 search terms per US region (BigQuery public dataset). Downstream: "
           "trending-terms dashboard, regional-leaders report, ad-demand forecasting features.")


def model_name() -> str:
    return os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")


async def _chat(client, system: str, user: str) -> dict | None:
    key = os.getenv("GROQ_API_KEY", "")
    if not key:
        return None
    try:
        r = await client.post(
            GROQ_URL, timeout=25, headers={"Authorization": f"Bearer {key}"},
            json={"model": model_name(), "temperature": 0.3, "response_format": {"type": "json_object"},
                  "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}]})
        r.raise_for_status()
        return json.loads(r.json()["choices"][0]["message"]["content"])
    except Exception as e:
        log.warning("groq_call_failed", error=str(e))
        return None


# ---------------- Agent 1 ----------------
async def plan_injection(client, pipeline: dict, error_type: str, n_rows: int) -> tuple[float, str]:
    lo, hi = FRACTION_BOUNDS[error_type]
    out = await _chat(
        client,
        f"You are the Ingestion Agent acting as a chaos tester. {CONTEXT} Decide how severe the '{error_type}' "
        f"fault should be for this run. Reply as JSON: {{\"fraction\": number between {lo} and {hi}, "
        "\"scenario\": \"one sentence describing the upstream cause\"}",
        f"Pipeline: {pipeline['name']}. Batch rows: {n_rows}.")
    try:
        return clamp_fraction(error_type, float(out["fraction"])), str(out.get("scenario", ""))
    except Exception:
        return (lo + hi) / 2, ""


async def write_error_log(client, pipeline: dict, error_type: str, findings: list[dict], n_rows: int, scenario: str) -> tuple[str, str]:
    evidence = "; ".join(f["detail"] for f in findings)
    out = await _chat(
        client,
        f"You are the Ingestion Agent. {CONTEXT} Write the realistic error log a loader/validator would emit: "
        "3-5 lines with timestamps, task name, exception/check name and the concrete numbers from the evidence. "
        "Do NOT suggest fixes. Reply as JSON: {\"error_log\": \"...\"}",
        f"Pipeline: {pipeline['name']} ({pipeline['dag_id']})\nError type: {error_type}\nScenario: {scenario}\n"
        f"Evidence: {evidence}\nBatch size: {n_rows}")
    if out and isinstance(out.get("error_log"), str) and out["error_log"].strip():
        return out["error_log"].strip(), model_name()
    return f"[{pipeline['dag_id']}] ERROR {error_type}: {evidence}", "rule-based-fallback"


# ---------------- Agent 2 ----------------
_FALLBACK_WHY = {
    "reload_last_good_batch": "Safest: restores the last validated data; recent changes arrive on the next run.",
    "accept_partial_batch": "Fastest, but dashboards will under-report until the next full run.",
    "skip_and_keep_previous": "No data change; downstream keeps yesterday's numbers and stays consistent.",
}


def _alternatives(raw, recommended: str) -> list[dict]:
    seen, out = set(), []
    for a in (raw if isinstance(raw, list) else []):
        act = a.get("action") if isinstance(a, dict) else None
        if act in MANUAL_ACTIONS and act not in seen:
            seen.add(act)
            out.append({"action": act, "label": MANUAL_ACTIONS[act], "why": str(a.get("why", ""))[:200] or _FALLBACK_WHY[act]})
    for act in MANUAL_ACTIONS:
        if act not in seen:
            out.append({"action": act, "label": MANUAL_ACTIONS[act], "why": _FALLBACK_WHY[act]})
    return sorted(out, key=lambda x: x["action"] != recommended)


async def remediation_agent(client, error_type: str, error_log: str, findings: list[dict]) -> dict:
    auto = ALLOWED_ACTION[error_type]
    pb = ERROR_PLAYBOOK[error_type]
    out = await _chat(
        client,
        f"You are the Remediation Agent. {CONTEXT} Read the failure log and validator evidence, find the root cause "
        f"and choose ONE action from: {', '.join(sorted(set(ALLOWED_ACTION.values())))}. Reply as JSON: "
        "{\"root_cause\": str, \"action\": str, \"explanation\": str (1-2 plain sentences), \"risk\": \"low\"|\"high\", "
        "\"confidence\": number 0-1, \"recommendation\": \"approve\"|\"reject\", \"downstream_impact\": str, "
        "\"risk_if_approved\": str, \"risk_if_rejected\": str, \"alternatives\": [{\"action\": one of "
        f"{list(MANUAL_ACTIONS)}, \"why\": str}}]}}",
        f"Error log:\n{error_log}\n\nValidator evidence: {json.dumps(findings)}")
    if not out:
        return {"root_cause": pb["description"], "action": auto, "explanation": pb["proposed_fix"],
                "risk": "low" if pb["auto_fixable"] else "high", "confidence": 0.7, "model": "rule-based-fallback",
                "recommendation": "approve",
                "downstream_impact": "Trending-terms and regional-leader marts would under-report until fixed.",
                "risk_if_approved": "Last good batch is slightly stale; new data arrives on the next run.",
                "risk_if_rejected": "Dashboards keep a partial batch and the incident escalates to on-call.",
                "alternatives": _alternatives([], auto)}
    action, note = out.get("action"), ""
    if action != auto:   # guardrail: only the whitelisted action for this error type may run
        note = f" (model proposed '{action}'; overridden by playbook guardrail)"
        action = auto
    try:
        conf = float(out.get("confidence", 0.7))
    except (TypeError, ValueError):
        conf = 0.7
    return {"root_cause": str(out.get("root_cause", ""))[:400], "action": action,
            "explanation": str(out.get("explanation", ""))[:500] + note,
            "risk": out.get("risk", "low"), "confidence": conf, "model": model_name(),
            "recommendation": "reject" if str(out.get("recommendation")).lower() == "reject" else "approve",
            "downstream_impact": str(out.get("downstream_impact", ""))[:400],
            "risk_if_approved": str(out.get("risk_if_approved", ""))[:300],
            "risk_if_rejected": str(out.get("risk_if_rejected", ""))[:300],
            "alternatives": _alternatives(out.get("alternatives"), auto)}


# ---------------- Human instruction -> whitelisted action ----------------
async def interpret_instruction(client, text: str) -> str | None:
    out = await _chat(
        client,
        "Map the engineer's instruction to exactly one action. Options: "
        + "; ".join(f"{k} = {v}" for k, v in MANUAL_ACTIONS.items())
        + ". If none fits, use null. Reply as JSON: {\"action\": str|null}",
        text)
    if out and out.get("action") in MANUAL_ACTIONS:
        return out["action"]
    t = text.lower()
    if any(w in t for w in ("skip", "ignore", "keep previous", "leave")):
        return "skip_and_keep_previous"
    if any(w in t for w in ("partial", "as-is", "as is", "accept")):
        return "accept_partial_batch"
    if any(w in t for w in ("reload", "restore", "last good", "backup", "rollback")):
        return "reload_last_good_batch"
    return None
