"""Self-healing error classification playbook for Pipeline Autopilot.

Maps Great Expectations / ingestion failures to a human-readable diagnosis,
a proposed remediation, and whether the platform is allowed to auto-heal it
(subject to human approval in the Control Tower).
"""
from __future__ import annotations
from typing import Optional
import structlog

log = structlog.get_logger(__name__)

ERROR_PLAYBOOK = {
    "schema_mismatch": {
        "description": "Unexpected column detected",
        "proposed_fix": "Add column with NULL default and rerun",
        "auto_fixable": True,
    },
    "null_threshold_exceeded": {
        "description": "Nulls exceeded 10% in critical column",
        "proposed_fix": "Quarantine affected rows, load clean rows only",
        "auto_fixable": True,
    },
    "row_count_anomaly": {
        "description": "Row count dropped >50% vs yesterday",
        "proposed_fix": "Reload from yesterday's backup partition",
        "auto_fixable": False,
    },
    "data_type_mismatch": {
        "description": "Column type changed in source",
        "proposed_fix": "Cast to expected type with fallback to NULL",
        "auto_fixable": True,
    },
}


def classify_error(error_type: str) -> dict:
    """Return the playbook entry for a known error type.

    Falls back to a generic, non-auto-fixable entry for unknown types so the
    caller always receives a proposed fix to surface to a human operator.
    """
    entry = ERROR_PLAYBOOK.get(error_type)
    if entry is None:
        log.warning("unknown_error_type", error_type=error_type)
        return {
            "error_type": error_type,
            "description": "Unclassified failure — manual investigation required",
            "proposed_fix": "Escalate to on-call data engineer",
            "auto_fixable": False,
        }
    return {"error_type": error_type, **entry}


def classify_from_expectation(failed_check: dict) -> Optional[dict]:
    """Translate a failed Great Expectations result into an error type.

    `failed_check` is expected to carry an `expectation_type` and the
    observed metric so we can pick the right playbook bucket.
    """
    exp = failed_check.get("expectation_type", "")
    if "columns_to_match" in exp or "table_columns" in exp:
        return classify_error("schema_mismatch")
    if "not_be_null" in exp:
        return classify_error("null_threshold_exceeded")
    if "table_row_count" in exp:
        return classify_error("row_count_anomaly")
    if "of_type" in exp or "in_type_list" in exp:
        return classify_error("data_type_mismatch")
    return None
