"""Failure playbook. Critical types always need a human; the rest are auto-healed by Agent 2."""
from __future__ import annotations
import structlog

log = structlog.get_logger(__name__)

CRITICAL_TYPES = {"row_count_anomaly"}  # 1 of every 8 injected errors

ERROR_PLAYBOOK = {
    "schema_mismatch": {"description": "Unexpected / renamed columns in source feed",
                        "proposed_fix": "Map renamed columns back to contract, drop unexpected ones, rerun", "auto_fixable": True},
    "null_threshold_exceeded": {"description": "Nulls exceeded 10% in critical column",
                                "proposed_fix": "Quarantine affected rows, load clean rows only", "auto_fixable": True},
    "data_type_mismatch": {"description": "Column type changed in source",
                           "proposed_fix": "Cast to expected type with fallback to NULL", "auto_fixable": True},
    "duplicate_records": {"description": "Duplicate primary keys in incoming batch",
                          "proposed_fix": "Deduplicate on id, keep first occurrence", "auto_fixable": True},
    "invalid_range": {"description": "Values outside physically valid ranges",
                      "proposed_fix": "Quarantine out-of-range rows, load the rest", "auto_fixable": True},
    "row_count_anomaly": {"description": "Row count dropped >50% vs last good batch",
                          "proposed_fix": "Reload from last known-good batch snapshot", "auto_fixable": False},
}


def classify_error(error_type: str) -> dict:
    entry = ERROR_PLAYBOOK.get(error_type)
    if entry is None:
        log.warning("unknown_error_type", error_type=error_type)
        return {"error_type": error_type, "description": "Unclassified failure — manual investigation required",
                "proposed_fix": "Escalate to on-call data engineer", "auto_fixable": False}
    return {"error_type": error_type, **entry}
