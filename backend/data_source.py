"""Real data: Google Trends public dataset in BigQuery (bigquery-public-data.google_trends.top_terms).
Top-25 Google Search terms per US region (DMA), refreshed daily by Google."""

from __future__ import annotations

import json
import logging
import os
import random
from datetime import date

logger = logging.getLogger(__name__)

EXPECTED = [
    "id", "refresh_date", "region", "region_id",
    "term", "week", "rank", "score"
]

RANK_MIN, RANK_MAX = 1, 25

# One whitelisted automatic action per error type.
ALLOWED_ACTION = {
    "schema_mismatch": "rename_columns",
    "null_threshold_exceeded": "quarantine_null_rows",
    "data_type_mismatch": "cast_types",
    "duplicate_records": "dedupe_rows",
    "invalid_range": "quarantine_out_of_range",
    "row_count_anomaly": "reload_last_good_batch",
}

# Manual choices for critical incidents.
MANUAL_ACTIONS = {
    "reload_last_good_batch": "Reload last known-good batch",
    "accept_partial_batch": "Accept the partial batch as-is",
    "skip_and_keep_previous": "Skip this run, keep previous data",
}

FRACTION_BOUNDS = {
    "null_threshold_exceeded": (0.15, 0.40),
    "data_type_mismatch": (0.02, 0.08),
    "duplicate_records": (0.10, 0.30),
    "invalid_range": (0.05, 0.20),
    "row_count_anomaly": (0.05, 0.30),
    "schema_mismatch": (1.0, 1.0),
}

# Bounded query: do not download the entire dataset.
SQL = """
SELECT
    CAST(refresh_date AS STRING) AS refresh_date,
    CAST(dma_id AS STRING) AS region_id,
    dma_name AS region,
    term,
    CAST(week AS STRING) AS week,
    rank,
    score
FROM `bigquery-public-data.google_trends.top_terms`
WHERE refresh_date >= DATE_SUB(
    CURRENT_DATE(), INTERVAL 7 DAY
)
ORDER BY refresh_date DESC, week DESC
LIMIT 250
"""


# --------------------------------------------------
# BigQuery extraction
# --------------------------------------------------

def fetch_batch_sync() -> list[dict]:
    """Fetch a bounded real-data batch from BigQuery."""
    from google.cloud import bigquery
    from google.oauth2 import service_account

    info = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])

    creds = service_account.Credentials.from_service_account_info(
        info
    )

    client = bigquery.Client(
        project=info["project_id"],
        credentials=creds,
    )

    cfg = bigquery.QueryJobConfig(
        maximum_bytes_billed=2 * 1024**3
    )

    logger.info("Starting BigQuery extraction")

    try:
        query_job = client.query(SQL, job_config=cfg)

        results = query_job.result(
            page_size=250,
            timeout=45,
        )

        raw = [dict(row) for row in results]

        logger.info(
            "BigQuery returned %d rows",
            len(raw)
        )

        if not raw:
            logger.warning("BigQuery returned no rows")
            return []

        latest = max(
            row["refresh_date"] for row in raw
        )

        raw = [
            row for row in raw
            if row["refresh_date"] == latest
        ]

        latest_week = max(
            row["week"] for row in raw
        )

        batch = []

        for r in raw:
            if r["week"] != latest_week:
                continue

            batch.append({
                "id": (
                    f"{r['refresh_date']}|"
                    f"{r['region_id']}|"
                    f"{r['term']}|"
                    f"{r['week']}"
                ),
                "refresh_date": r["refresh_date"],
                "region": r["region"] or "unknown",
                "region_id": r["region_id"],
                "term": r["term"],
                "week": r["week"],
                "rank": int(r["rank"]),
                "score": (
                    None if r["score"] is None
                    else float(r["score"])
                ),
            })

        logger.info(
            "Prepared ETL batch: %d rows",
            len(batch)
        )

        return batch

    except Exception:
        logger.exception("BigQuery extraction failed")
        raise

    finally:
        client.close()


# --------------------------------------------------
# Agent 1: Inject errors into a separate copy
# --------------------------------------------------

def clamp_fraction(error_type: str, f: float) -> float:
    lo, hi = FRACTION_BOUNDS.get(
        error_type, (0.1, 0.3)
    )
    return max(lo, min(hi, f))


def inject(
    error_type: str,
    rows: list[dict],
    rng: random.Random,
    fraction: float | None = None,
) -> list[dict]:

    # Preserve the original clean batch.
    rows = [dict(r) for r in rows]

    n = len(rows)

    if n == 0:
        return rows

    lo, hi = FRACTION_BOUNDS.get(
        error_type, (0.1, 0.3)
    )

    frac = clamp_fraction(
        error_type,
        fraction if fraction is not None
        else (lo + hi) / 2,
    )

    k = min(n, max(1, int(n * frac)))

    if error_type == "schema_mismatch":
        for r in rows:
            r["rank_position"] = r.pop("rank")
            r["gt_schema_v2"] = "v2"

    elif error_type == "null_threshold_exceeded":
        for r in rng.sample(rows, k):
            r["rank"] = None

    elif error_type == "data_type_mismatch":
        for r in rows:
            r["rank"] = str(r["rank"])

        for r in rng.sample(rows, k):
            r["rank"] = "N/A"

    elif error_type == "duplicate_records":
        rows += [
            dict(r) for r in rng.sample(rows, k)
        ]

    elif error_type == "invalid_range":
        for r in rng.sample(rows, k):
            r["rank"] = rng.choice([0, -3, 999])

    elif error_type == "row_count_anomaly":
        rows = rng.sample(rows, k)

    return rows


# --------------------------------------------------
# Validation engine
# --------------------------------------------------

def validate(
    rows: list[dict],
    baseline: int,
) -> list[dict]:

    if not rows:
        return [{
            "type": "row_count_anomaly",
            "detail": "batch is empty",
        }]

    cols = set().union(*(r.keys() for r in rows))

    missing = set(EXPECTED) - cols
    extra = cols - set(EXPECTED)

    if missing or extra:
        return [{
            "type": "schema_mismatch",
            "detail": (
                f"missing columns {sorted(missing)}, "
                f"unexpected columns {sorted(extra)}"
            ),
        }]

    n = len(rows)

    strs = sum(
        1 for r in rows
        if isinstance(r["rank"], str)
    )

    if strs:
        return [{
            "type": "data_type_mismatch",
            "detail": (
                f"column 'rank' expected int, "
                f"{strs}/{n} values are strings"
            ),
        }]

    nulls = sum(
        1 for r in rows
        if r["rank"] is None
    )

    if nulls / n > 0.10:
        return [{
            "type": "null_threshold_exceeded",
            "detail": (
                f"'rank' null ratio {nulls/n:.0%} "
                f"exceeds 10% threshold ({nulls}/{n})"
            ),
        }]

    dups = n - len({
        r["id"] for r in rows
    })

    if dups:
        return [{
            "type": "duplicate_records",
            "detail": (
                f"{dups} duplicate primary keys on 'id'"
            ),
        }]

    oor = sum(
        1 for r in rows
        if r["rank"] is not None
        and not (
            RANK_MIN <= r["rank"] <= RANK_MAX
        )
    )

    if oor:
        return [{
            "type": "invalid_range",
            "detail": (
                f"{oor} rows with 'rank' outside "
                f"valid range {RANK_MIN}..{RANK_MAX}"
            ),
        }]

    if baseline and n < 0.5 * baseline:
        return [{
            "type": "row_count_anomaly",
            "detail": (
                f"row count {n} is "
                f"{1 - n/baseline:.0%} below "
                f"last good batch ({baseline})"
            ),
        }]

    return []


# --------------------------------------------------
# Whitelisted remediation actions
# --------------------------------------------------

def apply_fix(
    action: str,
    rows: list[dict],
    last_good: list[dict],
) -> tuple[list[dict], int]:

    if action == "rename_columns":
        out = []

        for r in rows:
            r = dict(r)

            if (
                "rank_position" in r
                and "rank" not in r
            ):
                r["rank"] = r.pop(
                    "rank_position"
                )

            out.append({
                k: r.get(k)
                for k in EXPECTED
            })

        return out, 0

    if action == "quarantine_null_rows":
        keep = [
            r for r in rows
            if r["rank"] is not None
        ]
        return keep, len(rows) - len(keep)

    if action == "cast_types":
        out = []
        quarantined = 0

        for r in rows:
            r = dict(r)

            try:
                r["rank"] = int(r["rank"])
            except (TypeError, ValueError):
                quarantined += 1
                continue

            out.append(r)

        return out, quarantined

    if action == "dedupe_rows":
        seen = set()
        out = []

        for r in rows:
            if r["id"] not in seen:
                seen.add(r["id"])
                out.append(r)

        return out, len(rows) - len(out)

    if action == "quarantine_out_of_range":
        keep = [
            r for r in rows
            if (
                isinstance(r["rank"], (int, float))
                and RANK_MIN <= r["rank"] <= RANK_MAX
            )
        ]

        return keep, len(rows) - len(keep)

    if action == "reload_last_good_batch":
        return [
            dict(r) for r in last_good
        ], 0

    if action == "accept_partial_batch":
        return rows, 0

    if action == "skip_and_keep_previous":
        return [], 0

    raise ValueError(
        f"unknown action {action}"
    )


# --------------------------------------------------
# Load data: Bronze -> Silver -> Gold
# --------------------------------------------------

async def load(conn, rows: list[dict]) -> None:

    if not rows:
        return

    recs = [
        (
            r["id"],
            date.fromisoformat(r["refresh_date"]),
            r["region"],
            r["region_id"],
            r["term"],
            date.fromisoformat(r["week"]),
            r["rank"],
            r["score"],
        )
        for r in rows
    ]

    logger.info(
        "Loading %d rows into Neon",
        len(recs)
    )

    async with conn.transaction():

        await conn.executemany(
            """
            INSERT INTO bronze.raw_trends
            (
                id, refresh_date, region,
                region_id, term, week, rank, score
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (id) DO UPDATE SET
                refresh_date=EXCLUDED.refresh_date,
                region=EXCLUDED.region,
                region_id=EXCLUDED.region_id,
                term=EXCLUDED.term,
                week=EXCLUDED.week,
                rank=EXCLUDED.rank,
                score=EXCLUDED.score
            """,
            recs,
        )

        logger.info(
            "Bronze data loaded"
        )

        await conn.execute(
            """
            DELETE FROM bronze.raw_trends
            WHERE refresh_date < current_date - 7
            """
        )

        await conn.execute(
            """
            DELETE FROM silver.trends_clean
            WHERE refresh_date < current_date - 7
            """
        )

        await conn.execute(
            """
            INSERT INTO silver.trends_clean
            (
                id, refresh_date, region,
                term, week, rank, score
            )
            SELECT
                id,
                refresh_date,
                trim(region),
                lower(trim(term)),
                week,
                rank,
                score
            FROM bronze.raw_trends
            WHERE rank BETWEEN 1 AND 25
            ON CONFLICT (id) DO UPDATE SET
                refresh_date=EXCLUDED.refresh_date,
                region=EXCLUDED.region,
                term=EXCLUDED.term,
                week=EXCLUDED.week,
                rank=EXCLUDED.rank,
                score=EXCLUDED.score
            """
        )

        logger.info(
            "Silver data loaded"
        )

        await conn.execute(
            "TRUNCATE gold.trending_terms"
        )

        await conn.execute(
            """
            INSERT INTO gold.trending_terms
            SELECT
                term,
                count(DISTINCT region),
                round(avg(rank)::numeric, 2),
                min(rank)
            FROM silver.trends_clean
            GROUP BY 1
            ORDER BY 2 DESC
            LIMIT 25
            """
        )

        await conn.execute(
            "TRUNCATE gold.region_leaders"
        )

        await conn.execute(
            """
            INSERT INTO gold.region_leaders
            SELECT DISTINCT ON (region)
                region,
                term,
                score
            FROM silver.trends_clean
            WHERE rank = 1
            ORDER BY region, term
            """
        )

    logger.info(
        "Neon Bronze/Silver/Gold load completed"
    )
