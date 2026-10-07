"""Real data: Google Trends public dataset in BigQuery (bigquery-public-data.google_trends.top_terms).
Top-25 Google Search terms per US region (DMA), refreshed daily by Google. Free tier: 1 TB queries/month."""
from __future__ import annotations
import json, os, random
from datetime import date

EXPECTED = ["id", "refresh_date", "region", "region_id", "term", "week", "rank", "score"]
RANK_MIN, RANK_MAX = 1, 25

ALLOWED_ACTION = {  # automatic fixes, one per error type
    "schema_mismatch": "rename_columns",
    "null_threshold_exceeded": "quarantine_null_rows",
    "data_type_mismatch": "cast_types",
    "duplicate_records": "dedupe_rows",
    "invalid_range": "quarantine_out_of_range",
    "row_count_anomaly": "reload_last_good_batch",
}
MANUAL_ACTIONS = {  # what a human can pick after rejecting the AI's fix
    "reload_last_good_batch": "Reload last known-good batch",
    "accept_partial_batch": "Accept the partial batch as-is",
    "skip_and_keep_previous": "Skip this run, keep previous data",
}
# how much of the batch Agent 1 may corrupt, per error type (code clamps the AI's choice)
FRACTION_BOUNDS = {"null_threshold_exceeded": (0.15, 0.40), "data_type_mismatch": (0.02, 0.08),
                   "duplicate_records": (0.10, 0.30), "invalid_range": (0.05, 0.20),
                   "row_count_anomaly": (0.05, 0.30), "schema_mismatch": (1.0, 1.0)}

SQL = """
SELECT CAST(refresh_date AS STRING) AS refresh_date, CAST(dma_id AS STRING) AS region_id, dma_name AS region,
       term, CAST(week AS STRING) AS week, rank, score
FROM `bigquery-public-data.google_trends.top_terms`
WHERE refresh_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 4 DAY)
"""


def fetch_batch_sync() -> list[dict]:
    """Blocking BigQuery call — run via asyncio.to_thread."""
    from google.cloud import bigquery
    from google.oauth2 import service_account
    info = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
    creds = service_account.Credentials.from_service_account_info(info)
    client = bigquery.Client(project=info["project_id"], credentials=creds)
    cfg = bigquery.QueryJobConfig(maximum_bytes_billed=2 * 1024**3)  # hard cost guard
    raw = [dict(r) for r in client.query(SQL, job_config=cfg).result()]
    if not raw:
        return []
    latest = max(r["refresh_date"] for r in raw)
    raw = [r for r in raw if r["refresh_date"] == latest]
    week = max(r["week"] for r in raw)
    return [{"id": f"{r['refresh_date']}|{r['region_id']}|{r['term']}|{r['week']}",
             "refresh_date": r["refresh_date"], "region": r["region"] or "unknown", "region_id": r["region_id"],
             "term": r["term"], "week": r["week"], "rank": int(r["rank"]),
             "score": None if r["score"] is None else float(r["score"])}
            for r in raw if r["week"] == week]


# ---------- Agent-1 side: corrupt a copy of the real batch ----------
def clamp_fraction(error_type: str, f: float) -> float:
    lo, hi = FRACTION_BOUNDS.get(error_type, (0.1, 0.3))
    return max(lo, min(hi, f))


def inject(error_type: str, rows: list[dict], rng: random.Random, fraction: float | None = None) -> list[dict]:
    rows = [dict(r) for r in rows]
    n = len(rows)
    lo, hi = FRACTION_BOUNDS.get(error_type, (0.1, 0.3))
    frac = clamp_fraction(error_type, fraction if fraction is not None else (lo + hi) / 2)
    k = max(1, int(n * frac))
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
        rows += [dict(r) for r in rng.sample(rows, k)]
    elif error_type == "invalid_range":
        for r in rng.sample(rows, k):
            r["rank"] = rng.choice([0, -3, 999])
    elif error_type == "row_count_anomaly":
        rows = rng.sample(rows, k)
    return rows


# ---------- validation (the evidence both agents work from) ----------
def validate(rows: list[dict], baseline: int) -> list[dict]:
    if not rows:
        return [{"type": "row_count_anomaly", "detail": "batch is empty"}]
    cols = set().union(*(r.keys() for r in rows))
    missing, extra = set(EXPECTED) - cols, cols - set(EXPECTED)
    if missing or extra:
        return [{"type": "schema_mismatch",
                 "detail": f"missing columns {sorted(missing)}, unexpected columns {sorted(extra)}"}]
    n = len(rows)
    strs = sum(1 for r in rows if isinstance(r["rank"], str))
    if strs:
        return [{"type": "data_type_mismatch", "detail": f"column 'rank' expected int, {strs}/{n} values are strings"}]
    nulls = sum(1 for r in rows if r["rank"] is None)
    if nulls / n > 0.10:
        return [{"type": "null_threshold_exceeded",
                 "detail": f"'rank' null ratio {nulls/n:.0%} exceeds 10% threshold ({nulls}/{n})"}]
    dups = n - len({r["id"] for r in rows})
    if dups:
        return [{"type": "duplicate_records", "detail": f"{dups} duplicate primary keys on 'id'"}]
    oor = sum(1 for r in rows if r["rank"] is not None and not (RANK_MIN <= r["rank"] <= RANK_MAX))
    if oor:
        return [{"type": "invalid_range", "detail": f"{oor} rows with 'rank' outside valid range {RANK_MIN}..{RANK_MAX}"}]
    if baseline and n < 0.5 * baseline:
        return [{"type": "row_count_anomaly",
                 "detail": f"row count {n} is {1 - n/baseline:.0%} below last good batch ({baseline})"}]
    return []


# ---------- fixes ----------
def apply_fix(action: str, rows: list[dict], last_good: list[dict]) -> tuple[list[dict], int]:
    """Returns (rows_to_load, quarantined). Whitelisted actions only."""
    if action == "rename_columns":
        out = []
        for r in rows:
            r = dict(r)
            if "rank_position" in r and "rank" not in r:
                r["rank"] = r.pop("rank_position")
            out.append({k: r.get(k) for k in EXPECTED})
        return out, 0
    if action == "quarantine_null_rows":
        keep = [r for r in rows if r["rank"] is not None]
        return keep, len(rows) - len(keep)
    if action == "cast_types":
        out = []
        for r in rows:
            r = dict(r)
            try:
                r["rank"] = int(r["rank"])
            except (TypeError, ValueError):
                r["rank"] = None
            out.append(r)
        return out, 0
    if action == "dedupe_rows":
        seen, out = set(), []
        for r in rows:
            if r["id"] not in seen:
                seen.add(r["id"]); out.append(r)
        return out, len(rows) - len(out)
    if action == "quarantine_out_of_range":
        keep = [r for r in rows if RANK_MIN <= r["rank"] <= RANK_MAX]
        return keep, len(rows) - len(keep)
    if action == "reload_last_good_batch":
        return [dict(r) for r in last_good], 0
    if action == "accept_partial_batch":
        return rows, 0
    if action == "skip_and_keep_previous":
        return [], 0
    raise ValueError(f"unknown action {action}")


# ---------- load bronze -> silver -> gold ----------
async def load(conn, rows: list[dict]) -> None:
    if not rows:
        return
    recs = [(r["id"], date.fromisoformat(r["refresh_date"]), r["region"], r["region_id"], r["term"],
             date.fromisoformat(r["week"]), r["rank"], r["score"]) for r in rows]
    async with conn.transaction():
        await conn.executemany(
            """INSERT INTO bronze.raw_trends (id,refresh_date,region,region_id,term,week,rank,score)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (id) DO NOTHING""", recs)
        await conn.execute("DELETE FROM bronze.raw_trends WHERE refresh_date < current_date - 7")
        await conn.execute("DELETE FROM silver.trends_clean WHERE refresh_date < current_date - 7")
        await conn.execute(
            """INSERT INTO silver.trends_clean (id,refresh_date,region,term,week,rank,score)
               SELECT id,refresh_date,trim(region),lower(trim(term)),week,rank,score
               FROM bronze.raw_trends WHERE rank BETWEEN 1 AND 25 ON CONFLICT (id) DO NOTHING""")
        await conn.execute("TRUNCATE gold.trending_terms")
        await conn.execute(
            """INSERT INTO gold.trending_terms
               SELECT term, count(DISTINCT region), round(avg(rank)::numeric,2), min(rank)
               FROM silver.trends_clean GROUP BY 1 ORDER BY 2 DESC LIMIT 25""")
        await conn.execute("TRUNCATE gold.region_leaders")
        await conn.execute(
            """INSERT INTO gold.region_leaders
               SELECT DISTINCT ON (region) region, term, score
               FROM silver.trends_clean WHERE rank=1 ORDER BY region, term""")
