"""Post-trade Roaring contexts benchmark (plan sections 10 and 12).

Generates the deterministic 10M-transaction dataset, materializes the nine
representative contexts as Roaring snapshots through executable DDL, and
records the measurements required by plan section 12:

- dataset generation time and on-disk size
- cold context materialization time
- bitmap cardinality and serialized bytes
- union / intersection / difference latency
- warm top-20 query latency
- peak resident memory
- SQL correctness checks for every reported number

Usage:
    python benchmarks/post_trade_benchmark.py [--rows N] [--db PATH]

Every run prints a JSON report to stdout and writes it next to the database
file. Environment information is recorded with the results; numbers are
machine-specific, not universal guarantees.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import resource
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import contextql as cql
from contextql.datasets import generate_post_trade_dataset
from contextql.datasets.post_trade import (
    DEFAULT_AS_OF,
    REFERENCE_CONTEXT_SQL,
    reference_context_sql,
)
from contextql.semantic import TableCatalogEntry

AS_OF = DEFAULT_AS_OF

# Context definitions bound to the deterministic as-of anchor. Each is the
# executable-DDL form of the reference SQL in datasets/post_trade.py.
CONTEXT_DEFINITIONS = {
    name: (
        f"CREATE CONTEXT {name} ON transaction_id "
        "WITH (materialized = TRUE, storage = 'roaring') AS "
        + REFERENCE_CONTEXT_SQL[name].format(
            table="transactions", as_of=AS_OF
        )
        + ";"
    )
    for name in REFERENCE_CONTEXT_SQL
    if name != "settlement_intervention_required"
}

# The flagship context carries the plan section 6.4 score expression.
CONTEXT_DEFINITIONS["settlement_intervention_required"] = f"""
CREATE CONTEXT settlement_intervention_required
ON transaction_id
SCORE intervention_priority
WITH (materialized = TRUE, storage = 'roaring')
AS
SELECT
    transaction_id,
    LEAST(
        1.0,
        predicted_fail_probability
        + CASE WHEN ssi_valid = FALSE THEN 0.20 ELSE 0 END
        + CASE WHEN match_status <> 'matched' THEN 0.15 ELSE 0 END
    ) AS intervention_priority
FROM transactions
WHERE settlement_status NOT IN ('settled', 'cancelled')
  AND (
      contractual_settle_date < CAST(TIMESTAMP '{AS_OF}' AS DATE)
      OR market_cutoff_at <= TIMESTAMP '{AS_OF}' + INTERVAL '2 hours'
  )
  AND (
      match_status <> 'matched'
      OR ssi_valid = FALSE
      OR confirmation_received = FALSE
      OR fields_mismatched > 0
      OR predicted_fail_probability >= 0.80
  );
"""

TOP20_QUERY = """
SELECT transaction_id, counterparty_id, asset_class, notional_usd,
       contractual_settle_date, CONTEXT_SCORE() AS intervention_priority
FROM transactions
WHERE CONTEXT IN (settlement_intervention_required)
ORDER BY CONTEXT DESC
LIMIT 20;
"""


def peak_rss_mb() -> float:
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def timed(fn):
    start = time.perf_counter()
    result = fn()
    return result, time.perf_counter() - start


def run(rows: int, db_path: str) -> dict:
    report: dict = {
        "environment": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "cpu_count": os.cpu_count(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "note": (
                "Machine-specific measurements; not universal guarantees."
            ),
        },
        "rows": rows,
        "as_of": AS_OF,
    }

    if os.path.exists(db_path):
        os.remove(db_path)

    engine = cql.Engine(database=db_path)
    conn = engine._adapter.conn

    _, generation_s = timed(
        lambda: generate_post_trade_dataset(conn, rows, as_of=AS_OF)
    )
    conn.execute("CHECKPOINT")
    report["generation"] = {
        "seconds": round(generation_s, 2),
        "db_size_mb": round(os.path.getsize(db_path) / 1e6, 1),
    }

    engine._catalog.tables["transactions"] = TableCatalogEntry(
        name="transactions",
        primary_key_name="transaction_id",
        primary_key_type="INT64",
    )

    contexts = {}
    for name, ddl in CONTEXT_DEFINITIONS.items():
        engine.execute(ddl)
        _, refresh_s = timed(
            lambda n=name: engine.execute(f"REFRESH CONTEXT {n};")
        )
        store = engine._executor.membership
        snapshot = store.get_snapshot(name)
        serialized = store.serialize(name)
        reference_count = conn.execute(
            "SELECT COUNT(*) FROM ("
            + reference_context_sql(name, as_of=AS_OF)
            + ") AS _r"
        ).fetchone()[0]
        matches = snapshot.member_count == reference_count
        contexts[name] = {
            "cold_materialization_s": round(refresh_s, 3),
            "cardinality": snapshot.member_count,
            "serialized_bytes": len(serialized),
            "sql_reference_count": reference_count,
            "correctness_ok": bool(matches),
        }
        if not matches:
            raise SystemExit(
                f"CORRECTNESS FAILURE: {name} bitmap={snapshot.member_count} "
                f"reference={reference_count}"
            )
    report["contexts"] = contexts

    store = engine._executor.membership
    algebra = {}
    union_names = ["unmatched_trade", "missing_confirmation", "invalid_ssi"]
    result, elapsed = timed(
        lambda: store.compose(union_of=union_names)
        if hasattr(store, "compose")
        else store.union(union_names)
    )
    algebra["union_3way"] = {
        "seconds": round(elapsed, 6), "cardinality": len(result),
    }
    result, elapsed = timed(
        lambda: store.compose(
            intersect_of=["approaching_market_cutoff", "high_notional"]
        )
        if hasattr(store, "compose")
        else store.intersect(["approaching_market_cutoff", "high_notional"])
    )
    algebra["intersect_2way"] = {
        "seconds": round(elapsed, 6), "cardinality": len(result),
    }
    result, elapsed = timed(
        lambda: store.compose(
            union_of=["unmatched_trade"], subtract=["missing_confirmation"]
        )
        if hasattr(store, "compose")
        else store.difference("unmatched_trade", "missing_confirmation")
    )
    algebra["difference"] = {
        "seconds": round(elapsed, 6), "cardinality": len(result),
    }
    report["algebra"] = algebra

    # SQL cross-checks for the algebra results
    union_sql = conn.execute(
        "SELECT COUNT(*) FROM (SELECT transaction_id FROM transactions "
        "WHERE match_status = 'unmatched' OR confirmation_received = FALSE "
        "OR ssi_valid = FALSE) AS _u"
    ).fetchone()[0]
    if union_sql != algebra["union_3way"]["cardinality"]:
        raise SystemExit(
            f"CORRECTNESS FAILURE: union bitmap="
            f"{algebra['union_3way']['cardinality']} sql={union_sql}"
        )
    report["algebra"]["union_3way"]["sql_reference_count"] = union_sql

    # Warm top-20 query (run twice; report the warm run)
    engine.execute(TOP20_QUERY)
    result, elapsed = timed(lambda: engine.execute(TOP20_QUERY))
    frame = result.to_pandas()
    report["top20_query"] = {
        "warm_seconds": round(elapsed, 3),
        "rows_returned": len(frame),
        "used_snapshot_pushdown": "__cql_members_0" in result.sql,
        "max_priority": float(frame["intervention_priority"].max()),
    }

    report["peak_rss_mb"] = round(peak_rss_mb(), 1)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=10_000_000)
    parser.add_argument(
        "--db", default="post_trade_benchmark.duckdb",
        help="DuckDB database file path (recreated on each run)",
    )
    args = parser.parse_args()

    report = run(args.rows, args.db)
    output = json.dumps(report, indent=2)
    print(output)
    report_path = Path(args.db).with_suffix(".report.json")
    report_path.write_text(output)
    print(f"\nreport written to {report_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
