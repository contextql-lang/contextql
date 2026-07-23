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
import ctypes
import json
import os
import platform
import subprocess
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
ORDER BY CONTEXT DESC, transaction_id ASC
LIMIT 20;
"""


def peak_rss_mb() -> float:
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        class ProcessMemoryCounters(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
            ]

        counters = ProcessMemoryCounters()
        counters.cb = ctypes.sizeof(counters)
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        psapi.GetProcessMemoryInfo.argtypes = [
            wintypes.HANDLE,
            ctypes.POINTER(ProcessMemoryCounters),
            wintypes.DWORD,
        ]
        psapi.GetProcessMemoryInfo.restype = wintypes.BOOL
        process = kernel32.GetCurrentProcess()
        if not psapi.GetProcessMemoryInfo(
            process, ctypes.byref(counters), counters.cb
        ):
            raise OSError(
                ctypes.get_last_error(),
                "GetProcessMemoryInfo failed",
            )
        return counters.PeakWorkingSetSize / (1024.0 * 1024.0)

    import resource

    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports bytes; Linux and other supported Unix targets report KiB.
    divisor = 1024.0 * 1024.0 if sys.platform == "darwin" else 1024.0
    return rss / divisor


def available_memory_mb() -> float:
    if os.name == "nt":
        class MemoryStatusEx(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]

        status = MemoryStatusEx()
        status.dwLength = ctypes.sizeof(status)
        if not ctypes.windll.kernel32.GlobalMemoryStatusEx(
            ctypes.byref(status)
        ):
            raise OSError("GlobalMemoryStatusEx failed")
        return status.ullAvailPhys / (1024.0 * 1024.0)
    pages = os.sysconf("SC_AVPHYS_PAGES")
    page_size = os.sysconf("SC_PAGE_SIZE")
    return pages * page_size / (1024.0 * 1024.0)


def timed(fn):
    start = time.perf_counter()
    result = fn()
    return result, time.perf_counter() - start


def run(rows: int, db_path: str) -> dict:
    import duckdb
    import pandas
    import pyroaring

    try:
        git_sha = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], text=True
        ).strip()
        git_dirty = bool(
            subprocess.check_output(
                ["git", "status", "--porcelain"], text=True
            ).strip()
        )
    except Exception:
        git_sha = "unknown"
        git_dirty = None
    report: dict = {
        "environment": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "cpu_count": os.cpu_count(),
            "cpu": (
                platform.processor()
                or os.environ.get("PROCESSOR_IDENTIFIER", "unknown")
            ),
            "available_memory_mb": round(available_memory_mb(), 1),
            "git_sha": git_sha,
            "git_dirty": git_dirty,
            "duckdb": duckdb.__version__,
            "pandas": pandas.__version__,
            "pyroaring": getattr(pyroaring, "__version__", "unknown"),
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
            "storage_kind": snapshot.storage_kind,
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

    # SQL cross-check every algebra result.
    algebra_sql = {
        "union_3way": (
            f"({reference_context_sql('unmatched_trade', as_of=AS_OF)}) "
            "UNION "
            f"({reference_context_sql('missing_confirmation', as_of=AS_OF)}) "
            "UNION "
            f"({reference_context_sql('invalid_ssi', as_of=AS_OF)})"
        ),
        "intersect_2way": (
            f"({reference_context_sql('approaching_market_cutoff', as_of=AS_OF)}) "
            "INTERSECT "
            f"({reference_context_sql('high_notional', as_of=AS_OF)})"
        ),
        "difference": (
            f"({reference_context_sql('unmatched_trade', as_of=AS_OF)}) "
            "EXCEPT "
            f"({reference_context_sql('missing_confirmation', as_of=AS_OF)})"
        ),
    }
    for name, sql in algebra_sql.items():
        reference_count = conn.execute(
            f"SELECT COUNT(*) FROM ({sql}) AS _reference"
        ).fetchone()[0]
        algebra[name]["sql_reference_count"] = reference_count
        algebra[name]["correctness_ok"] = (
            reference_count == algebra[name]["cardinality"]
        )
        if not algebra[name]["correctness_ok"]:
            raise SystemExit(
                f"CORRECTNESS FAILURE: {name} bitmap="
                f"{algebra[name]['cardinality']} sql={reference_count}"
            )

    # Warm top-20 query (run twice; report the warm run)
    engine.execute(TOP20_QUERY)
    result, elapsed = timed(lambda: engine.execute(TOP20_QUERY))
    frame = result.to_pandas()
    reference_top20 = conn.execute(
        f"""
        SELECT transaction_id,
               LEAST(
                   1.0,
                   predicted_fail_probability
                   + CASE WHEN ssi_valid = FALSE THEN 0.20 ELSE 0 END
                   + CASE WHEN match_status <> 'matched' THEN 0.15 ELSE 0 END
               ) AS intervention_priority
        FROM transactions
        WHERE transaction_id IN (
            {reference_context_sql(
                'settlement_intervention_required', as_of=AS_OF
            )}
        )
        ORDER BY intervention_priority DESC, transaction_id ASC
        LIMIT 20
        """
    ).df()
    actual_pairs = [
        (int(row.transaction_id), float(row.intervention_priority))
        for row in frame.itertuples()
    ]
    reference_pairs = [
        (int(row.transaction_id), float(row.intervention_priority))
        for row in reference_top20.itertuples()
    ]
    top20_matches = actual_pairs == reference_pairs
    if not top20_matches:
        raise SystemExit(
            "CORRECTNESS FAILURE: top-20 IDs/scores differ from SQL. "
            f"actual={actual_pairs!r} reference={reference_pairs!r}"
        )
    report["top20_query"] = {
        "warm_seconds": round(elapsed, 3),
        "rows_returned": len(frame),
        "used_snapshot_pushdown": "__cql_members_0" in result.sql,
        "max_priority": float(frame["intervention_priority"].max()),
        "exact_sql_match": top20_matches,
        "transaction_ids": [
            int(value) for value in frame["transaction_id"]
        ],
        "scores": [
            float(value) for value in frame["intervention_priority"]
        ],
    }

    # Simulate a connector-supplied context and prove that native/connector
    # composition and REMOTE evidence narrowing use the same exact ID set.
    connector_ids = [
        int(row[0])
        for row in conn.execute(
            """
            SELECT transaction_id FROM transactions
            WHERE transaction_id % 997 = 0
            ORDER BY transaction_id
            """
        ).fetchall()
    ]
    engine.register_snapshot_context(
        "benchmark_connector",
        entity_key="transaction_id",
        entity_key_type="INT64",
    )
    now = datetime.now(timezone.utc)
    store.put_snapshot(
        "benchmark_connector",
        connector_ids,
        computed_at=now,
        data_as_of=now,
        storage_kind="roaring",
    )
    composed = store.compose(
        union_of=[
            "settlement_intervention_required",
            "benchmark_connector",
        ]
    )
    native_reference = {
        int(row[0])
        for row in conn.execute(
            reference_context_sql(
                "settlement_intervention_required", as_of=AS_OF
            )
        ).fetchall()
    }
    composed_reference = native_reference | set(connector_ids)
    if set(composed) != composed_reference:
        raise SystemExit(
            "CORRECTNESS FAILURE: native plus connector composition differs "
            "from SQL/set reference."
        )

    from contextql.providers import RemoteResult

    class BenchmarkEvidenceProvider:
        def __init__(self):
            self.requested = None
            self.columns = None

        def query(
            self,
            resource,
            filters,
            columns,
            limit=None,
            *,
            entity_filter=None,
        ):
            self.requested = set(int(value) for value in entity_filter.ids())
            self.columns = set(columns)
            return RemoteResult(
                rows=[
                    {
                        "remote_transaction_id": entity_id,
                        "evidence_code": f"E-{entity_id}",
                    }
                    for entity_id in sorted(self.requested)
                ]
            )

    evidence_provider = BenchmarkEvidenceProvider()
    engine.register_remote_provider("benchmark", evidence_provider)
    evidence = engine.execute(
        """
        SELECT t.transaction_id, e.evidence_code
        FROM transactions AS t
        JOIN REMOTE(benchmark.evidence) AS e
          ON t.transaction_id = e.remote_transaction_id
        WHERE CONTEXT IN (
            settlement_intervention_required,
            benchmark_connector
        );
        """
    )
    evidence_ids = set(
        int(value) for value in evidence.to_pandas()["transaction_id"]
    )
    if evidence_provider.requested != composed_reference:
        raise SystemExit(
            "CORRECTNESS FAILURE: REMOTE request did not receive the exact "
            "composed membership."
        )
    if evidence_ids != composed_reference:
        raise SystemExit(
            "CORRECTNESS FAILURE: REMOTE result differs from composed "
            "reference membership."
        )
    remote_calls = [
        call for call in evidence.trace.provider_calls
        if call.provider_type == "REMOTE"
    ]
    report["connector_remote"] = {
        "connector_cardinality": len(connector_ids),
        "composed_cardinality": len(composed),
        "reference_count": len(composed_reference),
        "exact_reference_match": evidence_ids == composed_reference,
        "requested_cardinality": len(evidence_provider.requested),
        "trace_requested_cardinality": (
            remote_calls[0].requested_entity_count
            if remote_calls else None
        ),
        "filter_encoding": (
            "roaring64"
            if len(composed_reference) > 10_000 else "entity_ids"
        ),
        "requested_columns": sorted(evidence_provider.columns),
    }

    report["peak_rss_mb"] = round(peak_rss_mb(), 1)
    report["max_refresh_batch_rows"] = (
        engine._executor.ddl.last_refresh_max_batch_size
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=10_000_000)
    parser.add_argument(
        "--db", default="post_trade_benchmark.duckdb",
        help="DuckDB database file path (recreated on each run)",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional JSON report path",
    )
    args = parser.parse_args()

    report = run(args.rows, args.db)
    output = json.dumps(report, indent=2)
    print(output)
    report_path = (
        Path(args.output)
        if args.output else Path(args.db).with_suffix(".report.json")
    )
    report_path.write_text(output)
    print(f"\nreport written to {report_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
