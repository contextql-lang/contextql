"""ContextQL Resource-Constrained Demo

Runs 5 benchmark scenarios inside a Docker container with deliberately
low resource limits (0.5 CPU, 128 MB RAM) to demonstrate that ContextQL
delivers operational intelligence under real-world constraints.

Scenarios cover the full feature set: context algebra, cross-entity
identity maps, MCP federation, REMOTE data sources, and the combined
7-stage pipeline — all timed and memory-tracked.
"""
from __future__ import annotations

import resource
import time
from dataclasses import dataclass
from pathlib import Path

import contextql as cql
from contextql.providers import FraudDetectionMCP, JiraRemoteProvider, PriorityMCP

# ── Formatting helpers ────────────────────────────────────────────────

BOLD = "\033[1m"
DIM = "\033[2m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RED = "\033[31m"
CYAN = "\033[36m"
RST = "\033[0m"
BAR = "=" * 72
THIN = "-" * 72


@dataclass
class BenchmarkResult:
    name: str
    time_ms: float
    rows: int
    rss_kb: int
    threshold_ms: float

    @property
    def passed(self) -> bool:
        return self.time_ms <= self.threshold_ms


# ── Resource detection ────────────────────────────────────────────────


def _read_cgroup_value(path: str) -> str | None:
    try:
        return Path(path).read_text().strip()
    except (FileNotFoundError, PermissionError):
        return None


def detect_container_resources() -> dict[str, str]:
    """Read cgroup v2 limits to prove the container is constrained."""
    info: dict[str, str] = {}

    cpu_max = _read_cgroup_value("/sys/fs/cgroup/cpu.max")
    if cpu_max:
        parts = cpu_max.split()
        if len(parts) == 2 and parts[0] != "max":
            quota, period = int(parts[0]), int(parts[1])
            info["cpu_limit"] = f"{quota / period:.2f} CPUs"
        else:
            info["cpu_limit"] = "unlimited"

    mem_max = _read_cgroup_value("/sys/fs/cgroup/memory.max")
    if mem_max and mem_max != "max":
        mb = int(mem_max) / (1024 * 1024)
        info["memory_limit"] = f"{mb:.0f} MB"
    else:
        info["memory_limit"] = "unlimited"

    return info


def current_rss_kb() -> int:
    """Peak RSS in KB via getrusage."""
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss


# ── Engine setup ──────────────────────────────────────────────────────


def setup_engine() -> cql.Engine:
    engine = cql.demo()

    engine.register_mcp_provider(
        "fraud_model",
        FraudDetectionMCP(threshold=0.6),
    )
    engine.register_mcp_provider(
        "priority_model",
        PriorityMCP(),
    )
    engine.register_remote_provider(
        "jira",
        JiraRemoteProvider(seed=42),
    )
    engine.register_identity_map(
        "vendor",
        {"invoices.vendor_id": "vendors.vendor_id"},
    )
    return engine


# ── Scenarios ─────────────────────────────────────────────────────────


def run_scenario(
    engine: cql.Engine,
    name: str,
    query: str,
    threshold_ms: float,
    description: str,
    traditional_lines: int,
) -> BenchmarkResult:
    cql_lines = len([l for l in query.strip().splitlines() if l.strip()])

    print(f"\n{THIN}")
    print(f"  {BOLD}{name}{RST}")
    print(f"  {DIM}{description}{RST}")
    print(f"  {DIM}CQL: {cql_lines} lines  |  Traditional SQL equivalent: ~{traditional_lines} lines{RST}")
    print(THIN)
    print(f"{DIM}{query.strip()}{RST}")

    t0 = time.perf_counter()
    result = engine.execute(query)
    elapsed_ms = (time.perf_counter() - t0) * 1000
    rss = current_rss_kb()

    result.show(max_rows=8)

    status = f"{GREEN}PASS{RST}" if elapsed_ms <= threshold_ms else f"{YELLOW}SLOW{RST}"
    print(f"\n  {status}  {elapsed_ms:7.1f} ms  |  {result.row_count} rows  |  RSS {rss // 1024} MB")

    return BenchmarkResult(
        name=name,
        time_ms=elapsed_ms,
        rows=result.row_count,
        rss_kb=rss,
        threshold_ms=threshold_ms,
    )


def scenario_1(engine: cql.Engine) -> BenchmarkResult:
    return run_scenario(
        engine,
        "1. Context Algebra — Weighted Union",
        """
        SELECT invoice_id, vendor_id, amount, status,
               CONTEXT_SCORE() AS urgency
        FROM invoices
        WHERE CONTEXT IN (
            open_invoice,
            overdue_invoice WEIGHT 1.5,
            disputed_invoice WEIGHT 0.8
        )
        ORDER BY CONTEXT DESC
        LIMIT 15;
        """,
        threshold_ms=1000,
        description="Compose 3 contexts with weighted scoring in a single query",
        traditional_lines=35,
    )


def scenario_2(engine: cql.Engine) -> BenchmarkResult:
    return run_scenario(
        engine,
        "2. Cross-Entity Context — Identity Map",
        """
        SELECT i.invoice_id, i.amount, i.status,
               v.vendor_name, v.risk_tier,
               CONTEXT_SCORE() AS combined_risk
        FROM invoices AS i
        JOIN vendors AS v ON i.vendor_id = v.vendor_id
        WHERE CONTEXT ON i IN (overdue_invoice WEIGHT 1.0)
          AND CONTEXT ON v IN (risky_vendor WEIGHT 2.0)
        ORDER BY CONTEXT DESC
        LIMIT 10;
        """,
        threshold_ms=1000,
        description="Bridge invoice and vendor contexts via identity map + JOIN",
        traditional_lines=45,
    )


def scenario_3(engine: cql.Engine) -> BenchmarkResult:
    return run_scenario(
        engine,
        "3. MCP Federation — Fraud + Priority Models",
        """
        SELECT invoice_id, vendor_id, amount,
               CONTEXT_SCORE() AS composite_risk
        FROM invoices
        WHERE CONTEXT IN (
            MCP(fraud_model),
            MCP(priority_model) WEIGHT 0.5
        )
        ORDER BY CONTEXT DESC
        LIMIT 10;
        """,
        threshold_ms=1000,
        description="Call 2 external ML models (MCP providers) and combine scores",
        traditional_lines=50,
    )


def scenario_4(engine: cql.Engine) -> BenchmarkResult:
    return run_scenario(
        engine,
        "4. REMOTE Join — Jira Issue Enrichment",
        """
        SELECT i.invoice_id, i.amount,
               j.status AS jira_status, j.priority AS jira_priority,
               CONTEXT_SCORE() AS risk
        FROM invoices AS i
        JOIN REMOTE(jira.issues) AS j ON i.invoice_id = j.issue_id
        WHERE CONTEXT IN (MCP(fraud_model), overdue_invoice)
        ORDER BY CONTEXT DESC
        LIMIT 10;
        """,
        threshold_ms=1000,
        description="Materialize REMOTE Jira data + MCP fraud context in one query",
        traditional_lines=60,
    )


def scenario_5(engine: cql.Engine) -> BenchmarkResult:
    return run_scenario(
        engine,
        "5. Full Pipeline — Combined Intelligence",
        """
        SELECT i.invoice_id, i.amount, i.status,
               v.vendor_name, v.risk_tier,
               j.status AS jira_status,
               CONTEXT_SCORE() AS risk_score
        FROM invoices AS i
        JOIN vendors AS v ON i.vendor_id = v.vendor_id
        JOIN REMOTE(jira.issues) AS j ON i.invoice_id = j.issue_id
        WHERE CONTEXT ON i IN (
            overdue_invoice,
            MCP(fraud_model) WEIGHT 2.0
        )
          AND CONTEXT ON v IN (risky_vendor WEIGHT 1.5)
        ORDER BY CONTEXT DESC
        LIMIT 20;
        """,
        threshold_ms=1500,
        description="MCP + REMOTE + identity map + 3 contexts + ORDER BY CONTEXT",
        traditional_lines=80,
    )


# ── Main ──────────────────────────────────────────────────────────────


def main() -> None:
    print(f"\n{BAR}")
    print(f"  {BOLD}ContextQL — Resource-Constrained Benchmark{RST}")
    print(BAR)

    # Detect and display container limits
    resources = detect_container_resources()
    if resources:
        print(f"\n  {CYAN}Container Resources:{RST}")
        for key, value in resources.items():
            label = key.replace("_", " ").title()
            print(f"    {label}: {BOLD}{value}{RST}")
    else:
        print(f"\n  {DIM}(Not running in a cgroup-limited container){RST}")

    # Setup
    print(f"\n  {DIM}Initializing engine (6 tables, ~900 rows, 9 contexts)...{RST}")
    t_setup = time.perf_counter()
    engine = setup_engine()
    setup_ms = (time.perf_counter() - t_setup) * 1000
    print(f"  Engine ready in {setup_ms:.0f} ms  |  RSS {current_rss_kb() // 1024} MB")

    # Run scenarios
    results = [
        scenario_1(engine),
        scenario_2(engine),
        scenario_3(engine),
        scenario_4(engine),
        scenario_5(engine),
    ]

    # Summary table
    print(f"\n{BAR}")
    print(f"  {BOLD}Results Summary{RST}")
    print(BAR)
    print(f"  {'Scenario':<45} {'Time':>8} {'Rows':>6} {'RSS':>6} {'Status':>8}")
    print(f"  {THIN}")

    total_ms = 0.0
    peak_rss = 0
    all_passed = True
    for r in results:
        status = f"{GREEN}PASS{RST}" if r.passed else f"{YELLOW}SLOW{RST}"
        if not r.passed:
            all_passed = False
        total_ms += r.time_ms
        peak_rss = max(peak_rss, r.rss_kb)
        print(f"  {r.name:<45} {r.time_ms:>7.1f}ms {r.rows:>5} {r.rss_kb // 1024:>4} MB   {status}")

    print(f"  {THIN}")
    print(f"  {'Total':.<45} {total_ms:>7.1f}ms {'':>5} {peak_rss // 1024:>4} MB")

    # Closing
    mem_limit = resources.get("memory_limit", "N/A")
    cpu_limit = resources.get("cpu_limit", "N/A")

    print(f"\n{BAR}")
    if all_passed:
        print(f"  {GREEN}{BOLD}All 5 scenarios completed successfully.{RST}")
    else:
        print(f"  {YELLOW}{BOLD}All scenarios completed (some exceeded time threshold).{RST}")
    print(f"  Total query time: {BOLD}{total_ms:.0f} ms{RST}")
    print(f"  Peak memory: {BOLD}{peak_rss // 1024} MB{RST} / {mem_limit}")
    print(f"  CPU limit: {cpu_limit}")
    print(f"\n  5 queries. 3 federated providers. {engine.catalog.tables().__len__()} tables.")
    print(f"  All inside {cpu_limit} CPU and {mem_limit} RAM.")
    print(BAR)


if __name__ == "__main__":
    main()
