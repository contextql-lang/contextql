"""ContextQL Killer Demo: Procurement War Room

Seven escalating scenes that showcase ContextQL's full feature set —
from basic context queries to ML-augmented, cross-entity, federated
operational intelligence over a 240-invoice procurement dataset.

Run from the repo root with executor extras installed:

    source .venv/bin/activate
    python examples/killer_demo.py
"""

from __future__ import annotations

import contextql as cql
from contextql.providers import FraudDetectionMCP, PriorityMCP, JiraRemoteProvider


# ── Helpers ────────────────────────────────────────────────────────────────────

BOLD = "\033[1m"
DIM  = "\033[2m"
RST  = "\033[0m"
BAR  = "=" * 72
THIN = "-" * 72


def banner(scene: int, title: str, subtitle: str) -> None:
    print(f"\n{BAR}")
    print(f"  {BOLD}Scene {scene}: {title}{RST}")
    print(f"  {DIM}{subtitle}{RST}")
    print(BAR)


def annotation(text: str) -> None:
    print(f"\n  {DIM}-> {text}{RST}")
    print(THIN)


# ── Engine setup ───────────────────────────────────────────────────────────────


def setup_engine() -> cql.Engine:
    """Build the demo engine with tables, contexts, providers, and identity map."""
    engine = cql.demo()

    # ── MCP providers ──────────────────────────────────────────────────────────
    engine.register_mcp_provider(
        "fraud_model",
        FraudDetectionMCP(threshold=0.6),
    )
    engine.register_mcp_provider(
        "priority_model",
        PriorityMCP(),
    )

    # ── REMOTE provider ────────────────────────────────────────────────────────
    engine.register_remote_provider(
        "jira",
        JiraRemoteProvider(seed=42),
    )

    # ── Identity map (invoices.vendor_id <-> vendors.vendor_id) ────────────────
    engine.register_identity_map(
        "vendor",
        {"invoices.vendor_id": "vendors.vendor_id"},
    )

    return engine


# ── Scenes ─────────────────────────────────────────────────────────────────────


def scene_1(engine: cql.Engine) -> None:
    """Situational Awareness — basic context union with weighted scoring."""
    banner(1, "Situational Awareness",
           "Find the most urgent open invoices with composite context scoring")

    result = engine.execute("""
        SELECT invoice_id, vendor_id, amount, status,
               CONTEXT_SCORE() AS urgency
        FROM invoices
        WHERE CONTEXT IN (open_invoice, overdue_invoice WEIGHT 1.5)
        ORDER BY CONTEXT DESC
        LIMIT 15;
    """)
    result.show()
    annotation("Combined 2 contexts (union + weight), ranked by composite score.")


def scene_2(engine: cql.Engine) -> None:
    """Risk Amplification — cross-entity context with identity map."""
    banner(2, "Risk Amplification",
           "Invoices where BOTH the invoice AND the vendor are in risk contexts")

    result = engine.execute("""
        SELECT i.invoice_id, i.amount, i.status,
               v.vendor_name, v.risk_tier,
               CONTEXT_SCORE() AS combined_risk
        FROM invoices AS i
        JOIN vendors AS v ON i.vendor_id = v.vendor_id
        WHERE CONTEXT ON i IN (overdue_invoice WEIGHT 1.0)
          AND CONTEXT ON v IN (risky_vendor WEIGHT 2.0)
        ORDER BY CONTEXT DESC
        LIMIT 10;
    """)
    result.show()
    annotation("Multi-table CONTEXT ON binding, identity map resolves vendor_id across tables.")


def scene_3(engine: cql.Engine) -> None:
    """ML Augmentation — MCP fraud provider."""
    banner(3, "ML Augmentation",
           "Bring in an external fraud-scoring model as an MCP provider")

    result = engine.execute("""
        SELECT invoice_id, vendor_id, amount,
               CONTEXT_SCORE() AS fraud_risk
        FROM invoices
        WHERE CONTEXT IN (MCP(fraud_model))
        ORDER BY CONTEXT DESC
        LIMIT 10;
    """)
    result.show()
    annotation("FraudDetectionMCP provider scored 240 invoices; top 10 shown by fraud risk.")


def scene_4(engine: cql.Engine) -> None:
    """External Enrichment — REMOTE Jira join with MCP context."""
    banner(4, "External Enrichment",
           "Join MCP fraud results with live Jira ticket data via REMOTE federation")

    result = engine.execute("""
        SELECT i.invoice_id, i.amount,
               j.status AS jira_status, j.priority AS jira_priority,
               CONTEXT_SCORE() AS risk
        FROM invoices AS i
        JOIN REMOTE(jira.issues) AS j ON i.invoice_id = j.issue_id
        WHERE CONTEXT IN (MCP(fraud_model), overdue_invoice)
        ORDER BY CONTEXT DESC
        LIMIT 10;
    """)
    result.show()
    annotation("MCP + SQL context union, enriched with REMOTE Jira data in one query.")


def scene_5(engine: cql.Engine) -> None:
    """Custom Intelligence — @context decorator + QueryBuilder."""
    banner(5, "Custom Intelligence",
           "Define a context with @context decorator, query with the fluent builder API")

    @engine.context(
        "high_value_open",
        entity_key="invoice_id",
        has_score=True,
        score_column="urgency",
    )
    def high_value_open():
        return (
            "SELECT invoice_id, amount / 24250.0 AS urgency "
            "FROM invoices "
            "WHERE status = 'open' AND amount > 5000"
        )

    result = (
        engine.query("invoices")
        .select("invoice_id", "amount", "status", "CONTEXT_SCORE() AS priority")
        .where_context("high_value_open", "open_invoice")
        .order_by_context()
        .limit(10)
        .execute()
    )
    result.show()
    annotation("@context decorator registered a scored context; QueryBuilder generated the SQL.")


def scene_6(engine: cql.Engine) -> None:
    """Statistical Outliers — GLOBAL() and ZSCORE() window macros."""
    banner(6, "Statistical Outliers",
           "Identify statistical anomalies among open invoices using window macros")

    result = engine.execute("""
        SELECT invoice_id, amount,
               GLOBAL(AVG(amount)) AS portfolio_avg,
               ZSCORE(amount) AS z_amount
        FROM invoices
        WHERE CONTEXT IN (overdue_invoice)
        ORDER BY CONTEXT DESC
        LIMIT 15;
    """)
    result.show()
    annotation("GLOBAL() and ZSCORE() macros expand to window functions at lowering time.")


def scene_7(engine: cql.Engine) -> None:
    """Executive Dashboard — full pipeline + Result API + EXPLAIN."""
    banner(7, "Executive Dashboard",
           "Full pipeline combining SQL + MCP + vendor risk, then inspect the result")

    query = """
        SELECT i.invoice_id, i.amount, i.status,
               v.vendor_name, v.risk_tier,
               CONTEXT_SCORE() AS risk_score
        FROM invoices AS i
        JOIN vendors AS v ON i.vendor_id = v.vendor_id
        WHERE CONTEXT ON i IN (overdue_invoice, MCP(fraud_model) WEIGHT 2.0)
          AND CONTEXT ON v IN (risky_vendor WEIGHT 1.5)
        ORDER BY CONTEXT DESC
        LIMIT 20;
    """
    result = engine.execute(query)
    result.show()

    # Result API showcase
    print(f"\n  Rows returned : {result.row_count}")
    print(f"  Columns       : {result.columns}")
    print(f"  Generated SQL :\n    {result.sql[:120]}...")

    # EXPLAIN plan
    print(f"\n{THIN}")
    print("  EXPLAIN plan:")
    print(THIN)
    plan = engine.explain(query)
    print(plan)
    annotation("Full pipeline: SQL + MCP + cross-entity contexts + Result API + EXPLAIN.")


# ── Main ───────────────────────────────────────────────────────────────────────


def main() -> None:
    engine = setup_engine()

    scene_1(engine)
    scene_2(engine)
    scene_3(engine)
    scene_4(engine)
    scene_5(engine)
    scene_6(engine)
    scene_7(engine)

    print(f"\n{BAR}")
    print(f"  {BOLD}Demo complete.{RST}  7 scenes, 1 language, unlimited operational intelligence.")
    print(BAR)


if __name__ == "__main__":
    main()
