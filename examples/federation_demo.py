"""Universal Decision Layer — ContextQL Federation Demo

Demonstrates MCP context providers, REMOTE data sources, and identity maps
working together over a 240-invoice dataset.

Run from the repo root with the executor extras installed:

    source .venv/bin/activate
    python examples/federation_demo.py
"""

from __future__ import annotations

import random

import pandas as pd

import contextql as cql
from contextql.providers import MCPResult, RemoteResult


# ─────────────────────────────────────────────────────────────────────────────
# Demo provider classes
# ─────────────────────────────────────────────────────────────────────────────


class FraudMCP:
    """Simulates an ML fraud-scoring model.

    Flags invoices with a synthetic 'fraud amount' above 15 000 as high-fraud.
    The pseudo-random amount for invoice i is: 250 + ((i * 137) % 24000).
    """

    def resolve(self, entity_type, params, limit=None):
        fraud_ids = [i for i in range(1, 241) if (250 + ((i * 137) % 24000)) > 15_000]
        scores = [(250 + ((i * 137) % 24000)) / 24_250.0 for i in fraud_ids]
        return MCPResult(
            entity_type=entity_type,
            entity_ids=fraud_ids,
            scores=scores,
        )


class PriorityMCP:
    """Simulates a priority-scoring model (amount-based, all invoices)."""

    def resolve(self, entity_type, params, limit=None):
        all_ids = list(range(1, 241))
        scores = [(250 + ((i * 137) % 24000)) / 24_250.0 for i in all_ids]
        return MCPResult(
            entity_type=entity_type,
            entity_ids=all_ids,
            scores=scores,
        )


class JiraRemote:
    """Simulates a Jira-like issue tracker.

    Issues 1–30 correspond to invoice IDs 1–30.
    """

    def query(self, resource, filters, columns, limit=None):
        rng = random.Random(42)
        rows = [
            {
                "issue_id": i,
                "status": rng.choice(["OPEN", "IN_PROGRESS", "CLOSED"]),
                "priority": rng.choice(["HIGH", "MEDIUM", "LOW"]),
            }
            for i in range(1, 241)
        ]
        return RemoteResult(rows=rows)


# ─────────────────────────────────────────────────────────────────────────────
# Engine setup
# ─────────────────────────────────────────────────────────────────────────────


def build_engine() -> cql.Engine:
    engine = cql.Engine()

    # 240 invoices with pseudo-random amounts
    invoice_ids = list(range(1, 241))
    amounts = [250 + ((i * 137) % 24_000) for i in invoice_ids]
    vendors = [f"V{((i * 7) % 20) + 1:02d}" for i in invoice_ids]

    invoices = pd.DataFrame({
        "invoice_id": invoice_ids,
        "vendor_id":  vendors,
        "amount":     amounts,
    })
    engine.register_table("invoices", invoices, primary_key="invoice_id")

    # Context: high-amount invoices (used as a SQL context for variety)
    engine.register_context(
        "large_invoice",
        "SELECT invoice_id FROM invoices WHERE amount > 15000",
        entity_key="invoice_id",
    )

    # MCP providers
    engine.register_mcp_provider("fraud_model", FraudMCP())
    engine.register_mcp_provider("priority_model", PriorityMCP())

    # REMOTE provider
    engine.register_remote_provider("jira", JiraRemote())

    return engine


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

DIVIDER = "─" * 68


def header(title: str) -> None:
    print(f"\n{DIVIDER}")
    print(f"  {title}")
    print(DIVIDER)


def main() -> None:
    engine = build_engine()

    # ── Section 1: MCP-only — fraud + priority scoring ────────────────────────
    header("Section 1 — MCP fraud + priority scoring  (top 10 by context score)")

    result1 = engine.execute(
        """
        SELECT invoice_id, amount, CONTEXT_SCORE() AS priority
        FROM invoices
        WHERE CONTEXT IN (MCP(fraud_model), MCP(priority_model))
        ORDER BY CONTEXT DESC
        LIMIT 10;
        """
    )
    result1.show()

    # ── Section 2: MCP + REMOTE join ──────────────────────────────────────────
    header("Section 2 — MCP fraud + REMOTE Jira join  (top 10 fraud invoices with Jira status)")

    result2 = engine.execute(
        """
        SELECT i.invoice_id, i.amount, j.status AS jira_status, j.priority AS jira_priority
        FROM invoices AS i
        JOIN REMOTE(jira.issues) AS j ON i.invoice_id = j.issue_id
        WHERE CONTEXT IN (MCP(fraud_model))
        ORDER BY CONTEXT DESC
        LIMIT 10;
        """
    )
    result2.show()

    # ── Section 3: EXPLAIN plan ───────────────────────────────────────────────
    header("Section 3 — EXPLAIN plan  (MCP fraud model)")

    plan = engine.explain(
        "SELECT invoice_id, CONTEXT_SCORE() AS s FROM invoices "
        "WHERE CONTEXT IN (MCP(fraud_model)) ORDER BY CONTEXT DESC LIMIT 10;"
    )
    print(plan)

    print(f"\n{DIVIDER}")
    print("  Demo complete.")
    print(DIVIDER)


if __name__ == "__main__":
    main()
