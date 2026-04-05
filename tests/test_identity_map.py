"""Tests for Engine.register_identity_map and cross-entity context resolution."""

from __future__ import annotations

import pandas as pd
import pytest

import contextql as cql
from contextql.providers import MCPResult


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture()
def cross_engine():
    """Engine with invoices + vendors where vendor FK resolves via identity map."""
    e = cql.Engine()

    invoices = pd.DataFrame({
        "invoice_id": [1, 2, 3, 4, 5],
        "vendor_id":  [10, 20, 10, 30, 20],
        "amount":     [100, 500, 200, 800, 50],
    })
    vendors = pd.DataFrame({
        "vendor_id":  [10, 20, 30],
        "risk_tier":  ["high", "low", "critical"],
    })

    e.register_table("invoices", invoices, primary_key="invoice_id")
    e.register_table("vendors",  vendors,  primary_key="vendor_id")

    # risky_vendor is keyed on vendor_id
    e.register_context(
        "risky_vendor",
        "SELECT vendor_id FROM vendors WHERE risk_tier IN ('high', 'critical')",
        entity_key="vendor_id",
    )
    return e


@pytest.fixture()
def renamed_engine():
    """Engine where the FK column has a different name than the context entity key."""
    e = cql.Engine()

    invoices = pd.DataFrame({
        "invoice_id":  [1, 2, 3],
        "supplier_id": [10, 20, 10],   # different name from context key
        "amount":      [100, 500, 200],
    })
    vendors = pd.DataFrame({
        "vendor_id": [10, 20],
        "risk_tier": ["high", "low"],
    })

    e.register_table("invoices", invoices, primary_key="invoice_id")
    e.register_table("vendors",  vendors,  primary_key="vendor_id")

    e.register_context(
        "risky_vendor",
        "SELECT vendor_id FROM vendors WHERE risk_tier = 'high'",
        entity_key="vendor_id",
    )
    return e


# ─────────────────────────────────────────────────────────────────────────────
# Section 1: Storage and API
# ─────────────────────────────────────────────────────────────────────────────


class TestIdentityMapRegistration:
    def test_register_stores_mapping(self, cross_engine):
        cross_engine.register_identity_map(
            "vendor", {"invoices.vendor_id": "vendors.vendor_id"}
        )
        assert "vendor" in cross_engine._identity_maps
        assert cross_engine._identity_maps["vendor"] == {
            "invoices.vendor_id": "vendors.vendor_id"
        }

    def test_multiple_maps_stored_independently(self, cross_engine):
        cross_engine.register_identity_map("a", {"t1.x": "t2.x"})
        cross_engine.register_identity_map("b", {"t1.y": "t2.y"})
        assert "a" in cross_engine._identity_maps
        assert "b" in cross_engine._identity_maps

    def test_map_passed_to_executor(self, cross_engine):
        cross_engine.register_identity_map(
            "vendor", {"invoices.vendor_id": "vendors.vendor_id"}
        )
        assert cross_engine._identity_maps is cross_engine._executor._identity_maps


# ─────────────────────────────────────────────────────────────────────────────
# Section 2: Cross-entity context resolution via shared FK column
# ─────────────────────────────────────────────────────────────────────────────


class TestCrossEntityResolution:
    def test_context_on_invoice_with_vendor_key(self, cross_engine):
        """risky_vendor keyed on vendor_id resolves against invoices via FK."""
        # invoices has vendor_id as FK — no identity map needed when
        # _collect_extra_key_cols adds it to the projection automatically
        result = cross_engine.execute(
            "SELECT invoice_id FROM invoices WHERE CONTEXT IN (risky_vendor);"
        )
        ids = sorted(result.to_pandas()["invoice_id"].tolist())
        # vendor_id 10 (high) and 30 (critical) map to invoices 1, 3, 4
        assert ids == [1, 3, 4]

    def test_identity_map_enables_renamed_fk(self, renamed_engine):
        """supplier_id in invoices maps to vendor_id in vendors via identity map."""
        renamed_engine.register_identity_map(
            "vendor",
            {"invoices.supplier_id": "vendors.vendor_id"},
        )
        result = renamed_engine.execute(
            "SELECT invoice_id FROM invoices WHERE CONTEXT IN (risky_vendor);"
        )
        ids = sorted(result.to_pandas()["invoice_id"].tolist())
        # vendor_id 10 (high) → supplier_id 10 → invoices 1, 3
        assert ids == [1, 3]

    def test_no_identity_map_renamed_fk_raises(self, renamed_engine):
        """Without identity map, mismatched key raises ValueError with helpful message."""
        with pytest.raises(ValueError, match="register_identity_map"):
            renamed_engine.execute(
                "SELECT invoice_id FROM invoices WHERE CONTEXT IN (risky_vendor);"
            )

    def test_identity_map_bidirectional(self, renamed_engine):
        """Map written as A→B also resolves B→A lookups."""
        # Register map in the opposite direction
        renamed_engine.register_identity_map(
            "vendor",
            {"vendors.vendor_id": "invoices.supplier_id"},  # reversed
        )
        result = renamed_engine.execute(
            "SELECT invoice_id FROM invoices WHERE CONTEXT IN (risky_vendor);"
        )
        ids = sorted(result.to_pandas()["invoice_id"].tolist())
        assert ids == [1, 3]


# ─────────────────────────────────────────────────────────────────────────────
# Section 3: Identity map with MCP providers
# ─────────────────────────────────────────────────────────────────────────────


class TestIdentityMapWithMCP:
    def test_mcp_on_renamed_fk_resolves_via_map(self, renamed_engine):
        """MCP provider with entity_key resolves vendor IDs via identity map."""

        class RiskyVendorMCP:
            def resolve(self, entity_type, params, limit=None):
                return MCPResult(entity_type=entity_type, entity_ids=[10])

        renamed_engine.register_mcp_provider(
            "risky_v", RiskyVendorMCP(), entity_key="vendor_id"
        )
        renamed_engine.register_identity_map(
            "vendor",
            {"invoices.supplier_id": "vendors.vendor_id"},
        )
        result = renamed_engine.execute(
            "SELECT invoice_id FROM invoices WHERE CONTEXT IN (MCP(risky_v));"
        )
        # entity_key="vendor_id" → identity map → supplier_id column
        # vendor_id 10 → supplier_id 10 → invoices 1, 3
        ids = sorted(result.to_pandas()["invoice_id"].tolist())
        assert ids == [1, 3]

    def test_mcp_without_entity_key_uses_catalog_pk(self, renamed_engine):
        """Without entity_key, MCP falls back to catalog PK (invoice_id)."""

        class RiskyVendorMCP:
            def resolve(self, entity_type, params, limit=None):
                return MCPResult(entity_type=entity_type, entity_ids=[10])

        renamed_engine.register_mcp_provider("risky_v", RiskyVendorMCP())
        renamed_engine.register_identity_map(
            "vendor",
            {"invoices.supplier_id": "vendors.vendor_id"},
        )
        result = renamed_engine.execute(
            "SELECT invoice_id FROM invoices WHERE CONTEXT IN (MCP(risky_v));"
        )
        # No entity_key → uses catalog PK (invoice_id).
        # entity_id 10 is not in invoice_id [1,2,3] → 0 rows.
        assert result.row_count == 0

    def test_mcp_with_direct_pk_match_still_works(self, renamed_engine):
        """MCP returning actual PKs works without entity_key or identity map."""

        class InvoiceMCP:
            def resolve(self, entity_type, params, limit=None):
                return MCPResult(entity_type=entity_type, entity_ids=[1, 3])

        renamed_engine.register_mcp_provider("inv_mcp", InvoiceMCP())
        result = renamed_engine.execute(
            "SELECT invoice_id FROM invoices WHERE CONTEXT IN (MCP(inv_mcp));"
        )
        ids = sorted(result.to_pandas()["invoice_id"].tolist())
        assert ids == [1, 3]

    def test_mcp_scoring_with_identity_map(self, renamed_engine):
        """MCP scoring uses identity-mapped column for score lookups."""

        class ScoredVendorMCP:
            def resolve(self, entity_type, params, limit=None):
                return MCPResult(
                    entity_type=entity_type,
                    entity_ids=[10, 20],
                    scores=[0.9, 0.3],
                )

        renamed_engine.register_mcp_provider(
            "scored_v", ScoredVendorMCP(), entity_key="vendor_id"
        )
        renamed_engine.register_identity_map(
            "vendor",
            {"invoices.supplier_id": "vendors.vendor_id"},
        )
        result = renamed_engine.execute(
            "SELECT invoice_id, CONTEXT_SCORE() AS s FROM invoices "
            "WHERE CONTEXT IN (MCP(scored_v)) ORDER BY CONTEXT DESC;"
        )
        df = result.to_pandas()
        # supplier_id 10 → invoices 1, 3 (score 0.9)
        # supplier_id 20 → invoice 2 (score 0.3)
        assert len(df) == 3
        assert df.iloc[0]["s"] == pytest.approx(0.9)


class TestMCPEntityKeyNoSilentFallback:
    """Verify that MCP never silently falls back to df.columns[0]."""

    def test_no_pk_no_context_raises(self):
        """Without catalog PK or registered contexts, MCP raises ValueError."""
        e = cql.Engine()
        df = pd.DataFrame({"col_a": [1, 2, 3], "col_b": [10, 20, 30]})
        e.register_table("data", df)  # no primary_key

        class SimpleMCP:
            def resolve(self, entity_type, params, limit=None):
                return MCPResult(entity_type=entity_type, entity_ids=[1])

        e.register_mcp_provider("test_mcp", SimpleMCP())
        with pytest.raises(ValueError, match="Cannot determine entity key column"):
            e.execute(
                "SELECT col_a FROM data WHERE CONTEXT IN (MCP(test_mcp));"
            )

    def test_error_message_mentions_identity_map(self):
        """The error message guides the user to register_identity_map."""
        e = cql.Engine()
        df = pd.DataFrame({"x": [1], "y": [2]})
        e.register_table("t", df)

        class SimpleMCP:
            def resolve(self, entity_type, params, limit=None):
                return MCPResult(entity_type=entity_type, entity_ids=[1])

        e.register_mcp_provider("m", SimpleMCP())
        with pytest.raises(ValueError, match="register_identity_map"):
            e.execute("SELECT x FROM t WHERE CONTEXT IN (MCP(m));")
