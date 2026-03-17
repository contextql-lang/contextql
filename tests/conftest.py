"""Shared fixtures for ContextQL tests."""
import pytest

from contextql.parser import ContextQLParser
from contextql.linter import (
    Catalog, CatalogContext, CatalogTable, CatalogEventLog, ContextQLLinter,
)


@pytest.fixture
def parser():
    return ContextQLParser()


@pytest.fixture
def catalog():
    """Standard test catalog with invoices, vendors, and contexts."""
    cat = Catalog()
    cat.add_table(CatalogTable("invoices", "invoice_id", "INT64",
                               columns={"invoice_id": "INT64", "amount": "DOUBLE", "status": "VARCHAR"}))
    cat.add_table(CatalogTable("vendors", "vendor_id", "INT64",
                               columns={"vendor_id": "INT64", "name": "VARCHAR"}))
    cat.add_table(CatalogTable("orders", "order_id", "INT64"))

    cat.add_context(CatalogContext("late_invoice", "invoice_id", "INT64"))
    cat.add_context(CatalogContext("high_value", "invoice_id", "INT64", has_score=True))
    cat.add_context(CatalogContext("supplier_risk", "vendor_id", "INT64", has_score=True))
    cat.add_context(CatalogContext("temporal_ctx", "invoice_id", "INT64", is_temporal=True))
    cat.add_context(CatalogContext("dep_child", "invoice_id", "INT64",
                                   dependencies=["dep_parent"]))
    cat.add_context(CatalogContext("dep_parent", "invoice_id", "INT64"))

    cat.add_event_log(CatalogEventLog("order_log", "orders", "order_id", "activity", "event_time"))
    return cat


@pytest.fixture
def linter(catalog):
    return ContextQLLinter(catalog)
