from contextql.linter import Catalog, CatalogContext, CatalogTable, ContextQLLinter

catalog = Catalog()
catalog.add_table(CatalogTable(name="invoices", primary_key="invoice_id", primary_key_type="INT64"))
catalog.add_table(CatalogTable(name="vendors", primary_key="vendor_id", primary_key_type="INT64"))
catalog.add_context(CatalogContext(name="late_invoice", entity_key="invoice_id", entity_key_type="INT64", has_score=False))
catalog.add_context(CatalogContext(name="supplier_risk", entity_key="vendor_id", entity_key_type="INT64", has_score=True))

linter = ContextQLLinter(catalog)
query = """
SELECT i.invoice_id, v.vendor_id, CONTEXT_SCORE() AS score
FROM invoices i
JOIN vendors v ON i.invoice_id = v.vendor_id
WHERE CONTEXT IN (late_invoice, supplier_risk)
ORDER BY CONTEXT DESC;
"""

for diag in linter.lint(query):
    print(f"{diag.severity.upper()} {diag.rule_id}: {diag.message}")
    if diag.suggestion:
        print(f"  suggestion: {diag.suggestion}")
