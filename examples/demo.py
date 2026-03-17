from contextql.engine import ContextQLEngine

engine = ContextQLEngine()

engine.conn.execute("""
CREATE TABLE invoices (
    invoice_id INTEGER,
    amount DOUBLE,
    status TEXT
)
""")

engine.conn.execute("""
INSERT INTO invoices VALUES
(1, 100, 'open'),
(2, 500, 'open'),
(3, 200, 'paid')
""")

engine.create_context(
    "high_value",
    "SELECT invoice_id FROM invoices WHERE amount > 300",
    "invoice_id"
)

print(engine.resolve_context("high_value"))