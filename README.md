# ContextQL

**ContextQL** is a SQL-first query language for operational intelligence.

It introduces **contexts as first-class query primitives**, enabling reusable definitions of operational situations such as risk conditions, process anomalies, and compliance violations.

Example:

```sql
SELECT *
FROM invoices
WHERE CONTEXT IN (late_invoice, supplier_risk)
ORDER BY CONTEXT DESC
LIMIT 20;
```

ContextQL combines:

- SQL-compatible syntax
- process intelligence functions
- context-based filtering and ranking
- columnar-native execution
- optional vector and AI integrations

## Project Goals

- Provide a reusable abstraction for operational contexts
- Enable millisecond retrieval of prioritized business situations
- Maintain compatibility with modern analytical engines

## Resources

Specification: `SPEC.md`  
Whitepaper: `WHITEPAPER.md`

Project website: https://contextql.org
