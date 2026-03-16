# ContextQL Language Specification

Version: v0.1  
Status: Draft
Copyright (c) 2026 Anton du Plessis (github/adpatza)

## Overview

ContextQL is a SQL-first language that introduces contexts as reusable query primitives for operational intelligence systems.

## Core Features

- CREATE CONTEXT
- WHERE CONTEXT IN (...)
- ORDER BY CONTEXT
- Context ranking
- Context windowing
- Process-aware analytic functions

## Example

```sql
CREATE CONTEXT late_invoice
ON invoice_id AS
SELECT invoice_id
FROM invoices
WHERE due_date < CURRENT_DATE
AND paid_date IS NULL;
```

```sql
SELECT *
FROM invoices
WHERE CONTEXT IN (late_invoice)
ORDER BY CONTEXT DESC;
```

## Licences
The ContextQL specification is licensed under CC-BY-4.0.
Reference implementations are licensed under Apache 2.0.
