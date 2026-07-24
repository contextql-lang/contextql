# Post-Trade Roaring Contexts — Walkthrough

Copyright (c) 2026 Anton du Plessis

This walkthrough covers the `post-trade-roaring-contexts` branch outcome:
resolving, composing, and querying operational contexts over a deterministic
10-million-transaction post-trade dataset using Roaring Bitmap membership
snapshots. Plan: `docs/plans/post-trade-roaring-contexts.md`. All data is
synthetic; the DeepSee integration is a mock behind a documented contract
(`docs/architecture/DEEPSEE_CONNECTOR.md`).

## The question it answers

> Across 10 million transactions, which trades are most likely to fail
> settlement unless someone intervenes before the applicable market cutoff?

## Quick start

```bash
# Engine (this repo)
pip install -e ".[dev,executor,roaring]"
python benchmarks/post_trade_benchmark.py            # measurements + gates

# Server (sibling repo, includes the mock DeepSee connector)
cd ../contextql-server
pip install -e "../contextql[executor,roaring]" -e ".[dev]"
python demo/post_trade_demo.py                       # ten demo scenarios
```

Both default to 10,000,000 rows (`--rows` to change). Runtime is
machine-specific; see `docs/benchmarks/` for the clean-commit measurement and
environment details.

## What happens under the hood

1. **Dataset** (`contextql/datasets/post_trade.py`) — 10M causally
   correlated transactions generated entirely inside DuckDB from hash-derived
   randomness: deterministic for a given `(rows, seed, as_of)`, no Python
   objects, no Pandas.
2. **Executable DDL** — `CREATE CONTEXT ... WITH (materialized = TRUE,
   storage = 'roaring') AS SELECT ...` runs through `Engine.execute()`,
   validates options (SPEC §6, E150-E160), records versions, hashes, and
   audit events.
3. **Snapshots** — `REFRESH CONTEXT` evaluates the definition and promotes
   an immutable Roaring Bitmap snapshot atomically; failures keep the last
   good snapshot current; history records added/removed/score_changed
   events.
4. **Pushdown** — queries over materialized contexts compose bitmaps first
   and semi-join surviving IDs inside DuckDB; ten million rows are never
   transferred to Pandas. Missing snapshots raise E200; stale ones warn
   W100.
5. **Federation** — the mock DeepSee connector supplies membership + scores
   through the MCP role (ID lists or serialized roaring64 bitmaps) and
   evidence through the REMOTE role; the synchronizer ingests snapshots and
   watermark-ordered deltas into the same membership store that native
   contexts use (CS-8), so both compose in one predicate.
6. **Provenance** — results carry resolved context labels with snapshot
   versions (`name@vN`), provider calls, `data_as_of`, and the committed
   watermark.

## Representative query

```sql
SELECT t.transaction_id, t.counterparty_id, t.notional_usd,
       d.break_type, d.recommended_action,
       CONTEXT_SCORE() AS intervention_priority
FROM transactions AS t
LEFT JOIN REMOTE(deepsee.settlement_cases) AS d
  ON t.transaction_id = d.transaction_id
WHERE CONTEXT ON t IN (settlement_intervention_required,
                       deepsee_settlement_risk)
ORDER BY CONTEXT DESC
LIMIT 20;
```

## Measured results (10M rows, see docs/benchmarks/)

- Generation: 86.29 s, 855.1 MB on disk
- Cold materialization: 0.935 s average per context; 9/9 bitmap counts match
  reference SQL exactly
- Bitmap algebra: 0.447-18.865 ms in the measured run
- Warm top-20 intervention query: 753 ms with snapshot pushdown and exact SQL
  match
- Connector/native REMOTE narrowing: 46,703 requested IDs, exact reference
  match, `roaring64`
- Peak RSS: 1,270.4 MB

## Boundaries

- No claim is made about DeepSee's actual API; the mock is the executable
  contract until the discovery gate in `DEEPSEE_CONNECTOR.md` §6 is
  satisfied (plan PR 14).
- String/UUID/composite entity keys, RBAC/RLS, distributed federation, and
  streaming remain out of scope (plan §3).
