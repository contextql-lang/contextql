# Phase 5 — Gap Classification and Decision Preparation

Phase 5 converts Phase 4 claim-level evidence into decision-sized material gaps. It classifies maturity, severity, confidence, closure streams, and recommended disposition without deciding product intent.

## Start here

1. [`PHASE_5_CLASSIFICATION.md`](PHASE_5_CLASSIFICATION.md) — consolidated Phase 5 outcome.
2. [`GAP_REGISTER_METHOD.md`](GAP_REGISTER_METHOD.md) — normalization and coverage rules.
3. [`gap_register.csv`](gap_register.csv) — 30 material gaps.
4. [`TRUTH_REPAIR_QUEUE.md`](TRUTH_REPAIR_QUEUE.md) — 18 factual public-status repairs.
5. [`decisions/LANGUAGE_SEMANTICS_DOCKET.md`](decisions/LANGUAGE_SEMANTICS_DOCKET.md) — 10 language decision packets.
6. [`decisions/PLATFORM_OPERATIONS_DOCKET.md`](decisions/PLATFORM_OPERATIONS_DOCKET.md) — 11 platform/operations/security packets.

## Machine-readable artifacts

- [`gap_register.csv`](gap_register.csv)
- [`truth_repairs.csv`](truth_repairs.csv)
- [`decisions/language_decisions.csv`](decisions/language_decisions.csv)
- [`decisions/platform_decisions.csv`](decisions/platform_decisions.csv)

## Reproducibility

Run [`../tools/build_gap_register.py`](../tools/build_gap_register.py) from the repository root. The generator validates exact coverage of every Phase 4 conflict, adversarial finding, and high-confidence partial.

## Boundary

The dockets contain specialist recommendations. Except for factual truth-repair classification, every proposed semantic, compatibility, security, or roadmap outcome remains `recommended-awaiting-decision` until accepted by the design authority and recorded with explicit status and supersession metadata.
