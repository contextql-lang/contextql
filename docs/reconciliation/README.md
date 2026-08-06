# ContextQL Reconciliation Evidence

This directory contains the evidence produced by the ContextQL whitepaper, specification, core-runtime, and server reconciliation cycle through Phase 4.

## Frozen pair

- `contextql`: `a054c8fcc576f3913d98d664ddf71eeea56d9755`
- `contextql-server`: `78c9565c33237a21dbf87f11d92ac6c7f29a846e`

The pair is an audit baseline, not a general compatibility guarantee. Scope and exclusions are recorded in [`PHASE_0_BASELINE.md`](PHASE_0_BASELINE.md).

## Start here

1. [`PHASES_0_4_REPORT.md`](PHASES_0_4_REPORT.md) — consolidated outcome and material findings.
2. [`METHODOLOGY.md`](METHODOLOGY.md) — governing process, authority rules, maturity model, and later phases.
3. [`phase4/PHASE_4_TRACEABILITY.md`](phase4/PHASE_4_TRACEABILITY.md) — claim-to-evidence join and coverage.
4. [`phase4/ADVERSARIAL_REVIEW.md`](phase4/ADVERSARIAL_REVIEW.md) — high-risk challenge and decision docket.

## Phase artifacts

| Phase | Human-readable artifact | Machine-readable artifact |
|---|---|---|
| 0 — Charter and baseline | [`PHASE_0_BASELINE.md`](PHASE_0_BASELINE.md) | [`source_authority.csv`](source_authority.csv) |
| 1 — Claim corpus | [`phase1/PHASE_1_CLAIM_CORPUS.md`](phase1/PHASE_1_CLAIM_CORPUS.md) | [`phase1/claims.csv`](phase1/claims.csv) |
| 2 — Implementation inventories | [`phase2/CORE_IMPLEMENTATION_INVENTORY.md`](phase2/CORE_IMPLEMENTATION_INVENTORY.md), [`phase2/SERVER_IMPLEMENTATION_INVENTORY.md`](phase2/SERVER_IMPLEMENTATION_INVENTORY.md) | [`phase2/core_capabilities.csv`](phase2/core_capabilities.csv), [`phase2/server_capabilities.csv`](phase2/server_capabilities.csv) |
| 3 — Executable evidence | [`phase3/PHASE_3_EXECUTABLE_EVIDENCE.md`](phase3/PHASE_3_EXECUTABLE_EVIDENCE.md) | [`phase3/probes.json`](phase3/probes.json), [`phase3/test_results.json`](phase3/test_results.json) |
| 4 — Traceability join | [`phase4/PHASE_4_TRACEABILITY.md`](phase4/PHASE_4_TRACEABILITY.md), [`phase4/ADVERSARIAL_REVIEW.md`](phase4/ADVERSARIAL_REVIEW.md) | [`phase4/traceability.csv`](phase4/traceability.csv), [`phase4/high_risk_findings.csv`](phase4/high_risk_findings.csv) |

## Reproducibility tools

- [`tools/extract_claims.ps1`](tools/extract_claims.ps1) regenerates the Phase 1 corpus.
- [`tools/run_phase3_probes.py`](tools/run_phase3_probes.py) regenerates the Phase 3 behavioral probes.
- [`tools/join_claims.py`](tools/join_claims.py) regenerates the Phase 4 traceability matrix.

## Interpretation boundary

These artifacts establish traceable evidence. They do not adjudicate intended semantics, change product code, or rewrite the whitepaper. Those actions begin in Phase 5 after the design authority resolves the decision docket.
