# Phase 5 — Factual Truth-Repair Queue

## Purpose

This queue identifies public statements that can be corrected from pinned evidence without deciding the future language or architecture. It is deliberately separate from semantic adjudication.

The machine-readable queue is [`truth_repairs.csv`](truth_repairs.csv).

## Boundary

A factual truth repair may:

- narrow a current capability claim to demonstrated behavior;
- correct generated counts, routes, versions, or maturity;
- distinguish reference architecture from current implementation;
- disclose a known inconsistency;
- remove unsupported production/security/performance assurance.

It may not:

- choose between competing language semantics;
- bless incidental runtime behavior as the new contract;
- assign a release version or compatibility guarantee;
- convert future architecture into a delivery commitment;
- close a gap without acceptance evidence.

## Queue summary

The queue contains 18 recommended repairs:

- 1 P0 public-integrity repair;
- 16 P1 public-contract/status repairs;
- 1 P2 test-evidence repair.

The P0 is the current production-ready security/compliance assurance. At the pinned server boundary there is no authentication, RBAC, RLS, tenant enforcement, or tamper-evident audit chain. The design can remain as reference architecture, but present-tense assurance cannot.

## Recommended sequence

1. Remove or qualify unsupported security, compliance, SQL-conformance, and broad performance assurances.
2. Replace the binary implementation table with maturity-aware capability status.
3. Correct generated facts: statement/rule/decision/test counts, HTTP routes, and version mismatch.
4. Separate parse/model scaffolding from executable language and control-plane behavior.
5. Separate current single-node/storage/connector foundations from target architecture.
6. Repair the import-order-sensitive optional dependency test and preserve both Phase 3 observations until CI is deterministic.

## Phase handoff

These rows are recommendations only. Applying them belongs to the closure programme after Phase 5 classification is accepted. Rows that retain a future semantic or product choice point to that remaining decision explicitly.
