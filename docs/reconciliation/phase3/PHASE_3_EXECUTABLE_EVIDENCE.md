# Phase 3 — Executable Evidence

## Scope and reproducibility

Evidence was collected against the frozen pair:

- `contextql` `a054c8fcc576f3913d98d664ddf71eeea56d9755`;
- `contextql-server` `78c9565c33237a21dbf87f11d92ac6c7f29a846e`.

The disposable Windows environment used Python 3.12.13, pytest 9.1.1, DuckDB 1.5.5, and Pandas 3.0.5. The core package was installed editable with all optional dependency groups; the server package was installed editable with its development dependencies.

Machine-readable outputs:

- `test_results.json` records collection counts, outcomes, durations, environment, and JUnit hashes.
- `probes.json` records 27 claim-targeted probes with observed values or exact failures.
- `tools/run_phase3_probes.py` regenerates the probes without persistent product state.

The raw JUnit files remain outside the repository in the disposable evidence directory. Their SHA-256 hashes are recorded so an archived copy can be verified if retained later.

## Test suite results

### Core

- 556 tests collected.
- 554 passed, 1 failed, and 1 skipped in 239.315 seconds.
- The one failure is import-order-sensitive test behavior in `test_to_polars_raises_without_polars`.
- Polars was installed but not yet present in `sys.modules`; the test therefore expected `ImportError`, while `Result.to_polars()` imported the available package and succeeded.
- A targeted rerun with Polars imported before pytest passed.
- A controlled full-suite rerun with Polars imported before collection passed 555 tests with 1 skipped in 202.308 seconds.

This is classified as a test/evidence gap rather than a failing product capability. The controlled rerun found no additional core failures. The test should still be repaired because the default full-suite result depends on unrelated import order.

### Server

- 109 tests collected.
- 109 passed in 31.359 seconds.
- One Starlette/httpx deprecation warning was emitted.

## High-value probe findings

### SQL and parser claims

- `SELECT 1;` does not parse because the grammar requires `FROM`; broad “standard SQL passes through unchanged” claims are not supported.
- Malformed input aborts parsing; the parser does not recover and continue to a later valid statement.
- The whitepaper composite-context example without `ON` fails, while the grammar/spec form with `ON id` parses.
- The whitepaper arrow-chain process-model form fails, while `EXPECTED PATH (...)` parses.

### Parse/lower/execute ladder

- `SELECT` parses, lowers to `SELECT`, and executes.
- `EXPLAIN CONTEXT` parses, lowers to `UNKNOWN`, and does not execute.
- `CREATE EVENT LOG` parses and lowers to `CREATE_EVENT_LOG`, but does not execute.
- `CREATE PROCESS MODEL` parses and lowers to `CREATE_PROCESS_MODEL`; execution does not provide a process-model DDL path.
- MCP provider registration, `GRANT`, `CREATE NAMESPACE`, and `SET` parse but lower to `UNKNOWN` and do not execute.

These observations demonstrate why grammar acceptance cannot be reported as implemented language capability.

### Context and statement semantics

- With `context_a = {1,2}` and `context_b = {2,3}`, `context_a THEN context_b` returned `{2}`. The observed behavior is indistinguishable from intersection and does not prove staged candidate-scoped or temporal sequence semantics.
- Two SELECT statements remain present after lowering, but `Engine.execute` returned only the first statement's result. Multi-statement parsing is therefore not a multi-result execution contract.

### HTTP and version evidence

- Generated OpenAPI contains 27 method/path operations, including catalog, history, provider, identity, audit, refresh, and explain surfaces omitted from the server README.
- The routes are unversioned.
- Generated OpenAPI reports application version 0.3.0, while installed `contextql-server` package metadata reports 0.1.0.
- The installed core package reports 0.2.0.

### Decision metadata

- The register contains 101 decision headings. Older README, CLAUDE, and register-summary counts are stale.

## Evidence limitations

- This phase does not establish production-operational maturity, deployment reliability, security enforcement, or external compatibility.
- The committed 10-million-row benchmark artifact was inspected during static review but was not regenerated as part of this phase; performance claims remain scoped to its recorded environment.
- The server test run uses the reference SQLite implementation and in-process HTTP test client.
- Absence of a successful probe establishes behavior at the pinned pair only; it does not decide whether code or documentation should change.
