# Baseline Record — post-trade-roaring-contexts

Captured per plan §5 (Repository and branch preparation) before any behavior
changes on this branch.

**Date:** 2026-07-23

## Environment

| Item | Value |
|---|---|
| OS | Linux 7.0.0-28-generic (Ubuntu) |
| Python | 3.12.3 (fresh venv per repository) |
| lark | 1.3.1 |
| duckdb | 1.5.5 |
| pandas | 3.0.5 |
| pygls / lsprotocol | 2.1.1 / 2025.0.0 |
| fastapi | 0.139.2 |
| pydantic | 2.13.4 |

## contextql

- Commit: `50616cd` (plan document; last code change `6ad2886`)
- Install: `pip install -e ".[dev,executor,lsp]"`
- Result: **366 passed, 2 skipped** in 97.57s
- Failures: none

## contextql-server

- Commit: `a0b5f55` (after fast-forward recovery of
  `origin/context-resolution-layer` onto `post-trade-roaring-contexts`)
- Install: `pip install -e "../contextql[executor]"` then `pip install -e ".[dev]"`
- Result: **35 passed** in 2.93s
- Failures: none

## Notes

- Both repository venvs had to be recreated; the prior venvs were stale
  (missing `python`/`pip` binaries after a system Python upgrade).
- The server control-plane work (persistent catalog, provider registry,
  identity maps, audit, explain) was recovered onto this branch via
  fast-forward merge — exit criterion "no longer stranded on an unrelated
  remote branch" is met.
- `contextql[executor]>=0.2` added as an explicit server dependency; shared CI
  installs the engine from a sibling checkout of the matching branch.
