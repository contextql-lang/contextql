# Phase 1 — Whitepaper Claim Corpus

## Outcome

Phase 1 establishes an auditable baseline of **753 whitepaper claims** in
[`claims.csv`](claims.csv). Every claim has a content-derived stable ID, an
exact `WHITEPAPER.md` line location, the section in which it appears, a claim
class and domain, the maturity stated by the whitepaper, a normativity label,
and the evidence that would be needed to accept the claim.

The corpus is deliberately broader than the known problem list. Every numbered
whitepaper chapter has at least one claim, including Chapter 39, whose contract
is expressed almost entirely as fenced DDL rather than prose.

This is an extraction and initial-risk pass, not an adjudication. A conflict
note means that later phases must compare the named sources and executable
evidence; it does not by itself select a winner.

## Corpus schema

| Column | Meaning |
|---|---|
| `claim_id` | `CQL-WP-` plus the first 12 hexadecimal characters of a SHA-256 hash of normalized claim text. It remains stable if a claim moves, but intentionally changes if its substance is edited. |
| `source_path`, `source_lines`, `section` | Exact provenance in the current whitepaper. A range denotes the paragraph from which a sentence was extracted. |
| `atomic_claim` | A declarative sentence, table-row contract, status item, or DDL form suitable for independent verification. |
| `claim_class` | Status, language surface, behavior/semantics, architecture, requirement, interface, performance, or general design claim. |
| `domain` | Primary specialist review domain. |
| `stated_maturity_or_target_version` | What the whitepaper says locally; it is not an independently verified maturity judgment. |
| `normativity` | Normative, aspirational, design intent, or descriptive assertion. |
| `expected_evidence` | Minimum evidence family needed to substantiate the claim. |
| `corroborating_or_related_sources` | Initial routing to specification, decisions, README, tooling, architecture, or plan material. |
| `potential_conflict_or_drift` | A concrete inconsistency or volatility signal discovered during specialist review. Blank means “not yet flagged,” not “verified.” |
| `review_risk` | `high`, `medium`, or `normal`, used to order later evidence collection. |
| `initial_notes` | Extraction qualification, including sentences that may warrant finer splitting during adjudication. |

## Extraction and review method

1. The extractor walks `WHITEPAPER.md` while retaining the active Markdown
   section and exact source lines.
2. Prose is split at sentence and semicolon boundaries. Commas and conjunctions
   are preserved because blindly splitting them often changes technical meaning.
   Such rows are explicitly marked for a possible reviewer split.
3. Bullet and numbered-list items are treated as independent claims.
4. Markdown tables are normalized one row at a time, retaining every header/value
   relationship in the row.
5. Fenced examples are excluded except in Chapter 39. Each semicolon-terminated
   Chapter 39 form is retained because that chapter is the whitepaper's DDL
   reference, not merely an example.
6. Claims are classified mechanically, then specialist-reviewed for high-risk
   themes: implementation status, scoring and warning codes, temporal semantics,
   lifecycle, bounded execution, performance, federation, security, and deferred
   features.
7. Cross-references were seeded against `SPEC.md`, `DECISIONS.md`, `README.md`,
   `docs/TOOLING.md`, `docs/LANGUAGE_SERVER_SPEC.md`, the bitmap and resolution
   plane architecture records, and the correctness/hardening plans. There is no
   `CLAUDE.md` in this worktree, so none could be included.

The reproducible extractor is
[`../tools/extract_claims.ps1`](../tools/extract_claims.ps1). Run it from the
repository root with:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File docs/reconciliation/tools/extract_claims.ps1
```

## Coverage

### By review domain

| Domain | Claims |
|---|---:|
| Language semantics | 175 |
| Product architecture | 148 |
| Storage and lifecycle | 99 |
| Process intelligence | 96 |
| Security and governance | 76 |
| Developer platform | 70 |
| Federation and identity | 52 |
| Execution and optimization | 37 |
| **Total** | **753** |

### By claim class

| Claim class | Claims |
|---|---:|
| Design claim | 300 |
| Behavior/semantics | 156 |
| Architecture | 99 |
| Language surface | 65 |
| Interface contract | 40 |
| Requirement | 40 |
| Performance target | 28 |
| Maturity/status | 25 |

### By whitepaper area

| Area | Claims |
|---|---:|
| Abstract and Chapters 1–10 | 228 |
| Chapters 11–20 | 140 |
| Chapters 21–30 | 131 |
| Chapters 31–40 | 104 |
| Chapters 41–43 | 29 |
| Appendix A glossary | 121 |
| **Total** | **753** |

The appendix count is intentionally substantial: it restates many public
contracts in a form that can drift independently from the body. Later phases
should link true restatements to a canonical claim rather than silently discard
them.

### Maturity labels carried from the whitepaper

| Stated maturity or target | Claims |
|---|---:|
| Architecture claim; not stated locally | 681 |
| Implemented (whitepaper v0.2 assertion) | 17 |
| v1 target or contract | 15 |
| Implementation Phase 1 | 10 |
| Implementation Phase 2 | 10 |
| v2 target or discussion | 7 |
| Future direction; version unspecified | 6 |
| Specified reference architecture | 3 |
| Designed protocol surface | 3 |
| Deferred to v2 | 1 |

The concentration of 681 locally unqualified architecture claims is itself a
finding. Readers cannot reliably distinguish shipped behavior from design
intent by reading most chapters in isolation.

## Initial risk findings

The ledger contains 50 high-risk rows with a concrete drift note, 231 medium-risk
rows, and 472 normal-priority rows. The most consequential clusters are below.

| Claim | Risk discovered during extraction |
|---|---|
| `CQL-WP-7D5BDE108A84` — temporal qualifiers filter temporal column values | `SPEC.md` §§6 and the newer CS-16/CS-22 decisions resolve `AT` and `BETWEEN` against recorded membership history. This is a direct semantic conflict, not just missing implementation. |
| `CQL-WP-9A51E8EA195F` — out-of-range scores emit W100 | The decision to warn is consistent, but the implemented registry uses W003. `SPEC.md` and the registry reserve W100 for stale snapshots. |
| `CQL-WP-F6B02C9B10FA` — scoreless context windows emit W101 | The implemented code is W001; `SPEC.md` and the registry assign W101 to failed refresh. |
| `CQL-WP-945AA0A449F8` — `CONTEXT_SCORE()` scope failure is E111 | `README.md` and `contextql/errors.py` use E108; E111 is the score-expression type error. |
| `CQL-WP-BF1F3291584C` — every context follows a nine-state lifecycle | The executable catalog currently uses free-form string states, including `active` and `draft`; `SPEC.md` does not canonically define this nine-state machine. |
| `CQL-WP-FFCA3426B13A` — stale state emits W010 | The current specification and registry use W100 for staleness; W010 and the adjacent W012 freshness contract are absent. |
| `CQL-WP-9B44A1743052` — dependency skew emits W013 | W013 is recorded in a decision, but is absent from `SPEC.md` and the implementation error registry. |
| `CQL-WP-9E08C9D03070` — standard SQL passes through unchanged | This is broader than the scoped conformance declaration in Chapter 40 and requires dialect-specific evidence. |
| `CQL-WP-8D44BBF86B38` — execution targets DuckDB, Polars, and Arrow | Current public implementation status identifies a hybrid DuckDB engine and DuckDB adapter; equivalent Polars/Arrow execution needs evidence or maturity relabeling. |
| `CQL-WP-B537AEBC3005` — bitmaps provide O(1) membership probes | The architecture supports fast bitmap membership, but the end-to-end asymptotic assertion is stronger than the current benchmark and architecture evidence. |
| `CQL-WP-55617C0F6BB4` — millisecond-class retrieval | The correctness-consolidation plan still calls for benchmark provenance, so the claim must remain unverified until raw, reproducible evidence is attached. |
| `CQL-WP-0E0C6F65E3BA` — LLM synthesis is deferred to v2 | Section 34 presents a substantial API surface without consistently repeating the deferred maturity label. |

The 17 “Implemented” rows in lines 112–128 are all high-priority verification
targets because they are version-sensitive inventory claims. Counts such as “27
statement types” and “11 lint rules” should ultimately be generated from
registries/tests or removed from manually maintained prose.

## Quality checks

The generated corpus passed the following structural checks:

- 753 unique claim IDs; no collisions or duplicate IDs.
- Every required field is populated.
- Every source line or range is within the current `WHITEPAPER.md`.
- Every numbered chapter has at least one extracted claim.
- 361 claims carry at least one initial cross-reference.
- All 50 rows marked high risk include a concrete conflict/drift note.

## Limitations and Phase 2 handoff

- “Atomic” is presently sentence- or table-row-level. Some source sentences bind
  several independently testable propositions. They are flagged in
  `initial_notes` and should be split only when evidence shows the clauses have
  different outcomes; the original ID can then become a parent/superseded claim.
- Cross-references are routing hints, not proof. Exact implementation symbols,
  tests, benchmark artifacts, and server endpoints belong in the Phase 2 evidence
  inventory and Phase 3 traceability matrix.
- Except for the normative DDL reference, illustrative SQL/Python snippets and
  diagrams were not independently extracted. Reviewers should add a claim when
  an example is the only place a behavior is promised.
- Maturity is intentionally inherited from local wording. The corpus does not
  infer that a feature is implemented merely because the whitepaper uses present
  tense.
- The server repository is not represented by executable evidence in this Phase
  1 worktree. Server claims require the pinned sibling-repository baseline before
  adjudication.
- Regulatory and security statements are design claims until enforcement and
  adversarial evidence is attached; they should not be interpreted as compliance
  certification.

Phase 2 should start with the 50 high-risk rows and the 17 implementation-status
rows, then collect executable evidence across the language, engine, server,
storage, federation, tooling, and benchmark surfaces. It should preserve these
claim IDs as the join key for all later matrices and decision records.
