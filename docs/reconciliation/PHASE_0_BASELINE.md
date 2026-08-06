# Phase 0 — Reconciliation Charter and Frozen Baseline

## Charter status

- **Cycle:** `reconciliation-2026-08`
- **Status:** Active
- **Authorised:** 2026-08-06 (Africa/Johannesburg)
- **Purpose:** Establish the evidence boundary and governance rules for Phases 1–4 of the ContextQL whitepaper, specification, core implementation, and server implementation reconciliation.
- **Design authority:** The ContextQL project owner or delegated human maintainer. Agents may establish facts and recommend dispositions, but may not decide unresolved product intent.
- **Reconciliation lead:** The single integration owner appointed for this cycle. The lead owns the canonical claim identifiers, evidence linkage, conflict queue, and integrated change history.

The cycle is an audit of a pinned product pair, not a declaration that either implementation is correct merely because it exists. Product source changes made after the cutoff require an explicit rebaseline and are not silently absorbed into this cycle.

## Questions this cycle must answer

1. Which atomic claims do the whitepaper and related documents make about intent, current status, performance, and future direction?
2. What public and internal capabilities are actually present in the pinned core and server implementations?
3. Which claims can be verified by executable evidence, including negative and boundary cases?
4. Where do accepted intent, prose, implementation, and verification disagree or remain incomplete?
5. Which unmatched implementations are intended capabilities, documentation lag, or orphans?
6. Which unrealised ideas should become decision packets or roadmap candidates after Phase 4, rather than being mistaken for current commitments?

## Frozen repositories

| Component | Repository | Frozen branch reference | Frozen commit | Package version | Audit role |
|---|---|---|---|---|---|
| ContextQL core | `https://github.com/contextql-lang/contextql.git` | `origin/main` | `a054c8fcc576f3913d98d664ddf71eeea56d9755` | `contextql 0.2.0` | Language, grammar, semantic model, execution, catalogs, provider contracts, SDK, tooling, tests, and benchmarks |
| ContextQL server | `https://github.com/contextql-lang/contextql-server.git` | `main` (also `origin/main` at freeze) | `78c9565c33237a21dbf87f11d92ac6c7f29a846e` | `contextql-server 0.1.0` | HTTP control plane, lifecycle, persistence, engine synchronisation, connectors, identity, audit, tests, and operational surfaces |

Commit identity, not a moving branch name, defines the evidence boundary. Branch names are provenance only.

## Compatibility baseline and assumptions

The two commits above are the sole supported pair for this audit. This is a **reconciliation compatibility baseline**, not proof of a published compatibility guarantee.

- Core declares Python `>=3.10` and package version `0.2.0`.
- Server declares Python `>=3.10` and dependency `contextql[executor,roaring]>=0.2`; the pinned core therefore satisfies the server's declared version floor.
- No cross-repository submodule, lockfile, or immutable package reference establishes this pair automatically. This charter supplies the missing audit pin.
- Dependency declarations use ranges. Exact interpreter, operating-system, package, database, and benchmark configuration must be recorded with every Phase 3 executable result. Until that environment record exists, runtime and performance results are provisional evidence.
- A successful import or version match is not sufficient evidence of semantic, API, persistence, or operational compatibility. Those dimensions require their own tests or probes.
- Findings apply to this pair only. They must not be generalised to later branches or releases without re-verification.

## Included evidence

### Core

All source-controlled material at core commit `a054c8fcc576f3913d98d664ddf71eeea56d9755`, including:

- `WHITEPAPER.md`, `SPEC.md`, `DECISIONS.md`, and `README.md`;
- grammar, parser, semantic and execution code;
- context DDL, catalogs, membership, history, snapshot, temporal, bitmap, provider, and identity paths;
- public SDK, builder, CLI, LSP, notebook, examples, and demos;
- tests, benchmarks, CI configuration, packaging, and source-controlled proposals or architectural documents.

### Server

All source-controlled material at server commit `78c9565c33237a21dbf87f11d92ac6c7f29a846e`, including:

- README and architecture or operational documentation;
- routes, request and response models, and generated OpenAPI behaviour;
- services, dependency wiring, repositories, migrations, and startup restoration;
- lifecycle, refresh, provider, identity, connector, explain, audit, and engine-integration paths;
- tests, demos, CI configuration, packaging, and configuration.

### Workspace-level context

The following workspace-level files may be used as historical, review, or process context: `CLAUDE.md`, `criticalreview.md`, `RoadmapGapClosurePlan.md`, and the source copy of this methodology. They are not product implementation evidence and are not normative unless a current accepted decision explicitly adopts their content.

## Explicit exclusions

The following are outside the baseline and must not be cited as evidence of the frozen product:

| Excluded material | Identifier or path | Reason |
|---|---|---|
| Discovery Bank pilot-pitch commit | `a95ddbf` (`Add Discovery Bank ContextQL pilot pitch`) | Branch-only feature/presentation work after the frozen core commit |
| Discovery Bank visual commit | `faf95f8` (`Add Discovery Bank concept visuals`) | Branch-only feature/presentation work after the frozen core commit |
| Local presentation modification | `docs/proposals/ContextQL_Discovery_Bank_Concept_Pitch.pptx` in the `agent/discovery-bank-pilot-pitch` worktree | Uncommitted local state with no immutable provenance |
| Any product change after the pinned SHAs | Any later commit or uncommitted change in either repository | Outside the frozen evidence boundary unless the charter is formally revised |
| Generated, cached, or local-environment output | Build products, caches, virtual environments, temporary files, editor state | Not durable or reproducible source evidence |

The exclusions do not judge product merit. They prevent sales, pilot, or presentation material from becoming accidental evidence of a shipped capability.

## Evidence cutoff

- **Source cutoff:** The immutable commits listed above.
- **Charter timestamp:** 2026-08-06 in Africa/Johannesburg.
- **Collection window:** Evidence may be collected after the charter timestamp, but every observation must execute or inspect the pinned source pair.
- **Rebaseline rule:** A proposed source change requires a charter amendment that records the old and new SHAs, reason, impact on completed rows, and design-authority approval. Previously collected evidence affected by the change becomes stale until repeated.

## Claim authority

There is no universal source of truth. Authority is assigned by claim type in `source_authority.csv`.

General precedence rules:

1. A current, accepted entry in `DECISIONS.md` is the strongest evidence of intended semantics or public contract. Superseded, open, or contradictory entries create a decision gap.
2. `SPEC.md` defines normative language intent where it does not conflict with a current accepted decision.
3. `WHITEPAPER.md` explains the architectural thesis and strategic direction; it does not prove current implementation, conformance, performance, or operational maturity.
4. Grammar, exported symbols, routes, migrations, and runtime code establish what surfaces or paths exist at the pinned commits. They do not establish that the behaviour is intended.
5. Tests and probes establish only the behaviour they assert or observe. Presence of a passing happy path does not prove complete semantics or production maturity.
6. Git history establishes provenance and change boundaries, not design authority.
7. Proposals, plans, TODOs, demos, mocks, and extension points are evidence of ideas or partial work, not accepted commitments.
8. A disagreement among applicable authorities is recorded; it is never silently resolved by selecting a convenient source.

## Audit invariants

These rules apply to all Phase 1–4 artifacts:

- **Atomicity:** One capability-register row represents one falsifiable claim.
- **Four-axis assessment:** Intended, present, verified, and operationally mature are assessed separately.
- **Stable identity:** Claims receive stable domain-prefixed IDs and are not renumbered to hide deletion, merging, or disagreement.
- **Bidirectional coverage:** Document claims must be matched to implementation evidence, and implementation capabilities must be matched back to intent or documentation.
- **Positive and negative evidence:** Reviewers search both for working paths and for placeholders, unimplemented branches, invalid inputs, failed transitions, unsafe defaults, missing persistence, and weak tests.
- **No maturity inflation:** A symbol, grammar production, route, or model alone is at most surface evidence. Maturity follows the M0–M6 gates in `METHODOLOGY.md`.
- **No silent inference:** Inferences are labelled as inferences, carry a confidence level, and identify the evidence from which they were derived.
- **Reproducibility:** Runtime and performance statements without command, environment, expected result, and observed result remain provisional.
- **Immutable citation:** Product evidence always names repository and full commit SHA. A bare working-tree path is insufficient.
- **Traceable conflict:** Every material contradiction receives a reconciliation status and either a proposed disposition or a decision packet.
- **Controlled scope:** Discovery Bank material and later source changes remain excluded even when they are locally visible.

## Evidence conventions

### Source citation

Use this form:

```text
core@a054c8fcc576f3913d98d664ddf71eeea56d9755:path/to/file.py:L10-L24#SymbolName
server@78c9565c33237a21dbf87f11d92ac6c7f29a846e:path/to/file.py:L10-L24#SymbolName
```

Use the narrowest useful line span and include a symbol when one exists. For whole-file metadata, use `#file`. Document claims use the same format and state whether the passage is normative, explanatory, status, performance, future, or non-binding.

### Executable evidence

Each probe or test record must include:

- stable evidence ID, such as `EV-LANG-001` or `EV-SRV-014`;
- pinned repository SHA or both SHAs for cross-repository tests;
- exact command, fixture, or public request;
- environment: OS, architecture, Python, installed package versions, relevant configuration, and database/runtime versions;
- expected and observed result;
- exit status and retained output or artifact location;
- collection timestamp and reviewer;
- affected capability IDs;
- whether it is positive, negative, boundary, compatibility, restart, failure-injection, security, or performance evidence.

### Confidence

- **High:** Direct immutable source evidence plus a focused passing or failing test/probe; independently reproducible where material.
- **Medium:** Direct static evidence or an executable observation with incomplete environmental or boundary coverage.
- **Low:** Naming similarity, indirect references, incomplete history, comments, TODOs, proposals, or unverified inference.

Absence is reported as “not found after the recorded search,” not as proof that a capability cannot exist.

## Agent and change controls

- Agents work only in their assigned artifact or source domain and must not edit primary product documents during Phases 0–4.
- Shared artifacts have one integration owner. Other agents submit structured rows or decision packets and avoid overlapping edits.
- Agents do not commit or push unless the reconciliation lead explicitly delegates that action.
- The reconciliation lead reviews the complete diff, validates generated or copied artifacts, and creates coherent checkpoint commits at opportune phase boundaries.
- Checkpoint commits must state the phase and artifact scope. Do not combine product implementation changes with evidence-only reconciliation artifacts.
- Pushes use the designated reconciliation branch. No force-push, history rewrite, destructive checkout, or reset is permitted.
- Existing user changes are preserved. Locally visible excluded material is neither modified nor staged.
- Claims, evidence, and findings are append-only in identity: corrections retain provenance and explain supersession.
- Agents may not resolve semantic, compatibility, lifecycle, security, or roadmap-intent conflicts. They prepare evidence-backed decision packets for the human design authority.
- Cross-review begins only after independent extraction or inventory submission, limiting confirmation bias.
- A reviewer must disclose when a conclusion depends on another agent's unverified row.

## Phase 0 exit criteria

Phase 0 is complete when:

- both repositories are pinned to immutable commits and their branch provenance is recorded;
- the compatibility assumptions and their limitations are explicit;
- included and excluded evidence is unambiguous;
- the authority matrix and evidence conventions are available to every reviewer;
- the cutoff and rebaseline process are recorded;
- agent ownership, integration, commit, and push controls are recorded;
- the methodology copy is verified against its source;
- the reconciliation worktree contains no unrelated product change.
