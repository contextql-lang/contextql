# Phase 5 Gap Register Method

## Purpose and scope

Phase 5 converts claim-level disagreement into decision-sized material gaps. The register is not a second traceability matrix and does not decide product semantics. It groups evidence that needs one coordinated disposition, records the capability maturity visible in the frozen Phase 0 pair, and makes unresolved intent explicit as `awaiting-design-authority`.

Inputs are frozen Phase 4 artifacts:

- `phase4/traceability.csv`: all 753 whitepaper claims, including 77 `conflict` rows;
- `phase4/high_risk_findings.csv`: 32 adversarial findings, including reverse findings with no whitepaper claim ID;
- Phase 2 core/server capability inventories for capability-ID validation;
- Phase 3 probes for probe-ID validation.

The output is `gap_register.csv`. It contains 30 normalized material gaps and the required fields:

```text
gap_id,title,claim_ids,finding_ids,capability_ids,probe_ids,
gap_classification,dimensions,severity,confidence,current_state,
recommended_disposition,closure_stream,decision_required,
decision_packet_hint,acceptance_evidence,dependencies
```

An empty `claim_ids` value is valid for a reverse finding discovered from code/server behavior. An empty `probe_ids` value means Phase 4 relied on static, test-suite, benchmark, or other recorded evidence rather than a named Phase 3 probe. The columns themselves are always present.

## Normalization rules

1. **Use one row per disposition unit.** Claims are combined when the same design choice and closure evidence resolves them. For example, score range/null behavior and strategy execution are one score-algebra contract, while score diagnostic-code identity remains a separate compatibility decision.
2. **Do not equate shared wording with shared closure.** The executing server explain endpoint is separate from general HTTP versioning because it has a safety/side-effect decision and different acceptance evidence.
3. **Assign each Phase 4 conflict claim once.** This prevents one stale sentence from inflating the roadmap and gives every conflict one primary closure home.
4. **Assign each Phase 4 finding once.** Reverse findings are retained even when they have no claim ID. A broad finding may contribute competing evidence to one normalized row while its individual claim IDs are routed to the more specific disposition unit.
5. **Retain strong partial evidence.** Every high-confidence `partial` row is either attached to a material gap or would require a documented exclusion. This cycle excludes none.
6. **Preserve evidence IDs.** Capability and named Phase 3 probe IDs are unions of the mapped claims/findings, then validated against their source inventories.
7. **Do not invent decisions.** A gap that changes semantics, compatibility, security policy, public versioning, or roadmap commitment has `decision_required=yes`, a decision question, and a disposition beginning `awaiting-design-authority`.
8. **Keep factual repairs actionable.** Generated counts, maturity labels, bounded claims, and deterministic test repair do not wait for a design decision when the evidence already establishes the correction.

`CQL-GAP-NNN` identifiers are persistent identities, not row numbers. New gaps receive the next unused ID; existing gaps keep their ID when titles, evidence, or disposition detail changes. If a gap is later merged, split, rejected, or closed, its ID must remain in history with a successor or closure reference and must not be reused.

Three claim overlaps in the Phase 4 findings were deliberately given one primary home:

- `CQL-WP-506FAB66AF2A` maps to `CQL-GAP-020` (explain side effects), not the general HTTP gap;
- `CQL-WP-7DBCE3652446` maps to `CQL-GAP-021` (provider activation/health), not the general HTTP gap;
- `CQL-WP-CFB95979A8CF` maps to `CQL-GAP-021` (provider registration-to-runtime contract), not the generic DDL ladder.

## Classification, maturity, and disposition

`gap_classification` uses the methodology taxonomy (`D`, `I`, `S`, `V`, `T`, `P`, `R`, and `C`). `dimensions` carries the affected surfaces. `current_state` embeds the applicable M0-M6 assessment because a normalized gap can span several capabilities at different maturity levels; it does not falsely collapse them to a single number.

The 30 normalized gaps classify as follows:

| Classification | Count |
|---|---:|
| Semantic divergence (`V`) | 9 |
| Documentation lag (`D`) | 6 |
| Partial vertical slice (`P`) | 6 |
| Cross-document conflict (`C`) | 4 |
| Test/evidence gap (`T`) | 2 |
| Implementation lag (`I`) | 1 |
| Specification gap (`S`) | 1 |
| Roadmap orphan/status leakage (`R`) | 1 |

Severity is 1 P0, 25 P1, and 4 P2. Twenty-five gaps require design-authority decisions; five are factual/evidence closure work. Severity is inherited from the material impact, normally the maximum severity of the combined findings, rather than averaged down during deduplication.

Closure streams use the four methodology lanes:

- `truth-repair` for false or over-broad current claims;
- `contract-repair` for specification, decision, syntax, API, and compatibility alignment;
- `vertical-slice-completion` for approved capabilities that stop at a layer boundary;
- `hardening` for negative cases, operational proof, security evidence, and regression controls.

A row can require more than one stream. This is sequencing information, not a decision that the entire designed feature must be built.

## High-confidence partial audit

All 15 high-confidence partials have one primary normalized gap:

| Claim | Gap | Treatment |
|---|---|---|
| `CQL-WP-50D6543E9F5B` | `CQL-GAP-004` | Existing linter works; generated rule inventory closes the stale count. |
| `CQL-WP-3D7A04797C8A` | `CQL-GAP-005` | SDK symbols are included in capability-specific maturity reporting. |
| `CQL-WP-F19F88419F29` | `CQL-GAP-005` | Direct provider runtimes exist but their bounded maturity belongs in the status-table repair. |
| `CQL-WP-0DE66027C15A` | `CQL-GAP-015` | Implemented lifecycle subsets do not establish the nine-state FSM. |
| `CQL-WP-35F33109BD23` | `CQL-GAP-021` | Runtime providers and designed registration DDL meet at the missing activation contract. |
| `CQL-WP-A9DD818D939D` | `CQL-GAP-027` | THEN syntax exists; staged semantics are unresolved. |
| `CQL-WP-09F05D8E1348` | `CQL-GAP-027` | Unlimited/left-associative behavior needs the same THEN semantic decision. |
| `CQL-WP-CD2EFEC6AEEC` | `CQL-GAP-027` | Scoped membership and downstream score are part of the THEN contract. |
| `CQL-WP-A3F2E3E6B766` | `CQL-GAP-027` | Non-commutativity needs an instrumented staged-evaluation proof. |
| `CQL-WP-C5C16B448A02` | `CQL-GAP-014` | OCEL deferral is sound; forward compatibility remains unproved. |
| `CQL-WP-0D29D6A2491C` | `CQL-GAP-028` | Event-log DDL parses/lowers but lacks execution and persistence. |
| `CQL-WP-CFB95979A8CF` | `CQL-GAP-021` | MCP registration belongs with provider activation, credentials, and health. |
| `CQL-WP-6AF540D3C3E9` | `CQL-GAP-021` | Remote registration has the same missing broker contract. |
| `CQL-WP-21CE7759130B` | `CQL-GAP-028` | Grammar coverage must be reported as a parse/lower/execute/persist ladder. |
| `CQL-WP-BB5E82C7FCCB` | `CQL-GAP-013` | Event-log syntax is not yet a data foundation for executable process functions. |

No high-confidence partial was excluded as immaterial. Lower-confidence partials remain in the Phase 4 matrix and can enter later cycles if additional evidence raises their materiality.

## Deterministic construction and validation

`../tools/build_gap_register.py` owns the normalization definitions and derives evidence-ID unions from the Phase 4 inputs. It sorts gap rows and every semicolon-delimited ID set, writes UTF-8 CSV with fixed field order and LF line endings, and fails before writing when validation does not pass.

Run from the repository root:

```powershell
python docs/reconciliation/tools/build_gap_register.py
```

The builder validates:

- exactly 77 `conflict` claims, each mapped to exactly one gap;
- all 32 findings, each mapped to exactly one gap;
- all 15 high-confidence partial claims, each mapped to exactly one gap;
- stable syntax and source existence for every gap, claim, finding, capability, and probe ID;
- allowed classification, severity, confidence, and decision values;
- every decision-required disposition begins `awaiting-design-authority`;
- every required column is populated, except legitimate empty claim/probe ID lists.

Two consecutive runs must produce a byte-identical `gap_register.csv`. Any new Phase 4 conflict, finding, or high-confidence partial fails coverage validation until it receives an explicit normalization disposition.

## Phase 5 boundary

This artifact proposes queues and acceptance evidence; it does not adjudicate them. In particular, it does not choose temporal, THEN, scoring, window, lifecycle, provider, identity, security, versioning, or DDL semantics. Those questions remain visibly awaiting the named design authority and are suitable inputs to Phase 6 decision packets.
