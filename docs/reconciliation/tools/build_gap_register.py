#!/usr/bin/env python3
"""Build and validate the Phase 5 normalized material-gap register.

The register intentionally stores material disposition units rather than one row
per whitepaper claim. Phase 4 conflict claims and findings are treated as frozen
inputs; a new or missing input makes generation fail instead of being silently
ignored.
"""

from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
RECON = ROOT / "docs" / "reconciliation"
TRACE_PATH = RECON / "phase4" / "traceability.csv"
FINDINGS_PATH = RECON / "phase4" / "high_risk_findings.csv"
OUTPUT_PATH = RECON / "phase5" / "gap_register.csv"

FIELDS = [
    "gap_id",
    "title",
    "claim_ids",
    "finding_ids",
    "capability_ids",
    "probe_ids",
    "gap_classification",
    "dimensions",
    "severity",
    "confidence",
    "current_state",
    "recommended_disposition",
    "closure_stream",
    "decision_required",
    "decision_packet_hint",
    "acceptance_evidence",
    "dependencies",
]

ID_RE = {
    "claim": re.compile(r"^CQL-WP-[0-9A-F]{12}$"),
    "finding": re.compile(r"^CQL-FND-HR-\d{3}$"),
    "capability": re.compile(r"^CQL-(?:CORE|SRV)-[A-Z]+-\d{3}$"),
    "probe": re.compile(r"^P3-[A-Z]+-\d{3}$"),
    "gap": re.compile(r"^CQL-GAP-\d{3}$"),
}


def split_ids(value: str) -> set[str]:
    return {part for part in value.split(";") if part}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def gap(
    number: int,
    title: str,
    finding_ids: str,
    classification: str,
    dimensions: str,
    severity: str,
    current_state: str,
    recommended_disposition: str,
    closure_stream: str,
    decision_required: str,
    decision_packet_hint: str,
    acceptance_evidence: str,
    dependencies: str = "none",
    *,
    extra_claim_ids: str = "",
    exclude_claim_ids: str = "",
    confidence: str = "high",
) -> dict[str, object]:
    return {
        "gap_id": f"CQL-GAP-{number:03d}",
        "title": title,
        "finding_ids": split_ids(finding_ids),
        "extra_claim_ids": split_ids(extra_claim_ids),
        "exclude_claim_ids": split_ids(exclude_claim_ids),
        "gap_classification": classification,
        "dimensions": dimensions,
        "severity": severity,
        "confidence": confidence,
        "current_state": current_state,
        "recommended_disposition": recommended_disposition,
        "closure_stream": closure_stream,
        "decision_required": decision_required,
        "decision_packet_hint": decision_packet_hint,
        "acceptance_evidence": acceptance_evidence,
        "dependencies": dependencies,
    }


GAPS = [
    gap(1, "Three-tier storage, isolation, and performance claims exceed evidence", "CQL-FND-HR-001", "V", "storage;performance;operations;narrative", "P1", "M3 for bitmap/snapshot paths; M0-M1 for warm/cold tiering and platform-wide isolation claims", "awaiting-design-authority: classify the three-tier model as current, committed target, or reference design; immediately bound published performance wording to reproducible evidence", "truth-repair;contract-repair;hardening", "yes", "What storage tiers and isolation guarantees are normative for the pinned release, and which exact benchmark claims may be published?", "Reproducible frozen benchmark; concurrent reader/refresh isolation probes; status-qualified storage architecture; documentation-to-evidence consistency check", "storage architecture decision; benchmark environment", extra_claim_ids="CQL-WP-7B607BB80EA6"),
    gap(2, "Grammar status conflates accepted syntax with executable language support", "CQL-FND-HR-002", "D", "syntax;runtime;devx;narrative", "P1", "M2 broad grammar surface with uneven M2-M4 lowering and execution", "Replace volatile counts and undifferentiated support claims with a generated parse/lower/analyze/execute/persist matrix", "truth-repair;hardening", "no", "none; this is factual maturity reporting", "Generated production count and statement ladder validated in CI; public status table distinguishes each maturity boundary"),
    gap(3, "Parser recovery is claimed but not implemented", "CQL-FND-HR-003", "D", "syntax;devx;narrative", "P1", "M4 normalized fail-fast diagnostics; no recovery contract", "awaiting-design-authority: choose editor/file recovery as a required capability or revise the claim to error reporting only", "truth-repair;contract-repair", "yes", "Must parsing recover after a malformed statement, and what synchronization boundary is required?", "Malformed-then-valid multi-statement fixture returns the later statement if recovery is chosen; otherwise all recovery wording is removed", "parser contract decision"),
    gap(4, "Linter rule inventory is stale and not generated", "CQL-FND-HR-004", "D", "semantics;devx;narrative", "P2", "M5 implemented linter with a stale hand-maintained count", "Generate the rule inventory from the canonical registry and map each normative rule to emitter and tests", "truth-repair;hardening", "no", "none; retain existing behavior and report it precisely", "Generated rule count/table matches runtime registration; each published rule has positive and negative tests"),
    gap(5, "Implemented status table hides capability-specific maturity", "CQL-FND-HR-005", "D", "api;runtime;devx;narrative", "P1", "M2-M5 capabilities grouped under one unqualified Implemented label", "Replace the bundled status list with capability-level M0-M6 labels, exact public symbols, negative boundaries, and current paths", "truth-repair;hardening", "no", "none; this is evidence-based status normalization", "Every listed component links to a current symbol and executable contract; stale paths and unsupported subcommands/features are absent or explicitly bounded"),
    gap(6, "Type system has duplicate authorities and no end-to-end enforcement", "CQL-FND-HR-006", "P", "semantics;types;runtime", "P1", "M2 type surfaces; integration explicitly TODO", "awaiting-design-authority: select the canonical type model and the v1 enforcement boundary before integrating or removing the duplicate", "contract-repair;vertical-slice-completion", "yes", "Which type representation is canonical, and which declaration/analyzer/executor boundaries must enforce it in v1?", "Declaration-to-executor type conformance fixtures; one canonical EntityKeyType; duplicate module removed or integrated", "type-system decision"),
    gap(7, "Transparent standard SQL pass-through is not supported", "CQL-FND-HR-007", "V", "syntax;semantics;compatibility;narrative", "P1", "M4 ContextQL SQL-like subset gated by Lark before DuckDB", "awaiting-design-authority: approve an explicit dialect and compatibility profile; revise pass-through claims to the measured subset", "truth-repair;contract-repair;hardening", "yes", "What SQL dialect/profile is promised, and are unsupported standard statements rejected, escaped to the adapter, or added incrementally?", "Positive and negative conformance suite by SQL feature/dialect; published profile and compatibility policy; SELECT without FROM has an intentional result", "SQL conformance decision"),
    gap(8, "Polars and Arrow outputs are described as execution targets", "CQL-FND-HR-008", "D", "execution;api;narrative", "P1", "M4 DuckDB execution adapter; M3 optional Polars/Arrow result conversion", "awaiting-design-authority: either retain Polars/Arrow adapters as planned work or describe them only as output formats", "truth-repair;contract-repair", "yes", "Are Polars and Arrow execution adapters committed capabilities or only result interoperability?", "Adapter contract tests for every claimed target, or whitepaper architecture consistently labels Polars/Arrow as result formats", "execution-target decision"),
    gap(9, "Diagnostic code ownership conflicts across authorities", "CQL-FND-HR-009;CQL-FND-HR-012", "C", "semantics;diagnostics;compatibility;devx", "P1", "M4 emitters exist, but stable public code identities conflict across whitepaper, SPEC, README, and registry", "awaiting-design-authority: establish one generated diagnostic registry with explicit supersession entries for W100/W003 and E108/E111", "contract-repair;hardening", "yes", "Which codes canonically identify score range and CONTEXT_SCORE scope errors, and how are conflicting published assignments superseded?", "Public API emits every canonical code; docs/SPEC/README validate against generated registry; supersession mapping is recorded", "diagnostic registry decision"),
    gap(10, "Score domain, null behavior, and strategy algebra lack one executable contract", "CQL-FND-HR-010;CQL-FND-HR-029", "S", "semantics;types;runtime", "P1", "M3 fixed accumulation behavior behind M2 syntax for multiple advertised strategies", "awaiting-design-authority: settle score domain, normalization, null/missing values, weighting, negation, aggregation formulas, and tie-breaking", "contract-repair;vertical-slice-completion;hardening", "yes", "What are the normative formulas and boundary behaviors for every score strategy and context-algebra operator?", "Golden matrix for every strategy across union/intersection/negation, missing and out-of-range scores, weights, ties, and snapshots", "score semantics decision; canonical type model"),
    gap(11, "Temporal qualifiers have incompatible meanings", "CQL-FND-HR-011", "V", "semantics;storage;compatibility", "P1", "M4 membership-history AT/BETWEEN behavior conflicts with temporal-column whitepaper semantics", "awaiting-design-authority: explicitly supersede one meaning or introduce distinct syntax for event-time filtering and membership-history queries", "contract-repair;hardening", "yes", "Do AT and BETWEEN filter definition columns, replay membership history, select snapshots, or require distinct operators?", "Normative fixtures distinguish the candidate models on identical data; retention and BETWEEN score behavior are specified; supersession is recorded", "temporal semantics decision"),
    gap(12, "Scoreless CONTEXT WINDOW legality, ordering, and code conflict", "CQL-FND-HR-013", "C", "semantics;diagnostics;devx;runtime", "P1", "M3 executable windowing with conflicting warning, legality, and deterministic-order contracts", "awaiting-design-authority: choose legal-with-warning versus invalid and define unscored truncation order; then align the diagnostic registry", "contract-repair;hardening", "yes", "Is a scoreless window legal, what ordering applies, and which canonical diagnostic identifies it?", "End-to-end fixtures on unsorted keys prove legality, ordering, ties, and emitted code; hover/SPEC/registry agree", "window semantics decision; diagnostic registry decision"),
    gap(13, "Analytical and process-intelligence semantics lack an executable runtime", "CQL-FND-HR-014", "I", "process;semantics;runtime;performance", "P1", "M1-M2 detailed function/process contracts and syntax scaffolding; no process execution subsystem", "awaiting-design-authority: classify analytical/process intelligence as normative v1, later committed work, or reference design before retaining executable and complexity claims", "truth-repair;contract-repair;vertical-slice-completion", "yes", "Which analytical and process functions are normative for the next release, and what is the minimum event-log/process-model vertical slice?", "Selected functions execute against declared event logs with null, complexity, conformance, persistence, composition, and negative fixtures; all others carry an explicit non-current state", "process scope decision; event-log DDL maturity", extra_claim_ids="CQL-WP-3DDEFC32743A;CQL-WP-6C2FBFB33076;CQL-WP-EB9C16129078;CQL-WP-592FD7422C0C;CQL-WP-A0461BEE69A5;CQL-WP-884179CDA117;CQL-WP-9A7287F9E5B7;CQL-WP-6564DF5E59FF;CQL-WP-0E6BAEAC9334;CQL-WP-95B19CA9EAB6;CQL-WP-AF0CEF665487;CQL-WP-6AAE1101C34B;CQL-WP-7664F5927A75;CQL-WP-379CFC1E7960;CQL-WP-DEEA6D64D68C;CQL-WP-293781313801;CQL-WP-CB219181F291;CQL-WP-D9726F61A7C4;CQL-WP-4BE824557591;CQL-WP-577D01DE1882;CQL-WP-8456FB60A256;CQL-WP-1AA9D0E7E560;CQL-WP-9C295B528B16;CQL-WP-41892100225A;CQL-WP-8D9A05AB3DB6;CQL-WP-1EEB5D6234B4;CQL-WP-A03C9942452A;CQL-WP-0E5BB26C9F00;CQL-WP-6EF8084DD356;CQL-WP-B066E922B6C2;CQL-WP-BB5E82C7FCCB"),
    gap(14, "OCEL forward-compatibility assurance is unproved", "CQL-FND-HR-015", "T", "process;compatibility;roadmap", "P2", "M1 explicit deferral; M2 current DDL shape; compatibility remains hypothetical", "awaiting-design-authority: retain deferral and classify forward-compatible as a requirement or aspiration", "truth-repair;contract-repair;hardening", "yes", "Is preserving an OCEL migration path a committed compatibility requirement for current event-log identifiers and catalog shapes?", "OCEL extension sketch plus migration/evolution fixture, or softened aspiration wording", "event-log contract; OCEL scope decision"),
    gap(15, "Nine-state lifecycle and query visibility are not enforced", "CQL-FND-HR-016", "V", "lifecycle;runtime;api;operations", "P1", "M3-M4 catalog state metadata and selected endpoints; no canonical nine-state FSM or query gate", "awaiting-design-authority: choose states, legal transitions, core/server create semantics, and query visibility before implementation or prose alignment", "contract-repair;vertical-slice-completion;hardening", "yes", "What is the canonical lifecycle FSM across core and server, including restart behavior and query visibility in every state?", "Transition matrix with invalid-transition, restart, concurrent refresh, and per-state query tests across core and HTTP", "lifecycle decision", extra_claim_ids="CQL-WP-0DE66027C15A;CQL-WP-2CC5775EC1F4;CQL-WP-74B0641FF237;CQL-WP-7916E6D4E504;CQL-WP-92AA67AD67CB;CQL-WP-05D01220C579;CQL-WP-93408FFA7C4D;CQL-WP-17EA58076442;CQL-WP-6171EA13A4C2"),
    gap(16, "Freshness vocabulary, diagnostics, and refresh policies conflict", "CQL-FND-HR-017", "V", "freshness;lifecycle;diagnostics;operations", "P1", "M3 stale_after and polling scheduler paths; documented max_staleness/strict policy branches and codes are absent", "awaiting-design-authority: adopt one freshness model and decide whether stale reads warn, trigger work, block, or fail", "contract-repair;vertical-slice-completion;hardening", "yes", "What are the canonical freshness thresholds, actions, diagnostic codes, dependency-version behavior, and restart guarantees?", "Clock-controlled stale/very-stale/failed-refresh/dependency-version tests across core/server and restart; canonical terms and codes generated into docs", "lifecycle decision; diagnostic registry decision"),
    gap(17, "Current-tense v1 streaming claims contradict explicit deferral", "CQL-FND-HR-018", "C", "streaming;operations;narrative", "P1", "M0-M1 generic streaming; M3 connector-specific change-feed synchronization", "Confirm deferral, separate connector incremental synchronization from generic streaming, and remove current-release assertions", "truth-repair;contract-repair", "no", "none; accepted evidence already establishes deferral unless design authority reopens scope", "All current-status prose labels generic streaming deferred; any future slice has delivery, watermark, replay, failure, and load contracts", "future streaming intake"),
    gap(18, "Detailed future prose leaks status without roadmap decisions", "CQL-FND-HR-019", "R", "roadmap;narrative", "P2", "M0 future directions with detailed present-tense architecture nearby", "awaiting-design-authority: classify each future item as committed, incubating, or illustrative; add sponsor, experiment, and target maturity only when promoted", "truth-repair;contract-repair", "yes", "Which future capabilities are committed roadmap outcomes versus research directions or illustrative architecture?", "Future sections visually separated; promoted items have problem, sponsor, decision state, dependency path, and falsifiable experiment", "roadmap authority"),
    gap(19, "Security, governance, audit, and multi-tenancy assurances are unenforced", "CQL-FND-HR-020", "V", "security;governance;audit;multi-tenancy;api", "P0", "M1 reference design and M2 parse/metadata/audit surfaces; no server-boundary auth, RBAC, RLS, tenant isolation, or tamper evidence", "awaiting-design-authority: immediately classify operational security/compliance prose as reference design and remove readiness assurances; require human security design authority before implementation claims resume", "truth-repair;contract-repair;vertical-slice-completion;hardening", "yes", "What is the approved threat model and minimum enforceable security boundary before any production or regulated-industry claim?", "Threat model; route authorization; cross-tenant abuse; RLS; credential-boundary; tamper-evident audit; privilege-denial tests; independent security review", "human security design authority", extra_claim_ids="CQL-WP-910E05A5DDDE;CQL-WP-D5C26877A0CD;CQL-WP-927698026F44;CQL-WP-1B0702467819"),
    gap(20, "Server explain executes instead of returning a dry plan", "CQL-FND-HR-021", "V", "api;security;semantics", "P1", "M4 executing trace endpoint conflicts with dry core explain and documented plan-only semantics", "awaiting-design-authority: require server explain to be dry or give execution/trace a distinct explicit endpoint and authorization policy", "contract-repair;vertical-slice-completion;hardening", "yes", "Must /query/explain be side-effect-free, and if trace execution remains, what endpoint name and authorization make that explicit?", "No-execution spies for adapter/provider/DDL paths; side-effect and authorization contract tests; OpenAPI documents behavior", "explain safety decision"),
    gap(21, "Provider registration, activation, and health do not form a runtime broker", "CQL-FND-HR-022", "P", "federation;runtime;operations;api", "P1", "M3 direct runtime registration and M4 persisted metadata; no persisted-provider activation or live health broker", "awaiting-design-authority: define provider factory, credential resolution, activation lifecycle, health semantics, and failure policy; align DDL/API status to that contract", "contract-repair;vertical-slice-completion;hardening", "yes", "How does a persisted or DDL-registered provider become an executable runtime after restart, and what does health mean?", "Restart and query an externally configured provider; bad credential, timeout, schema mismatch, disabled-state, and live-health fixtures", "provider activation decision; credential policy", extra_claim_ids="CQL-WP-35F33109BD23;CQL-WP-6AF540D3C3E9"),
    gap(22, "Exact path binding is described as global confidence-based identity resolution", "CQL-FND-HR-023", "V", "identity;federation;governance;narrative", "P1", "M4 deterministic table.column path mapping; M0-M1 canonical/probabilistic identity model", "awaiting-design-authority: rename/reframe the shipped mapping or approve a governed canonical identity model and confidence semantics", "truth-repair;contract-repair;vertical-slice-completion", "yes", "Is the supported capability deterministic path binding or global identity resolution, and are confidence/matching modes operational commitments?", "Cross-system key-type, ambiguity/confidence, provenance, lifecycle, privacy, and authorization fixtures appropriate to the chosen scope", "identity scope decision", extra_claim_ids="CQL-WP-0C45C94727B9"),
    gap(23, "Server and core release identity is contradictory", "CQL-FND-HR-024", "C", "api;packaging;compatibility", "P1", "M4 working pair with conflicting application, package, README, and core version identifiers", "awaiting-design-authority: choose the authoritative server version and compatibility policy, then generate every exposed identifier", "contract-repair;hardening", "yes", "Which server version is authoritative, and which core/server pairs are supported?", "CI assertion across package metadata, OpenAPI, startup audit, README, release tag, and published compatibility matrix", "release/version authority"),
    gap(24, "Context update accepts fields that are silently ignored", "CQL-FND-HR-025", "P", "api;semantics;compatibility", "P1", "M4 persistent update route with a request model broader than applied mutations", "awaiting-design-authority: define mutable fields and compatibility consequences; implement supported mutations and reject the rest with a stable error", "contract-repair;vertical-slice-completion;hardening", "yes", "Which context fields are mutable after creation, and what version/snapshot effects follow each mutation?", "One contract test per request field proves persisted and engine-observed change or explicit stable rejection", "context mutability decision; lifecycle decision"),
    gap(25, "Multi-statement parsing silently truncates execution", "CQL-FND-HR-026", "P", "api;execution;compatibility", "P1", "M4 parser/lowerer retain statements; public execute returns only the first result", "awaiting-design-authority: reject extras, execute transactionally, or return an explicit multi-result type", "contract-repair;vertical-slice-completion;hardening", "yes", "What is the public multi-statement execution, atomicity, error, and result contract across SDK and CLI?", "SELECT+DDL and DDL+failure atomicity probes; explicit multi-result or rejection tests; semicolon-in-literal CLI fixtures", "multi-statement decision"),
    gap(26, "Composite contexts and native parameters stop before execution", "CQL-FND-HR-027", "P", "semantics;runtime;persistence", "P1", "M2-M3 models and persistence; no composite materialization or native parameter validation/substitution", "awaiting-design-authority: approve composition/scoring and native binding semantics, or mark both surfaces designed", "contract-repair;vertical-slice-completion;hardening", "yes", "What are the v1 composition, score, refresh, and parameter binding/default/type/error semantics?", "Successful and negative create/query/refresh/restart tests for each strategy and parameter type/default/error case", "composition decision; score semantics; type-system decision"),
    gap(27, "THEN collapses to intersection instead of staged candidate-scoped evaluation", "CQL-FND-HR-028", "V", "semantics;runtime", "P1", "M3 syntax/lowering/execution with intersection-like behavior; staged semantics unproved and not enforced", "awaiting-design-authority: choose one normative meaning or introduce distinct operators for staged retrieval, temporal sequence, and intersection", "contract-repair;vertical-slice-completion;hardening", "yes", "What does THEN mean, including associativity, candidate propagation, score selection, side effects, and failures?", "Instrumented provider proves second-stage candidate scoping; associativity, non-commutativity, score, side-effect, and failure fixtures", "THEN semantics decision", extra_claim_ids="CQL-WP-A3F2E3E6B766"),
    gap(28, "Broad DDL grammar lacks parse-to-persistence completeness", "CQL-FND-HR-030", "P", "syntax;semantics;runtime;persistence", "P1", "M2 broad accepted grammar; several statements lower to UNKNOWN or lack execution and persistence", "awaiting-design-authority: classify each statement as normative v1, reserved syntax, or future; complete only approved vertical slices and report ladder maturity", "contract-repair;vertical-slice-completion;hardening", "yes", "Which event/process/provider/security/settings statements are executable v1 contracts versus reserved syntax?", "Machine-generated statement ladder requires parse, model, analyzer, executor, stable errors, persistence, and restart evidence before status elevation", "statement-surface decision; process/provider/security scope decisions", extra_claim_ids="CQL-WP-0D29D6A2491C;CQL-WP-CC3B700640DA", exclude_claim_ids="CQL-WP-CFB95979A8CF"),
    gap(29, "Core verification is import-order-sensitive", "CQL-FND-HR-031", "T", "verification;ci", "P2", "M5 product conversion path; full-suite green evidence is nondeterministic", "Repair optional-dependency discovery and rerun the entire frozen suite in clean dependency matrices", "hardening", "no", "none; test behavior is defective rather than semantically ambiguous", "Full frozen suite passes in clean environments with and without every optional result-format extra", "clean CI environments"),
    gap(30, "HTTP surface lacks a version and compatibility contract", "CQL-FND-HR-032", "D", "api;compatibility;narrative", "P1", "M4 persistent single-node API with 27 unversioned operations and incomplete README coverage", "awaiting-design-authority: choose route versioning and stability/deprecation policy; commit generated OpenAPI and document supported routes", "truth-repair;contract-repair;hardening", "yes", "Will the existing unversioned surface remain public, gain /v1 aliases, or migrate, and what compatibility/deprecation policy applies?", "Committed generated OpenAPI; compatibility diff in CI; examples for every supported public route; deprecation rules", "API versioning decision; server version identity", exclude_claim_ids="CQL-WP-506FAB66AF2A;CQL-WP-7DBCE3652446"),
]


def main() -> None:
    trace_rows = read_csv(TRACE_PATH)
    finding_rows = read_csv(FINDINGS_PATH)
    claims = {row["claim_id"]: row for row in trace_rows}
    findings = {row["finding_id"]: row for row in finding_rows}
    conflict_ids = {row["claim_id"] for row in trace_rows if row["join_status"] == "conflict"}
    high_partial_ids = {
        row["claim_id"]
        for row in trace_rows
        if row["join_status"] == "partial" and row["evidence_confidence"] == "high"
    }

    core_caps = {row["capability_id"] for row in read_csv(RECON / "phase2" / "core_capabilities.csv")}
    server_caps = {row["capability_id"] for row in read_csv(RECON / "phase2" / "server_capabilities.csv")}
    capability_ids = core_caps | server_caps
    with (RECON / "phase3" / "probes.json").open(encoding="utf-8") as handle:
        probe_ids = {item["probe_id"] for item in json.load(handle)["probes"]}

    output_rows: list[dict[str, str]] = []
    claim_to_gaps: defaultdict[str, list[str]] = defaultdict(list)
    finding_to_gaps: defaultdict[str, list[str]] = defaultdict(list)

    for definition in GAPS:
        gap_id = str(definition["gap_id"])
        if not ID_RE["gap"].fullmatch(gap_id):
            raise ValueError(f"invalid gap ID: {gap_id}")
        selected_findings = set(definition["finding_ids"])
        unknown_findings = selected_findings - findings.keys()
        if unknown_findings:
            raise ValueError(f"{gap_id}: unknown findings: {sorted(unknown_findings)}")

        selected_claims = set(definition["extra_claim_ids"])
        selected_caps: set[str] = set()
        selected_probes: set[str] = set()
        for finding_id in selected_findings:
            finding = findings[finding_id]
            selected_claims.update(split_ids(finding["claim_ids"]))
            selected_caps.update(split_ids(finding["capability_ids"]))
            selected_probes.update(ID_RE["probe"].findall(finding["evidence_ids"]))
            finding_to_gaps[finding_id].append(gap_id)
        selected_claims.difference_update(set(definition["exclude_claim_ids"]))

        unknown_claims = selected_claims - claims.keys()
        if unknown_claims:
            raise ValueError(f"{gap_id}: unknown claims: {sorted(unknown_claims)}")
        for claim_id in selected_claims:
            selected_caps.update(split_ids(claims[claim_id]["capability_ids"]))
            selected_probes.update(split_ids(claims[claim_id]["probe_ids"]))
            claim_to_gaps[claim_id].append(gap_id)

        for kind, values, known in (
            ("claim", selected_claims, claims.keys()),
            ("finding", selected_findings, findings.keys()),
            ("capability", selected_caps, capability_ids),
            ("probe", selected_probes, probe_ids),
        ):
            malformed = {value for value in values if not ID_RE[kind].fullmatch(value)}
            unknown = set(values) - set(known)
            if malformed or unknown:
                raise ValueError(
                    f"{gap_id}: invalid {kind} IDs; malformed={sorted(malformed)}, unknown={sorted(unknown)}"
                )

        row = {field: str(definition.get(field, "")) for field in FIELDS}
        row["claim_ids"] = ";".join(sorted(selected_claims))
        row["finding_ids"] = ";".join(sorted(selected_findings))
        row["capability_ids"] = ";".join(sorted(selected_caps))
        row["probe_ids"] = ";".join(sorted(selected_probes))
        output_rows.append(row)

    duplicate_gap_ids = [item for item, count in Counter(row["gap_id"] for row in output_rows).items() if count > 1]
    if duplicate_gap_ids:
        raise ValueError(f"duplicate gap IDs: {duplicate_gap_ids}")

    missing_conflicts = conflict_ids - claim_to_gaps.keys()
    duplicate_conflicts = {cid: gids for cid, gids in claim_to_gaps.items() if cid in conflict_ids and len(gids) != 1}
    if missing_conflicts or duplicate_conflicts:
        raise ValueError(
            f"conflict coverage failed; missing={sorted(missing_conflicts)}, duplicates={duplicate_conflicts}"
        )

    missing_findings = findings.keys() - finding_to_gaps.keys()
    duplicate_findings = {fid: gids for fid, gids in finding_to_gaps.items() if len(gids) != 1}
    if missing_findings or duplicate_findings:
        raise ValueError(
            f"finding coverage failed; missing={sorted(missing_findings)}, duplicates={duplicate_findings}"
        )

    missing_partials = high_partial_ids - claim_to_gaps.keys()
    duplicate_partials = {cid: gids for cid, gids in claim_to_gaps.items() if cid in high_partial_ids and len(gids) != 1}
    if missing_partials or duplicate_partials:
        raise ValueError(
            f"high-confidence partial coverage failed; missing={sorted(missing_partials)}, duplicates={duplicate_partials}"
        )

    allowed_classifications = set("ADISVTPORCU")
    allowed_severity = {"P0", "P1", "P2", "P3"}
    allowed_confidence = {"high", "medium", "low"}
    for row in output_rows:
        if row["gap_classification"] not in allowed_classifications:
            raise ValueError(f"{row['gap_id']}: bad classification")
        if row["severity"] not in allowed_severity:
            raise ValueError(f"{row['gap_id']}: bad severity")
        if row["confidence"] not in allowed_confidence:
            raise ValueError(f"{row['gap_id']}: bad confidence")
        if row["decision_required"] not in {"yes", "no"}:
            raise ValueError(f"{row['gap_id']}: bad decision_required")
        if row["decision_required"] == "yes" and not row["recommended_disposition"].startswith("awaiting-design-authority"):
            raise ValueError(f"{row['gap_id']}: unresolved decision lacks awaiting-design-authority disposition")
        # Reverse findings may legitimately have no claim IDs, and not every
        # material gap has a Phase 3 probe. The columns remain required.
        value_required = set(FIELDS) - {"claim_ids", "probe_ids"}
        if any(not row[field] for field in value_required):
            raise ValueError(f"{row['gap_id']}: empty required field")

    output_rows.sort(key=lambda row: row["gap_id"])
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(output_rows)

    print(
        f"wrote {len(output_rows)} gaps; covered {len(conflict_ids)} conflict claims, "
        f"{len(findings)} findings, and {len(high_partial_ids)} high-confidence partial claims"
    )


if __name__ == "__main__":
    main()
