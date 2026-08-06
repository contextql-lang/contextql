"""Build the Phase 4 claim-to-evidence traceability matrix.

The join is intentionally conservative. Broad chapter ranges establish only a
review relationship and therefore produce ``partial``/low-confidence joins.
Only individually reviewed rules can produce ``matched`` or ``conflict``.
No token-similarity or keyword score is used as evidence of semantic equality.
"""

from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable


ROOT = Path(__file__).resolve().parents[3]
CLAIMS_PATH = ROOT / "docs/reconciliation/phase1/claims.csv"
CORE_PATH = ROOT / "docs/reconciliation/phase2/core_capabilities.csv"
SERVER_PATH = ROOT / "docs/reconciliation/phase2/server_capabilities.csv"
PROBES_PATH = ROOT / "docs/reconciliation/phase3/probes.json"
OUTPUT_PATH = ROOT / "docs/reconciliation/phase4/traceability.csv"


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def source_start(claim: dict[str, str]) -> int:
    return int(claim["source_lines"].split("-", 1)[0])


def in_lines(start: int, end: int) -> Callable[[dict[str, str]], bool]:
    return lambda claim: start <= source_start(claim) <= end


def text_matches(pattern: str) -> Callable[[dict[str, str]], bool]:
    regex = re.compile(pattern, re.IGNORECASE)
    return lambda claim: bool(regex.search(claim["atomic_claim"]))


def exact_line(line: int) -> Callable[[dict[str, str]], bool]:
    return lambda claim: source_start(claim) == line


def all_of(*predicates: Callable[[dict[str, str]], bool]) -> Callable[[dict[str, str]], bool]:
    return lambda claim: all(predicate(claim) for predicate in predicates)


def not_text_matches(pattern: str) -> Callable[[dict[str, str]], bool]:
    regex = re.compile(pattern, re.IGNORECASE)
    return lambda claim: not regex.search(claim["atomic_claim"])


@dataclass(frozen=True)
class ScopeRule:
    name: str
    predicate: Callable[[dict[str, str]], bool]
    capabilities: tuple[str, ...]
    note: str


@dataclass(frozen=True)
class ReviewedRule:
    name: str
    predicate: Callable[[dict[str, str]], bool]
    capabilities: tuple[str, ...] = ()
    probes: tuple[str, ...] = ()
    status: str = "partial"
    confidence: str = "medium"
    note: str = ""


# These ranges are reviewed chapter/subsection relationships. They deliberately
# do not assert that an implementation capability satisfies every claim in the
# range. Their maximum result is partial/low-confidence until a ReviewedRule
# makes a narrower judgment.
SCOPE_RULES: tuple[ScopeRule, ...] = (
    ScopeRule("core constructs", in_lines(239, 339), ("CQL-CORE-CTX-001", "CQL-CORE-SCR-001", "CQL-CORE-TYP-001"), "Core entity/context/score constructs have related runtime and type-model evidence."),
    ScopeRule("type system", in_lines(341, 423), ("CQL-CORE-TYP-001", "CQL-CORE-SEM-001"), "The standalone type model exists, but many whitepaper coercion rules are delegated or not enforced."),
    ScopeRule("context algebra", in_lines(425, 489), ("CQL-CORE-CTX-002", "CQL-CORE-CTX-003", "CQL-CORE-SCR-001", "CQL-CORE-CMP-001"), "Executable algebra exists; staged THEN semantics and some algebraic properties need narrower evidence."),
    ScopeRule("context definitions", in_lines(491, 573), ("CQL-CORE-DDL-001", "CQL-CORE-DDL-002", "CQL-CORE-CMP-001", "CQL-CORE-PAR-003", "CQL-CORE-HIS-001", "CQL-CORE-OPT-001"), "Definition forms map to several independently mature surfaces."),
    ScopeRule("context query", in_lines(575, 633), ("CQL-CORE-CTX-002", "CQL-CORE-SCR-001", "CQL-CORE-QRY-001"), "Context filtering and score/count execution are reachable."),
    ScopeRule("ranking", in_lines(635, 720), ("CQL-CORE-SCR-001", "CQL-CORE-QRY-001"), "Ranking is executable, but not every documented strategy/tie-break rule is independently proven."),
    ScopeRule("windowing", in_lines(722, 745), ("CQL-CORE-WIN-001", "CQL-CORE-SCR-001"), "Window syntax executes; ordering and warning-code details require narrower checks."),
    ScopeRule("event logs", in_lines(747, 854), ("CQL-CORE-EVT-001", "CQL-CORE-PRC-001"), "Event-log syntax and lowering exist without an executable event-log catalog/runtime."),
    ScopeRule("operational analytics", in_lines(856, 1023), ("CQL-CORE-PRC-001", "CQL-CORE-EVT-001"), "Process syntax scaffolding is related evidence; function execution is not implemented as a subsystem."),
    ScopeRule("retrieval pipeline", in_lines(1025, 1096), ("CQL-CORE-PAR-001", "CQL-CORE-SEM-001", "CQL-CORE-QRY-001", "CQL-CORE-PUS-001", "CQL-CORE-BND-001"), "The reachable parser/lowerer/executor ladder implements a subset of the seven-stage design."),
    ScopeRule("physical operators", in_lines(1097, 1168), ("CQL-CORE-QRY-001", "CQL-CORE-BMP-001", "CQL-CORE-PUS-001", "CQL-CORE-SCR-001"), "Concrete runtime operations relate to the reference operators but do not establish a one-to-one physical plan."),
    ScopeRule("optimization", in_lines(1170, 1199), ("CQL-CORE-PUS-001", "CQL-CORE-BND-001", "CQL-CORE-QRY-001"), "Pushdown and boundedness are verified; the complete cost model is not."),
    ScopeRule("physical storage", in_lines(1201, 1254), ("CQL-CORE-BMP-001", "CQL-CORE-SNP-001", "CQL-CORE-HIS-001", "CQL-CORE-CAT-001", "CQL-SRV-BOOT-001", "CQL-SRV-PER-001", "CQL-SRV-PER-002", "CQL-SRV-PER-003", "CQL-SRV-PER-004"), "Bitmap/snapshot persistence is real; Arrow/Parquet tiers and all MVCC claims are not thereby proven."),
    ScopeRule("context operations", in_lines(1255, 1397), ("CQL-CORE-LIF-001", "CQL-CORE-DDL-001", "CQL-CORE-SNP-001", "CQL-CORE-AUD-001", "CQL-SRV-CAT-001", "CQL-SRV-CAT-002", "CQL-SRV-CAT-003", "CQL-SRV-CAT-004", "CQL-SRV-CAT-005", "CQL-SRV-CAT-006", "CQL-SRV-CAT-008", "CQL-SRV-SCH-001"), "Core/server lifecycle and refresh evidence covers a four-state subset, not the reference nine-state machine."),
    ScopeRule("freshness and maintenance", in_lines(1399, 1454), ("CQL-CORE-SNP-001", "CQL-CORE-HIS-001", "CQL-SRV-PER-004", "CQL-SRV-SCH-001"), "Atomic promotion, history, and scheduling are related to freshness; warning and DAG-cut contracts are separate."),
    ScopeRule("federation", in_lines(1456, 1587), ("CQL-CORE-MCP-001", "CQL-CORE-REM-001", "CQL-CORE-PRV-001", "CQL-CORE-BND-001", "CQL-SRV-PRO-001", "CQL-SRV-PRO-002", "CQL-SRV-PRO-003", "CQL-SRV-DS-001", "CQL-SRV-DS-002", "CQL-SRV-DS-003", "CQL-SRV-DS-004"), "Provider runtimes/connectors exist, while wire DDL and durable activation remain partial."),
    ScopeRule("identity", in_lines(1589, 1647), ("CQL-CORE-IDN-001", "CQL-SRV-ID-001", "CQL-SRV-ID-002", "CQL-SRV-BOOT-002"), "Exact path-pair maps are implemented; richer confidence/matching semantics are metadata only."),
    ScopeRule("security and governance", in_lines(1648, 1876), ("CQL-CORE-SEC-001", "CQL-CORE-AUD-001", "CQL-SRV-AUD-001", "CQL-SRV-AUD-002", "CQL-SRV-SEC-001"), "Syntax and audit events exist, but server authentication/RBAC/RLS/tenant enforcement does not."),
    ScopeRule("audit and lineage", in_lines(1781, 1800), ("CQL-CORE-TRC-001", "CQL-CORE-AUD-001", "CQL-SRV-AUD-001", "CQL-SRV-AUD-002"), "Query traces and audit records cover provenance metadata, but not the full hash-chain/lineage design."),
    ScopeRule("interpreter", in_lines(1877, 1936), ("CQL-CORE-PAR-001", "CQL-CORE-SEM-001", "CQL-CORE-QRY-001", "CQL-CORE-EXP-001", "CQL-CORE-ADP-001"), "The Python/DuckDB interpreter is integrated; the broader component diagram remains reference architecture."),
    ScopeRule("CLI", in_lines(1937, 2030), ("CQL-CORE-CLI-001",), "CLI/REPL evidence is executable, but watch mode is not included in the inventory capability."),
    ScopeRule("Python SDK", in_lines(2031, 2090), ("CQL-CORE-API-001", "CQL-CORE-RES-001", "CQL-CORE-BLD-001", "CQL-CORE-CTX-001"), "Public Python surfaces are integrated; async and configuration details need individual verification."),
    ScopeRule("Jupyter", in_lines(2092, 2115), ("CQL-CORE-JUP-001", "CQL-CORE-RES-001"), "The IPython extension is executable."),
    ScopeRule("diagnostics", in_lines(2196, 2278), ("CQL-CORE-ERR-001", "CQL-CORE-LNT-001", "CQL-CORE-RES-001"), "The registry, linter, renderer, and Result diagnostics exist; code assignments are not fully aligned."),
    ScopeRule("validation", in_lines(2281, 2365), ("CQL-CORE-DDL-001", "CQL-SRV-CAT-007", "CQL-SRV-CAT-009"), "VALIDATE and preview/history surfaces provide a subset of the proposed testing framework."),
    ScopeRule("connectivity", in_lines(2367, 2392), ("CQL-SRV-QRY-001", "CQL-SRV-QRY-002", "CQL-SRV-QRY-003", "CQL-SRV-QRY-004", "CQL-SRV-EXP-001", "CQL-SRV-EXP-002", "CQL-SRV-VER-001"), "A bounded unversioned REST API exists; gRPC/Arrow Flight/JDBC/ODBC are not evidenced."),
    ScopeRule("implementation strategy", in_lines(2394, 2438), ("CQL-CORE-PAR-001", "CQL-CORE-QRY-001", "CQL-CORE-ADP-001", "CQL-CORE-BMP-001", "CQL-CORE-LSP-001"), "The Python reference implementation exists; planned Rust/PyO3 work is not inferred from it."),
    ScopeRule("context DDL reference", in_lines(2442, 2486), ("CQL-CORE-DDL-001", "CQL-CORE-DDL-002", "CQL-CORE-CMP-001", "CQL-CORE-OPT-001"), "Context DDL is broadly executable, with form-specific differences retained for review."),
    ScopeRule("event DDL reference", in_lines(2488, 2498), ("CQL-CORE-EVT-001",), "Event-log DDL has parser/lowerer evidence but no executor path."),
    ScopeRule("process DDL reference", in_lines(2500, 2504), ("CQL-CORE-PRC-001",), "The current process-model grammar uses EXPECTED PATH, not the whitepaper arrow/variant form."),
    ScopeRule("security DDL reference", in_lines(2506, 2510), ("CQL-CORE-SEC-001", "CQL-SRV-SEC-001"), "Namespace syntax parses only and has no enforcement runtime."),
    ScopeRule("provider DDL reference", in_lines(2512, 2525), ("CQL-CORE-SEC-001", "CQL-CORE-MCP-001", "CQL-CORE-REM-001", "CQL-SRV-PRO-003"), "Provider registration parses but does not lower/execute or activate persisted records."),
    ScopeRule("SQL conformance", in_lines(2529, 2535), ("CQL-CORE-PAR-001", "CQL-CORE-PAR-002", "CQL-CORE-QRY-001", "CQL-CORE-ADP-001"), "The parser gates a SQL subset before DuckDB; broad unchanged pass-through is not supported."),
    ScopeRule("conclusion", in_lines(2579, 2589), ("CQL-CORE-CTX-002", "CQL-CORE-SCR-001", "CQL-CORE-BMP-001", "CQL-CORE-MCP-001", "CQL-CORE-REM-001", "CQL-CORE-BEN-001"), "The conclusion combines capabilities at materially different maturity levels."),
)


STATUS_LINE_RULES: dict[int, tuple[tuple[str, ...], str, str, str]] = {
    112: (("CQL-CORE-PAR-001", "CQL-CORE-PAR-002"), "partial", "medium", "Grammar exists and is tested, but the numeric statement-count/full-expression assertion is version-sensitive."),
    113: (("CQL-CORE-PAR-001",), "conflict", "high", "Parser exists, but P3-PAR-001 demonstrates abort-on-error rather than recovery to a later statement."),
    114: (("CQL-CORE-LNT-001",), "partial", "high", "The linter is integrated; the claimed rule count does not match the inventoried current rule set."),
    115: (("CQL-CORE-LNT-001",), "matched", "high", "Renderer/structured diagnostic behavior is directly inventoried and tested."),
    116: (("CQL-CORE-ERR-001",), "matched", "high", "The central error registry is directly inventoried and tested."),
    117: (("CQL-CORE-TYP-001",), "matched", "high", "Standalone type definitions are directly present; this does not imply full enforcement."),
    118: (("CQL-CORE-SEM-001",), "matched", "high", "Semantic models/lowering and in-memory catalog behavior are directly inventoried."),
    119: (("CQL-CORE-QRY-001", "CQL-CORE-SCR-001"), "matched", "high", "The hybrid DuckDB context filtering/scoring path is executable and tested."),
    120: (("CQL-CORE-ADP-001",), "matched", "high", "DuckDB is the sole executable adapter."),
    121: (("CQL-CORE-API-001", "CQL-CORE-RES-001"), "partial", "high", "Engine/ContextQL and Result are integrated; this row bundles several public symbols that should be verified individually."),
    122: (("CQL-CORE-MCP-001", "CQL-CORE-REM-001", "CQL-CORE-PRV-001"), "matched", "high", "Provider contracts and runtime paths are directly inventoried."),
    123: (("CQL-CORE-BLD-001",), "matched", "high", "QueryBuilder is executable and tested."),
    124: (("CQL-CORE-JUP-001",), "matched", "high", "The three IPython magic surfaces are directly inventoried."),
    125: (("CQL-CORE-CLI-001",), "matched", "high", "REPL, file, explain, and output modes are executable."),
    126: (("CQL-CORE-LSP-001",), "matched", "high", "The pygls server has integrated diagnostics/completion/hover/symbol evidence."),
    127: (("CQL-CORE-LSP-001",), "partial", "low", "The LSP capability is related; the inventory did not independently characterize the VS Code extension."),
    128: (("CQL-CORE-MCP-001", "CQL-CORE-REM-001", "CQL-CORE-PRV-001"), "partial", "high", "Runtime registration and execution exist; wire DDL remains parser-only."),
    132: (("CQL-CORE-BMP-001", "CQL-CORE-SNP-001", "CQL-SRV-PER-002"), "conflict", "high", "The whitepaper labels storage as specified, while bitmap/snapshot/persistence implementation is now verified/integrated; documentation understates maturity."),
    133: (("CQL-CORE-LIF-001", "CQL-SRV-CAT-004", "CQL-SRV-SCH-001"), "partial", "high", "Lifecycle/scheduling subsets exist, but not the nine-state reference architecture."),
    134: (("CQL-CORE-EVT-001", "CQL-CORE-PRC-001"), "matched", "high", "Reference-architecture labeling agrees with parser/lowerer-only M2 evidence."),
    138: (("CQL-CORE-SEC-001", "CQL-CORE-MCP-001", "CQL-CORE-REM-001"), "partial", "high", "Provider runtimes are integrated while registration DDL remains designed/parser-only."),
    139: (("CQL-CORE-IDN-001", "CQL-SRV-ID-001", "CQL-SRV-ID-002"), "conflict", "medium", "The document labels identity as designed, but exact path-pair identity is integrated; richer modes remain design-only."),
    140: (("CQL-CORE-SEC-001", "CQL-SRV-SEC-001"), "matched", "high", "Designed-only labeling is consistent with syntax scaffolding and absence of enforcement."),
}


REVIEWED_RULES: tuple[ReviewedRule, ...] = (
    ReviewedRule("parser recovery probe", exact_line(113), ("CQL-CORE-PAR-001",), ("P3-PAR-001",), "conflict", "high", "Malformed input aborts parsing; recovery does not continue to the later valid statement."),
    ReviewedRule("SELECT execution probe", exact_line(119), ("CQL-CORE-QRY-001",), ("P3-LAD-007", "P3-EXE-008"), "matched", "high", "The parse/lower/execute ladder confirms ordinary SELECT execution through the engine."),
    ReviewedRule("SQL pass-through probe", text_matches(r"standard SQL quer(?:y|ies).*pass through|pass through unchanged"), ("CQL-CORE-PAR-002", "CQL-CORE-QRY-001", "CQL-CORE-ADP-001"), ("P3-SQL-001",), "conflict", "high", "SELECT 1 fails before adapter execution because FROM is required; unchanged standard-SQL pass-through is too broad."),
    ReviewedRule("columnar adapter claim", all_of(exact_line(223), text_matches(r"DuckDB, Polars, and Arrow")), ("CQL-CORE-ADP-001", "CQL-CORE-RES-001"), (), "conflict", "high", "DuckDB is the sole executable adapter; Arrow/Polars are output conversions, not equivalent execution adapters."),
    ReviewedRule("performance overclaim", text_matches(r"millisecond-class|sub-microsecond"), ("CQL-CORE-BEN-001", "CQL-CORE-BMP-001"), (), "conflict", "high", "Committed evidence supports one scoped 10M scenario, not every broad latency/asymptotic claim."),
    ReviewedRule("O1 bitmap claim", text_matches(r"O\(1\).*membership"), ("CQL-CORE-BMP-001", "CQL-CORE-BEN-001"), (), "partial", "medium", "Bitmap membership is implemented, but end-to-end O(1) performance is not established by the recorded benchmark."),
    ReviewedRule("temporal semantics", text_matches(r"Temporal filters operate on temporal column values|AT qualifier filters by the temporal column"), ("CQL-CORE-HIS-001",), (), "conflict", "high", "Verified implementation follows SPEC/CS membership-history semantics, directly conflicting with the older column-filter statement."),
    ReviewedRule("score warning collision", text_matches(r"scores outside.*W100|W100.*score"), ("CQL-CORE-ERR-001", "CQL-CORE-LNT-001"), (), "conflict", "high", "Current registry uses W003 for score range and W100 for stale snapshots."),
    ReviewedRule("window warning collision", text_matches(r"W101: CONTEXT WINDOW|CONTEXT WINDOW.*W101"), ("CQL-CORE-WIN-001", "CQL-CORE-ERR-001"), (), "conflict", "high", "Current registry uses W001 for scoreless windows and W101 for failed refresh."),
    ReviewedRule("freshness warning collision", text_matches(r"\bW010\b|\bW012\b"), ("CQL-CORE-ERR-001", "CQL-CORE-SNP-001"), (), "conflict", "high", "W010/W012 are absent; current stale/failed-refresh warnings are W100/W101."),
    ReviewedRule("dependency warning gap", text_matches(r"\bW013\b"), ("CQL-CORE-ERR-001", "CQL-CORE-DDL-002"), (), "conflict", "high", "W013 is decision-only and absent from specification and implementation registry."),
    ReviewedRule("score scope code collision", text_matches(r"CONTEXT_SCORE\(\).*E111|Both are valid only.*E111"), ("CQL-CORE-ERR-001", "CQL-CORE-LNT-001"), (), "conflict", "high", "Current scope error is E108; E111 is score-expression type error."),
    ReviewedRule("nine-state lifecycle", text_matches(r"9-state|nine-state"), ("CQL-CORE-LIF-001", "CQL-SRV-CAT-004"), (), "conflict", "high", "Core/server expose four string states and do not enforce the documented nine-state machine."),
    ReviewedRule("lifecycle state rows", in_lines(1324, 1332), ("CQL-CORE-LIF-001", "CQL-SRV-CAT-004"), (), "conflict", "high", "Individual nine-state rows do not match the four-state executable lifecycle."),
    ReviewedRule("THEN staged semantics", text_matches(r"THEN.*only over|evaluates context B only|scoped.*THEN|THEN performs candidate"), ("CQL-CORE-CTX-003", "CQL-CORE-CTX-002"), ("P3-CTX-001",), "conflict", "medium", "Observed output equals intersection; the probe cannot establish distinct candidate-scoped or temporal staged evaluation."),
    ReviewedRule("THEN syntax", all_of(in_lines(463, 487), text_matches(r"THEN")), ("CQL-CORE-CTX-003", "CQL-CORE-CTX-002"), ("P3-CTX-001",), "partial", "high", "Syntax/lowering are executable; the probe does not prove all staged semantics or algebraic properties."),
    ReviewedRule("event-log DDL", text_matches(r"CREATE EVENT LOG"), ("CQL-CORE-EVT-001",), ("P3-LAD-011", "P3-EXE-012"), "partial", "high", "CREATE EVENT LOG parses/lowers but has no Engine.execute DDL path."),
    ReviewedRule("process-model DDL", text_matches(r"CREATE PROCESS MODEL"), ("CQL-CORE-PRC-001",), ("P3-PRO-001", "P3-PRO-002", "P3-LAD-013", "P3-EXE-014"), "conflict", "high", "Current EXPECTED PATH syntax differs from the whitepaper arrow/variant form and no executable process runtime is established."),
    ReviewedRule("process functions", in_lines(856, 1023), ("CQL-CORE-PRC-001", "CQL-CORE-EVT-001"), (), "conflict", "high", "Inventory negative evidence says process functions have no dedicated execution semantics beyond SQL reconstruction."),
    ReviewedRule("process deferrals", all_of(in_lines(856, 1023), text_matches(r"deferred to v2")), (), (), "unmatched", "none", "This is a future-intent boundary, not a shipped behavior claim; no implementation equivalence is inferred."),
    ReviewedRule("provider registration DDL", text_matches(r"Supported DDL form: REGISTER MCP|Supported DDL form: REGISTER REMOTE"), ("CQL-CORE-SEC-001", "CQL-CORE-MCP-001", "CQL-CORE-REM-001", "CQL-SRV-PRO-003"), ("P3-LAD-015", "P3-EXE-016"), "partial", "high", "Registration syntax parses but lowers to UNKNOWN, does not execute, and persisted records are not activated."),
    ReviewedRule("namespace DDL", text_matches(r"Supported DDL form: CREATE NAMESPACE"), ("CQL-CORE-SEC-001", "CQL-SRV-SEC-001"), ("P3-LAD-019", "P3-EXE-020"), "conflict", "high", "Namespace syntax parses but lowers to UNKNOWN and has no executable/enforced path."),
    ReviewedRule("security enforcement", all_of(in_lines(1648, 1876), text_matches(r"is enforced|are enforced|requires the .* privilege|fail-closed|is applied|maintains a hash-chained|tenant.*isolat|prevents"), not_text_matches(r"not supported in v1|deferred to v2")), ("CQL-CORE-SEC-001", "CQL-SRV-SEC-001", "CQL-SRV-AUD-002"), ("P3-LAD-017", "P3-EXE-018"), "conflict", "high", "Server inventory finds no authentication/RBAC/RLS/tenant enforcement and audit records are not hash chained."),
    ReviewedRule("security non-support boundary", all_of(in_lines(1648, 1876), text_matches(r"not supported in v1|deferred to v2")), ("CQL-SRV-SEC-001",), (), "matched", "medium", "The stated non-support boundary agrees with the inventory's absence of enforcement; this does not validate the surrounding future design."),
    ReviewedRule("REST versioning", all_of(in_lines(2367, 2384), text_matches(r"/v1/|/v2/|REST API")), ("CQL-SRV-QRY-001", "CQL-SRV-VER-001"), ("P3-API-001",), "conflict", "high", "OpenAPI proves an unversioned route surface and inconsistent application/package version metadata."),
    ReviewedRule("unsupported connectivity", text_matches(r"gRPC|Arrow Flight|JDBC|ODBC"), (), (), "unmatched", "none", "No core/server capability or targeted probe supports these connectivity claims at the pinned commits."),
    ReviewedRule("composite DDL reference", all_of(in_lines(2442, 2486), text_matches(r"AS COMPOSE")), ("CQL-CORE-CMP-001", "CQL-CORE-DDL-001"), ("P3-DDL-001", "P3-DDL-002"), "conflict", "high", "The whitepaper form without ON fails; the grammar/spec form with ON parses."),
    ReviewedRule("conformance surface", all_of(exact_line(2533), text_matches(r"grammar.*covers")), ("CQL-CORE-PAR-001", "CQL-CORE-PAR-002", "CQL-CORE-EVT-001", "CQL-CORE-PRC-001", "CQL-CORE-SEC-001"), ("P3-LAD-007", "P3-LAD-009", "P3-LAD-011", "P3-LAD-013", "P3-LAD-015", "P3-LAD-017", "P3-LAD-019", "P3-LAD-021", "P3-EXE-022"), "partial", "high", "Grammar acceptance is broad, but several statements lower to UNKNOWN and grammar does not prove execution."),
    ReviewedRule("LLM deferred", in_lines(2117, 2194), (), (), "unmatched", "none", "No implementation capability or executable probe covers LLM translation/synthesis; intended authority remains future-roadmap/decision material."),
    ReviewedRule("future directions", in_lines(2563, 2577), (), (), "unmatched", "none", "Future-intent claims are preserved without treating adjacent prototypes as roadmap approval."),
)


GLOSSARY_CAPABILITIES: dict[str, tuple[str, ...]] = {
    "AST": ("CQL-CORE-PAR-001", "CQL-CORE-SEM-001"),
    "Arrow": ("CQL-CORE-RES-001", "CQL-CORE-PRV-001"),
    "Context": ("CQL-CORE-CTX-001",),
    "Context algebra": ("CQL-CORE-CTX-002", "CQL-CORE-CTX-003", "CQL-CORE-SCR-001"),
    "Context score": ("CQL-CORE-SCR-001",),
    "Context window": ("CQL-CORE-WIN-001",),
    "DuckDB": ("CQL-CORE-ADP-001",),
    "Entity ID dictionary": ("CQL-CORE-IDN-001", "CQL-CORE-BMP-001"),
    "Identity map": ("CQL-CORE-IDN-001", "CQL-SRV-ID-001"),
    "Lark": ("CQL-CORE-PAR-001",),
    "MCP": ("CQL-CORE-MCP-001", "CQL-CORE-PRV-001"),
    "MVCC": ("CQL-CORE-SNP-001", "CQL-SRV-PER-004"),
    "ProcessTrace": ("CQL-CORE-PRC-001",),
    "REMOTE": ("CQL-CORE-REM-001", "CQL-CORE-PRV-001"),
    "Roaring bitmap": ("CQL-CORE-BMP-001",),
    "Snapshot": ("CQL-CORE-SNP-001", "CQL-SRV-PER-002"),
    "THEN chain": ("CQL-CORE-CTX-003",),
    "Vectorized execution": ("CQL-CORE-QRY-001", "CQL-CORE-ADP-001"),
}


def intended_authority(claim: dict[str, str]) -> str:
    maturity = claim["stated_maturity_or_target_version"].lower()
    domain = claim["domain"]
    claim_class = claim["claim_class"]
    if claim_class == "maturity-status":
        authority = "AUTH-010 implementation maturity"
    elif "future" in maturity or "v2" in maturity:
        authority = "AUTH-011 future intent"
    elif domain == "security-governance":
        authority = "AUTH-008 security and governance"
    elif domain == "storage-lifecycle":
        authority = "AUTH-006 persistence and lifecycle"
    elif domain == "federation-identity":
        authority = "AUTH-007 federation and identity"
    elif claim_class == "performance-target":
        authority = "AUTH-009 performance and scale"
    elif claim_class == "language-surface":
        authority = "AUTH-002 language syntax"
    elif domain == "language-semantics" or domain == "process-intelligence":
        authority = "AUTH-003 language semantics"
    elif domain == "developer-platform":
        authority = "AUTH-004 public Python/developer API; AUTH-005 for HTTP claims"
    else:
        authority = "AUTH-001 strategic purpose; AUTH-012 documentation status/examples"
    references = claim.get("corroborating_or_related_sources", "").strip()
    return f"{authority}; {references}" if references else authority


def main() -> None:
    claims = read_csv(CLAIMS_PATH)
    core = read_csv(CORE_PATH)
    server = read_csv(SERVER_PATH)
    probes_document = json.loads(PROBES_PATH.read_text(encoding="utf-8-sig"))
    capability_ids = {row["capability_id"] for row in core + server}
    probe_ids = {row["probe_id"] for row in probes_document["probes"]}

    rows: list[dict[str, str]] = []
    for claim in claims:
        capabilities: set[str] = set()
        probes: set[str] = set()
        methods: list[str] = []
        notes: list[str] = []
        status = "unmatched"
        confidence = "none"

        for rule in SCOPE_RULES:
            if rule.predicate(claim):
                capabilities.update(rule.capabilities)
                methods.append(f"curated-section-scope:{rule.name}")
                notes.append(rule.note)
        if capabilities:
            status = "partial"
            confidence = "low"

        line = source_start(claim)
        if line in STATUS_LINE_RULES:
            ids, status, confidence, note = STATUS_LINE_RULES[line]
            capabilities.update(ids)
            methods.append("reviewed-exact-status-line")
            notes.append(note)

        if claim["section"] == "Appendix A: Technical Glossary":
            term = claim["atomic_claim"].split(" --", 1)[0].strip()
            if term in GLOSSARY_CAPABILITIES:
                capabilities.update(GLOSSARY_CAPABILITIES[term])
                if status == "unmatched":
                    status, confidence = "partial", "low"
                methods.append("reviewed-glossary-term-link")
                notes.append("Glossary term is linked to a reviewed capability only as a related restatement; semantic equivalence is not inferred.")

        # Reviewed rules are ordered from broad to narrow. Later matching rules
        # may refine the disposition; conflict always dominates, and an explicit
        # unmatched rule can remove an over-broad scope association.
        for rule in REVIEWED_RULES:
            if not rule.predicate(claim):
                continue
            if rule.status == "unmatched":
                capabilities.clear()
                probes.clear()
            capabilities.update(rule.capabilities)
            probes.update(rule.probes)
            methods.append(f"reviewed-claim-rule:{rule.name}")
            notes.append(rule.note)
            if rule.status == "unmatched":
                status = "unmatched"
                confidence = rule.confidence
            elif rule.status == "conflict" or status != "conflict":
                status = rule.status
                confidence = rule.confidence

        unknown_capabilities = capabilities - capability_ids
        unknown_probes = probes - probe_ids
        if unknown_capabilities or unknown_probes:
            raise ValueError(f"unknown evidence IDs for {claim['claim_id']}: {unknown_capabilities}, {unknown_probes}")

        if not capabilities and not probes:
            status = "unmatched"
            confidence = "none"
            methods = methods or ["explicit-no-evidence-match"]
            notes = notes or ["No reviewed capability or executable probe directly addresses this claim at the pinned baselines."]

        # Phase 1's conflict note is preserved as intent/reference context. It is
        # not allowed to promote a join to conflict without Phase 2/3 evidence.
        if claim.get("potential_conflict_or_drift"):
            notes.append(f"Phase 1 risk: {claim['potential_conflict_or_drift']}")

        rows.append(
            {
                "claim_id": claim["claim_id"],
                "source_path": claim["source_path"],
                "source_lines": claim["source_lines"],
                "section": claim["section"],
                "atomic_claim": claim["atomic_claim"],
                "claim_class": claim["claim_class"],
                "domain": claim["domain"],
                "capability_ids": ";".join(sorted(capabilities)),
                "probe_ids": ";".join(sorted(probes)),
                "intended_authority_reference": intended_authority(claim),
                "join_status": status,
                "evidence_confidence": confidence,
                "match_method": ";".join(dict.fromkeys(methods)),
                "notes": " ".join(dict.fromkeys(notes)),
            }
        )

    if len(rows) != len(claims) or len({row["claim_id"] for row in rows}) != len(claims):
        raise ValueError("traceability output must contain exactly one row per unique claim")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    counts: dict[str, int] = {}
    for row in rows:
        counts[row["join_status"]] = counts.get(row["join_status"], 0) + 1
    print(f"wrote {len(rows)} rows to {OUTPUT_PATH}")
    print(" ".join(f"{key}={counts.get(key, 0)}" for key in ("matched", "partial", "conflict", "unmatched")))


if __name__ == "__main__":
    main()
