"""Run reproducible reconciliation probes against the pinned core/server pair.

The output is evidence, not a conformance oracle. Every probe records the
observed behavior without changing product state outside in-memory objects.
"""
from __future__ import annotations

import argparse
import importlib.metadata
import json
import platform
import re
import sys
from pathlib import Path
from typing import Any

import pandas as pd

import contextql as cql
from contextql.parser import ContextQLParser
from contextql.semantic import analyze_sql


ROOT = Path(__file__).resolve().parents[3]
SERVER_ROOT = ROOT.parents[1] / "contextql-server"


def outcome(callable_) -> dict[str, Any]:
    try:
        value = callable_()
        return {"ok": True, "value": value}
    except Exception as exc:  # evidence includes the exact observed failure
        return {
            "ok": False,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }


def parse_probe(parser: ContextQLParser, sql: str) -> dict[str, Any]:
    return outcome(lambda: parser.parse(sql).pretty())


def lower_probe(sql: str) -> dict[str, Any]:
    def run() -> list[str]:
        result = analyze_sql(sql)
        return [str(statement.kind) for statement in result.statements]

    return outcome(run)


def engine_probe(engine: cql.Engine, sql: str) -> dict[str, Any]:
    def run() -> dict[str, Any]:
        result = engine.execute(sql)
        return {
            "rows": result.row_count,
            "columns": result.columns,
            "records": result.to_pandas().to_dict(orient="records"),
        }

    return outcome(run)


def main() -> int:
    parser_arg = argparse.ArgumentParser()
    parser_arg.add_argument("--output", type=Path, required=True)
    args = parser_arg.parse_args()

    parser = ContextQLParser()
    engine = cql.Engine()
    engine.register_table(
        "facts",
        pd.DataFrame({"id": [1, 2, 3], "amount": [10, 20, 30]}),
        primary_key="id",
        primary_key_type="INT64",
    )
    engine.register_table("a_members", pd.DataFrame({"id": [1, 2]}))
    engine.register_table("b_members", pd.DataFrame({"id": [2, 3]}))
    engine.register_table(
        "events",
        pd.DataFrame(
            {
                "case_id": [1],
                "activity": ["created"],
                "event_time": ["2026-01-01T00:00:00Z"],
            }
        ),
    )
    engine.register_context(
        "context_a", "SELECT id FROM a_members", entity_key="id"
    )
    engine.register_context(
        "context_b", "SELECT id FROM b_members", entity_key="id"
    )

    statement_samples = {
        "select": "SELECT id FROM facts LIMIT 1;",
        "explain_context": "EXPLAIN CONTEXT context_a;",
        "create_event_log": (
            "CREATE EVENT LOG event_log FROM events ON case_id "
            "ACTIVITY activity TIMESTAMP event_time;"
        ),
        "create_process_model": (
            "CREATE PROCESS MODEL expected_flow FOR EVENT LOG event_log "
            "EXPECTED PATH ('created', 'completed');"
        ),
        "register_mcp": (
            "REGISTER MCP PROVIDER fraud ENDPOINT 'https://example.invalid' "
            "TRANSPORT HTTP ENTITY_TYPE invoice;"
        ),
        "grant": "GRANT QUERY ON CONTEXT context_a TO ROLE analyst;",
        "create_namespace": "CREATE NAMESPACE finance OWNER ROLE admin;",
        "set": "SET contextql.timeout = 30;",
    }

    probes: list[dict[str, Any]] = []

    def add(probe_id: str, claim: str, observed: dict[str, Any], expected: str) -> None:
        probes.append(
            {
                "probe_id": probe_id,
                "claim": claim,
                "expected_evidence": expected,
                "observed": observed,
            }
        )

    add(
        "P3-SQL-001",
        "A standard SELECT without FROM passes through unchanged.",
        parse_probe(parser, "SELECT 1;"),
        "Parse success would support the broad pass-through claim.",
    )
    add(
        "P3-PAR-001",
        "Malformed first input can be recovered so a later statement is parsed.",
        parse_probe(parser, "SELECT FROM; SELECT id FROM facts;"),
        "Recovery would return a tree containing the later valid statement.",
    )
    add(
        "P3-DDL-001",
        "The whitepaper composite-context example without ON is accepted.",
        parse_probe(
            parser,
            "CREATE CONTEXT combined AS COMPOSE (context_a, context_b) "
            "WITH STRATEGY UNION;",
        ),
        "Parse success would support the whitepaper example.",
    )
    add(
        "P3-DDL-002",
        "The grammar/spec composite-context form with ON is accepted.",
        parse_probe(
            parser,
            "CREATE CONTEXT combined ON id AS COMPOSE "
            "(context_a, context_b) WITH STRATEGY UNION;",
        ),
        "Parse success supports the current grammar/spec form.",
    )
    add(
        "P3-PRO-001",
        "The whitepaper arrow-chain process-model syntax is accepted.",
        parse_probe(
            parser,
            "CREATE PROCESS MODEL expected_flow AS 'created' -> 'completed';",
        ),
        "Parse success would support the whitepaper DDL reference.",
    )
    add(
        "P3-PRO-002",
        "The current EXPECTED PATH process-model syntax is accepted.",
        parse_probe(parser, statement_samples["create_process_model"]),
        "Parse success supports the grammar/spec form.",
    )

    for name, sql in statement_samples.items():
        add(
            f"P3-LAD-{len(probes) + 1:03d}",
            f"{name} has a dedicated semantic model after parsing.",
            lower_probe(sql),
            "A non-UNKNOWN statement kind supports semantic-surface maturity.",
        )
        add(
            f"P3-EXE-{len(probes) + 1:03d}",
            f"{name} executes through Engine.execute.",
            engine_probe(engine, sql),
            "Successful execution supports executable maturity.",
        )

    add(
        "P3-CTX-001",
        "THEN performs candidate-scoped or temporal staged evaluation distinct from intersection.",
        engine_probe(
            engine,
            "SELECT id FROM facts WHERE CONTEXT IN "
            "(context_a THEN context_b) ORDER BY id;",
        ),
        "A result distinguishable from set intersection is required for staged semantics.",
    )

    multi = lower_probe(
        "SELECT id FROM facts LIMIT 1; SELECT id FROM facts LIMIT 2;"
    )
    add(
        "P3-MUL-001",
        "Multiple parsed statements remain present after semantic lowering.",
        multi,
        "Two semantic statement kinds support multi-statement lowering.",
    )
    add(
        "P3-MUL-002",
        "Engine.execute executes all statements in a multi-statement input.",
        engine_probe(
            engine,
            "SELECT id FROM facts LIMIT 1; SELECT id FROM facts LIMIT 2;",
        ),
        "Evidence must show both results or an explicit multi-result contract.",
    )

    openapi_observed: dict[str, Any]
    try:
        sys.path.insert(0, str(SERVER_ROOT))
        from app.main import create_app

        app = create_app()
        schema = app.openapi()
        route_methods = []
        for path, operations in schema.get("paths", {}).items():
            for method in operations:
                route_methods.append(f"{method.upper()} {path}")
        openapi_observed = {
            "ok": True,
            "value": {
                "version": schema.get("info", {}).get("version"),
                "routes": sorted(route_methods),
            },
        }
    except Exception as exc:
        openapi_observed = {
            "ok": False,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
    add(
        "P3-API-001",
        "Generated OpenAPI represents the current public HTTP surface.",
        openapi_observed,
        "Generated routes and app version provide observed API evidence.",
    )

    decision_text = (ROOT / "DECISIONS.md").read_text(encoding="utf-8")
    decision_count = len(
        re.findall(r"^## (?:OQ|EQ|PQ|OPS|AD|GQ|DX|IM|CS)-\d+", decision_text, re.M)
    )
    add(
        "P3-DOC-001",
        "Decision-count metadata matches the actual decision register.",
        {"ok": True, "value": {"decision_headings": decision_count}},
        "The count can be compared with README/CLAUDE/status claims.",
    )

    result = {
        "environment": {
            "python": sys.version,
            "platform": platform.platform(),
            "contextql_package": importlib.metadata.version("contextql"),
            "contextql_server_package": importlib.metadata.version(
                "contextql-server"
            ),
            "pandas": importlib.metadata.version("pandas"),
            "duckdb": importlib.metadata.version("duckdb"),
            "pytest": importlib.metadata.version("pytest"),
        },
        "baselines": {
            "contextql": "a054c8fcc576f3913d98d664ddf71eeea56d9755",
            "contextql_server": "78c9565c33237a21dbf87f11d92ac6c7f29a846e",
        },
        "probes": probes,
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(f"wrote {len(probes)} probes to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
