"""ContextQL CLI — interactive REPL and query runner.

Entry point: ``cql``

Usage::

    cql                        # interactive REPL (bare engine)
    cql demo                   # interactive REPL with pre-loaded demo data
    cql demo --file query.cql  # run a .cql file against demo data
    cql --file query.cql       # run a .cql file against a bare engine
    cql explain "SELECT ..."   # print the query plan
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Optional

try:
    import readline  # noqa: F401 — enables arrow-key navigation and history in REPL
except ImportError:
    pass


# ============================================================
# Output formatting
# ============================================================


def _print_result(df, output_format: str = "table") -> None:
    if df.empty:
        print("(0 rows)")
        return

    if output_format == "json":
        print(df.to_json(orient="records", indent=2))
    elif output_format == "csv":
        print(df.to_csv(index=False), end="")
    else:
        print(df.to_string(index=False))


# ============================================================
# REPL
# ============================================================


def _run_repl(engine, output_format: str = "table") -> None:
    """Run the interactive ContextQL REPL until the user quits."""
    print("ContextQL interactive shell  (type \\q or Ctrl-D to quit)")
    print("  \\d   list tables and contexts")
    print("  \\q   quit")
    print("  Submit a query with a trailing semicolon.")
    print()

    buf: list[str] = []

    while True:
        prompt = "cql> " if not buf else "  -> "
        try:
            line = input(prompt)
        except EOFError:
            print()
            break
        except KeyboardInterrupt:
            print()
            buf = []
            continue

        stripped = line.strip()

        # ── Meta-commands ──────────────────────────────────────────────────
        if stripped == r"\q":
            break

        if stripped == r"\d":
            tables = engine.catalog.tables()
            contexts = engine.catalog.contexts()
            print(f"Tables   : {', '.join(tables) if tables else '(none)'}")
            print(f"Contexts : {', '.join(contexts) if contexts else '(none)'}")
            buf = []
            continue

        # ── Accumulate input until semicolon ───────────────────────────────
        buf.append(line)
        joined = " ".join(buf)

        if ";" not in joined:
            continue

        sql = joined.strip()
        buf = []

        if not sql:
            continue

        # ── Execute ────────────────────────────────────────────────────────
        try:
            t0 = time.monotonic()
            result = engine.execute(sql)
            elapsed = time.monotonic() - t0
            df = result.to_pandas()
            _print_result(df, output_format)
            print(f"\n({len(df)} {'row' if len(df) == 1 else 'rows'}, "
                  f"{elapsed * 1000:.1f} ms)")
        except Exception as exc:
            print(f"Error: {exc}", file=sys.stderr)


# ============================================================
# File execution
# ============================================================


def _run_file(engine, path: str, output_format: str = "table") -> None:
    """Execute all ContextQL statements in *path* against *engine*."""
    try:
        with open(path) as fh:
            sql = fh.read()
    except OSError as exc:
        print(f"Cannot open file: {exc}", file=sys.stderr)
        sys.exit(1)

    statements = [s.strip() for s in sql.split(";") if s.strip()]

    for stmt in statements:
        full = stmt + ";"
        try:
            t0 = time.monotonic()
            result = engine.execute(full)
            elapsed = time.monotonic() - t0
            df = result.to_pandas()
            _print_result(df, output_format)
            print(f"\n({len(df)} {'row' if len(df) == 1 else 'rows'}, "
                  f"{elapsed * 1000:.1f} ms)\n")
        except Exception as exc:
            print(f"Error in statement:\n  {full}\n{exc}", file=sys.stderr)
            sys.exit(1)


# ============================================================
# Entry point
# ============================================================


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        prog="cql",
        description="ContextQL interactive shell and query runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  cql                        interactive REPL
  cql demo                   REPL pre-loaded with demo datasets
  cql demo --file query.cql  run query.cql against demo datasets
  cql --file query.cql       run query.cql against a bare engine
  cql explain "SELECT ..."   print query plan
        """,
    )
    parser.add_argument(
        "subcommand",
        nargs="?",
        choices=["demo", "explain"],
        help="'demo' loads sample data; 'explain' prints a query plan",
    )
    parser.add_argument(
        "query",
        nargs="?",
        metavar="QUERY",
        help="ContextQL query (used with 'explain')",
    )
    parser.add_argument(
        "--file", "-f",
        metavar="FILE",
        help="execute all statements from FILE instead of opening a REPL",
    )
    parser.add_argument(
        "--output", "-o",
        choices=["table", "json", "csv"],
        default="table",
        metavar="FORMAT",
        help="output format: table (default), json, csv",
    )

    args = parser.parse_args(argv)

    try:
        import contextql as cql
    except ImportError:
        print("Error: contextql package is not installed.", file=sys.stderr)
        sys.exit(1)

    # ── explain ───────────────────────────────────────────────────────────────
    if args.subcommand == "explain":
        if not args.query:
            parser.error("'explain' requires a QUERY argument")
        engine = cql.Engine()
        print(engine.explain(args.query))
        return

    # ── demo ──────────────────────────────────────────────────────────────────
    if args.subcommand == "demo":
        print("Loading demo engine…", flush=True)
        engine = cql.demo()
        tables = engine.catalog.tables()
        contexts = engine.catalog.contexts()
        print(f"Tables   : {', '.join(tables)}")
        print(f"Contexts : {', '.join(contexts)}")
        print()
        if args.file:
            _run_file(engine, args.file, args.output)
        else:
            _run_repl(engine, args.output)
        return

    # ── bare engine ───────────────────────────────────────────────────────────
    engine = cql.Engine()
    if args.file:
        _run_file(engine, args.file, args.output)
    else:
        _run_repl(engine, args.output)


if __name__ == "__main__":
    main()
