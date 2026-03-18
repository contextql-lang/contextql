"""ContextQL Jupyter magic commands.

Load this extension in a Jupyter notebook with::

    %load_ext contextql

Then use::

    %cql_setup demo            # pre-load demo engine → stored as _cql_engine
    %cql_setup engine          # use 'engine' variable already in namespace
    %cql_contexts              # list tables and contexts

    %%cql                      # execute cell; result in _cql_result
    SELECT invoice_id FROM invoices WHERE CONTEXT IN (open_invoice)

    %%cql my_result            # execute cell; result stored as 'my_result'
    SELECT ...

The ``%%cql`` magic displays the resulting DataFrame inline and stores the
:class:`contextql.Result` object in the named variable (default ``_cql_result``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from IPython.core.interactiveshell import InteractiveShell


# ── magic implementation ──────────────────────────────────────────────────────


def _get_engine(ip: "InteractiveShell"):
    """Return the active engine stored as ``_cql_engine`` in the user namespace."""
    engine = ip.user_ns.get("_cql_engine")
    if engine is None:
        raise RuntimeError(
            "No ContextQL engine is active. "
            "Run '%cql_setup demo' or '%cql_setup engine_var' first."
        )
    return engine


def _cql_line_magic(ip: "InteractiveShell", line: str) -> None:
    """Handle ``%%cql`` line (variable name) — unused as line magic."""
    pass


def _cql_cell_magic(ip: "InteractiveShell", line: str, cell: str) -> None:
    """Execute a ContextQL query in a notebook cell.

    Usage::

        %%cql                  # result stored as _cql_result
        %%cql my_var           # result stored as my_var
    """
    import time

    var_name = line.strip() or "_cql_result"
    sql = cell.strip()
    if not sql:
        print("(empty query)")
        return

    engine = _get_engine(ip)

    try:
        t0 = time.monotonic()
        result = engine.execute(sql)
        elapsed = time.monotonic() - t0
        df = result.to_pandas()
    except Exception as exc:
        print(f"ContextQL error: {exc}")
        return

    ip.user_ns[var_name] = result

    try:
        from IPython.display import display
        display(df)
    except ImportError:
        print(df.to_string(index=False))

    rows = len(df)
    print(f"\n({rows} {'row' if rows == 1 else 'rows'}, {elapsed * 1000:.1f} ms)"
          f"  →  stored in '{var_name}'")


def _cql_setup_magic(ip: "InteractiveShell", line: str) -> None:
    """Set up the active ContextQL engine.

    Usage::

        %cql_setup demo           # create a demo engine
        %cql_setup engine         # use 'engine' from the notebook namespace
        %cql_setup my_engine_var  # use any variable from the notebook namespace
    """
    import contextql as cql

    arg = line.strip()
    if arg == "demo" or arg == "":
        print("Loading demo engine…", flush=True)
        engine = cql.demo()
        tables = engine.catalog.tables()
        contexts = engine.catalog.contexts()
        print(f"Tables   : {', '.join(tables)}")
        print(f"Contexts : {', '.join(contexts)}")
    else:
        engine = ip.user_ns.get(arg)
        if engine is None:
            print(f"Error: variable '{arg}' not found in namespace.")
            return
        if not isinstance(engine, cql.Engine):
            print(f"Error: '{arg}' is not a contextql.Engine instance.")
            return
        print(f"Using engine from '{arg}'.")

    ip.user_ns["_cql_engine"] = engine


def _cql_contexts_magic(ip: "InteractiveShell", line: str) -> None:
    """List the tables and contexts registered in the active engine.

    Usage::

        %cql_contexts
    """
    engine = _get_engine(ip)
    tables = engine.catalog.tables()
    contexts = engine.catalog.contexts()
    print(f"Tables   : {', '.join(tables) if tables else '(none)'}")
    print(f"Contexts : {', '.join(contexts) if contexts else '(none)'}")


# ── IPython extension entry point ─────────────────────────────────────────────


def load_ipython_extension(ip: "InteractiveShell") -> None:
    """Register ContextQL magics with IPython.

    Called automatically by ``%load_ext contextql``.
    """
    ip.register_magic_function(
        lambda line, cell: _cql_cell_magic(ip, line, cell),
        magic_kind="cell",
        magic_name="cql",
    )
    ip.register_magic_function(
        lambda line: _cql_setup_magic(ip, line),
        magic_kind="line",
        magic_name="cql_setup",
    )
    ip.register_magic_function(
        lambda line: _cql_contexts_magic(ip, line),
        magic_kind="line",
        magic_name="cql_contexts",
    )
    print("ContextQL magic loaded. Use %cql_setup demo to get started.")
