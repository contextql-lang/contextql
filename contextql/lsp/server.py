"""ContextQL Language Server (pygls v2).

Provides real-time diagnostics, completions, hover, and document symbols
for .cql files by wiring to the ContextQL parser and linter.
"""
from __future__ import annotations

import logging
import re

from lsprotocol import types
from pygls.lsp.server import LanguageServer
from pygls.workspace import TextDocument

from ..linter import Catalog, ContextQLLinter, LintDiagnostic

logger = logging.getLogger(__name__)

# ── Severity mapping ──────────────────────────────────────────────────

_SEVERITY_MAP = {
    "error": types.DiagnosticSeverity.Error,
    "warning": types.DiagnosticSeverity.Warning,
    "info": types.DiagnosticSeverity.Information,
}

# ── ContextQL keywords for completions ────────────────────────────────

_KEYWORD_COMPLETIONS = [
    types.CompletionItem(label="SELECT", kind=types.CompletionItemKind.Keyword),
    types.CompletionItem(label="FROM", kind=types.CompletionItemKind.Keyword),
    types.CompletionItem(label="WHERE", kind=types.CompletionItemKind.Keyword),
    types.CompletionItem(label="CONTEXT IN", kind=types.CompletionItemKind.Keyword,
                         insert_text="CONTEXT IN (${1:context_name})",
                         insert_text_format=types.InsertTextFormat.Snippet),
    types.CompletionItem(label="CONTEXT ON", kind=types.CompletionItemKind.Keyword,
                         insert_text="CONTEXT ON ${1:alias} IN (${2:context_name})",
                         insert_text_format=types.InsertTextFormat.Snippet),
    types.CompletionItem(label="CREATE CONTEXT", kind=types.CompletionItemKind.Keyword,
                         insert_text="CREATE CONTEXT ${1:name} ON ${2:entity_key} AS\n  ${3:SELECT}",
                         insert_text_format=types.InsertTextFormat.Snippet),
    types.CompletionItem(label="ORDER BY CONTEXT", kind=types.CompletionItemKind.Keyword),
    types.CompletionItem(label="CONTEXT_SCORE()", kind=types.CompletionItemKind.Function),
    types.CompletionItem(label="CONTEXT_COUNT()", kind=types.CompletionItemKind.Function),
    types.CompletionItem(label="WITH CONTEXT WINDOW", kind=types.CompletionItemKind.Keyword,
                         insert_text="WITH CONTEXT WINDOW ${1:100}",
                         insert_text_format=types.InsertTextFormat.Snippet),
    types.CompletionItem(label="JOIN", kind=types.CompletionItemKind.Keyword),
    types.CompletionItem(label="LEFT JOIN", kind=types.CompletionItemKind.Keyword),
    types.CompletionItem(label="GROUP BY", kind=types.CompletionItemKind.Keyword),
    types.CompletionItem(label="ORDER BY", kind=types.CompletionItemKind.Keyword),
    types.CompletionItem(label="HAVING", kind=types.CompletionItemKind.Keyword),
    types.CompletionItem(label="LIMIT", kind=types.CompletionItemKind.Keyword),
    types.CompletionItem(label="CREATE EVENT LOG", kind=types.CompletionItemKind.Keyword),
    types.CompletionItem(label="CREATE PROCESS MODEL", kind=types.CompletionItemKind.Keyword),
    types.CompletionItem(label="SHOW CONTEXTS", kind=types.CompletionItemKind.Keyword),
    types.CompletionItem(label="DESCRIBE CONTEXT", kind=types.CompletionItemKind.Keyword),
    types.CompletionItem(label="REFRESH CONTEXT", kind=types.CompletionItemKind.Keyword),
    types.CompletionItem(label="VALIDATE CONTEXT", kind=types.CompletionItemKind.Keyword),
]

# ── Hover documentation ──────────────────────────────────────────────

_HOVER_DOCS: dict[str, str] = {
    "CONTEXT": "**CONTEXT** — First-class query primitive representing an operational situation (risk condition, process anomaly, compliance violation).",
    "CONTEXT_SCORE": "**CONTEXT_SCORE()** — Returns the relevance score [0.0, 1.0] of the matched context for the current row. Requires `WHERE CONTEXT IN (...)`.",
    "CONTEXT_COUNT": "**CONTEXT_COUNT()** — Returns the number of contexts matched for the current row. Requires `WHERE CONTEXT IN (...)`.",
    "CONTEXT_WINDOW": "**WITH CONTEXT WINDOW *n*** — Limits results to the top *n* entities by context score. Requires scored contexts.",
    "THROUGHPUT_TIME_BETWEEN": "**THROUGHPUT_TIME_BETWEEN(event_log, activity_a, activity_b)** — Computes elapsed time between two process activities for each case.",
    "WEIGHT": "**WEIGHT *n*** — Assigns a relative weight to a context reference for score combination. Must be non-negative.",
    "TEMPORAL": "**TEMPORAL(column, granularity)** — Declares a context as time-varying, enabling `AT` and `BETWEEN` temporal qualifiers.",
    "COMPOSE": "**COMPOSE(ctx1, ctx2, ...) WITH STRATEGY** — Creates a composite context from multiple contexts using UNION, INTERSECT, or WEIGHTED strategy.",
    "SCORE": "**SCORE *expr*** — Defines the scoring expression for a context definition. Values should be in [0.0, 1.0].",
}

# ── Diagnostic conversion ────────────────────────────────────────────


def lint_to_lsp_diagnostics(diags: list[LintDiagnostic]) -> list[types.Diagnostic]:
    """Convert LintDiagnostic list to LSP Diagnostic list.

    Handles the 1-indexed → 0-indexed coordinate conversion.
    """
    result: list[types.Diagnostic] = []
    for d in diags:
        line = max(d.line - 1, 0)
        col = max(d.column - 1, 0)
        message = d.message
        if d.suggestion:
            message = f"{d.message}\n  Suggestion: {d.suggestion}"
        result.append(types.Diagnostic(
            range=types.Range(
                start=types.Position(line=line, character=col),
                end=types.Position(line=line, character=col + 1),
            ),
            message=message,
            severity=_SEVERITY_MAP.get(d.severity, types.DiagnosticSeverity.Error),
            code=d.rule_id,
            source="contextql",
        ))
    return result


# ── Server ────────────────────────────────────────────────────────────

server = LanguageServer("contextql-lsp", "v0.1")

# Default empty catalog — users can extend this via configuration later
_catalog = Catalog()
_linter = ContextQLLinter(_catalog)

_CREATE_PATTERN = re.compile(
    r"^\s*CREATE\s+(CONTEXT|EVENT\s+LOG|PROCESS\s+MODEL)\s+(\w+)",
    re.IGNORECASE | re.MULTILINE,
)

_SYMBOL_KIND_MAP = {
    "CONTEXT": types.SymbolKind.Class,
    "EVENT LOG": types.SymbolKind.Event,
    "PROCESS MODEL": types.SymbolKind.Module,
}


def _validate(ls: LanguageServer, doc: TextDocument) -> None:
    """Run the linter and publish diagnostics."""
    diags = _linter.lint(doc.source)
    lsp_diags = lint_to_lsp_diagnostics(diags)
    ls.text_document_publish_diagnostics(
        types.PublishDiagnosticsParams(
            uri=doc.uri,
            version=doc.version,
            diagnostics=lsp_diags,
        )
    )


@server.feature(types.TEXT_DOCUMENT_DID_OPEN)
def did_open(params: types.DidOpenTextDocumentParams) -> None:
    doc = server.workspace.get_text_document(params.text_document.uri)
    _validate(server, doc)


@server.feature(types.TEXT_DOCUMENT_DID_CHANGE)
def did_change(params: types.DidChangeTextDocumentParams) -> None:
    doc = server.workspace.get_text_document(params.text_document.uri)
    _validate(server, doc)


@server.feature(types.TEXT_DOCUMENT_DID_CLOSE)
def did_close(params: types.DidCloseTextDocumentParams) -> None:
    server.text_document_publish_diagnostics(
        types.PublishDiagnosticsParams(
            uri=params.text_document.uri,
            diagnostics=[],
        )
    )


@server.feature(types.TEXT_DOCUMENT_COMPLETION, types.CompletionOptions(
    trigger_characters=[" ", "("],
))
def completions(params: types.CompletionParams) -> types.CompletionList:
    items = list(_KEYWORD_COMPLETIONS)
    # Add catalog context names as completions
    for name in _catalog.context_names():
        items.append(types.CompletionItem(
            label=name,
            kind=types.CompletionItemKind.Variable,
            detail="context",
        ))
    return types.CompletionList(is_incomplete=False, items=items)


@server.feature(types.TEXT_DOCUMENT_HOVER)
def hover(params: types.HoverParams) -> types.Hover | None:
    doc = server.workspace.get_text_document(params.text_document.uri)
    lines = doc.source.split("\n")
    line_idx = params.position.line
    if line_idx >= len(lines):
        return None

    line = lines[line_idx]
    col = params.position.character

    # Extract word at cursor position
    word_start = col
    while word_start > 0 and (line[word_start - 1].isalnum() or line[word_start - 1] == "_"):
        word_start -= 1
    word_end = col
    while word_end < len(line) and (line[word_end].isalnum() or line[word_end] == "_"):
        word_end += 1
    word = line[word_start:word_end].upper()

    # Check two-word keywords (CONTEXT WINDOW, etc.)
    if word == "CONTEXT" and word_end < len(line):
        rest = line[word_end:].lstrip()
        if rest.upper().startswith("WINDOW"):
            word = "CONTEXT_WINDOW"

    content = _HOVER_DOCS.get(word)

    # Check catalog for context name hover (case-insensitive)
    if content is None:
        original_word = line[word_start:word_end]
        ctx = _catalog.get_context(original_word)
        if ctx:
            parts = [f"**Context: `{ctx.name}`**"]
            parts.append(f"- Entity key: `{ctx.entity_key}` ({ctx.entity_key_type})")
            if ctx.has_score:
                parts.append("- Scored: yes")
            if ctx.is_temporal:
                parts.append("- Temporal: yes")
            if ctx.parameters:
                parts.append(f"- Parameters: {', '.join(ctx.parameters)}")
            content = "\n".join(parts)

    if content is None:
        return None

    return types.Hover(
        contents=types.MarkupContent(
            kind=types.MarkupKind.Markdown,
            value=content,
        )
    )


@server.feature(types.TEXT_DOCUMENT_DOCUMENT_SYMBOL)
def document_symbols(params: types.DocumentSymbolParams) -> list[types.DocumentSymbol]:
    doc = server.workspace.get_text_document(params.text_document.uri)
    symbols: list[types.DocumentSymbol] = []

    for match in _CREATE_PATTERN.finditer(doc.source):
        kind_key = match.group(1).upper()
        # Normalize "EVENT LOG" and "PROCESS MODEL"
        if "LOG" in kind_key:
            kind_key = "EVENT LOG"
        elif "MODEL" in kind_key:
            kind_key = "PROCESS MODEL"

        name = match.group(2)
        line = doc.source[:match.start()].count("\n")
        end_line = doc.source[:match.end()].count("\n")
        sym_kind = _SYMBOL_KIND_MAP.get(kind_key, types.SymbolKind.Function)

        symbols.append(types.DocumentSymbol(
            name=name,
            kind=sym_kind,
            range=types.Range(
                start=types.Position(line=line, character=0),
                end=types.Position(line=end_line, character=len(match.group(0))),
            ),
            selection_range=types.Range(
                start=types.Position(line=line, character=match.start(2) - match.start()),
                end=types.Position(line=line, character=match.end(2) - match.start()),
            ),
        ))

    return symbols


def start() -> None:
    """Entry point for the contextql-lsp command."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    server.start_io()


if __name__ == "__main__":
    start()
