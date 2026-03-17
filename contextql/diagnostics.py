"""Rich diagnostic formatting for ContextQL.

Implements Rust/Elm-style diagnostic output per WHITEPAPER Section 35.3:

    error[E102]: entity key type mismatch for 'supplier_risk'
     --> query:4:7
      |
    2 | FROM invoices i
      |      -------- table 'invoices' has key 'invoice_id' (INT64)
    4 |       supplier_risk)
      |       ^^^^^^^^^^^^^
      |
      = help: use CONTEXT ON to bind explicitly
      = note: this query involves two entity types
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass(slots=True)
class Span:
    """Source location span."""
    line: int
    column: int
    length: int = 1


@dataclass(slots=True)
class Annotation:
    """Inline source annotation (underline with message)."""
    span: Span
    message: str
    is_primary: bool = True  # ^^^^ vs ----


@dataclass(slots=True)
class Diagnostic:
    """A complete diagnostic message."""
    code: str
    severity: str  # error | warning | info
    message: str
    source: str = ""  # filename or "query"
    span: Optional[Span] = None
    annotations: list[Annotation] = field(default_factory=list)
    help: Optional[str] = None
    note: Optional[str] = None


def format_diagnostic(diag: Diagnostic, source_text: str = "") -> str:
    """Format a diagnostic into Rust/Elm-style output."""
    lines: list[str] = []

    # Header: error[E102]: entity key type mismatch
    lines.append(f"{diag.severity}[{diag.code}]: {diag.message}")

    # Location pointer
    if diag.span:
        origin = diag.source or "query"
        lines.append(f" --> {origin}:{diag.span.line}:{diag.span.column}")

    # Source annotations
    if source_text and (diag.annotations or diag.span):
        source_lines = source_text.splitlines()
        gutter_width = _gutter_width(source_lines, diag)
        lines.append(f"{' ' * gutter_width} |")

        annotated_lines = _collect_annotated_lines(diag)
        for line_num in sorted(annotated_lines):
            if 1 <= line_num <= len(source_lines):
                src = source_lines[line_num - 1]
                lines.append(f"{line_num:>{gutter_width}} | {src}")

                for ann in annotated_lines[line_num]:
                    marker = "^" if ann.is_primary else "-"
                    offset = ann.span.column - 1
                    underline = " " * offset + marker * ann.span.length
                    ann_line = f"{' ' * gutter_width} | {underline}"
                    if ann.message:
                        ann_line += f" {ann.message}"
                    lines.append(ann_line)

        lines.append(f"{' ' * gutter_width} |")

    # Help and note
    if diag.help:
        for help_line in diag.help.splitlines():
            lines.append(f"  = help: {help_line}")
    if diag.note:
        for note_line in diag.note.splitlines():
            lines.append(f"  = note: {note_line}")

    return "\n".join(lines)


def format_simple(diag: Diagnostic) -> str:
    """One-line format: file:line:col: severity[code]: message."""
    origin = diag.source or "query"
    loc = ""
    if diag.span:
        loc = f":{diag.span.line}:{diag.span.column}"
    return f"{origin}{loc}: {diag.severity}[{diag.code}]: {diag.message}"


def _gutter_width(source_lines: list[str], diag: Diagnostic) -> int:
    max_line = 1
    if diag.span:
        max_line = max(max_line, diag.span.line)
    for ann in diag.annotations:
        max_line = max(max_line, ann.span.line)
    return len(str(min(max_line, len(source_lines))))


def _collect_annotated_lines(diag: Diagnostic) -> dict[int, list[Annotation]]:
    result: dict[int, list[Annotation]] = {}
    for ann in diag.annotations:
        result.setdefault(ann.span.line, []).append(ann)
    if diag.span and diag.span.line not in result:
        result[diag.span.line] = []
    return result
