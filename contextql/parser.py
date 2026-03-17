"""ContextQL parser.

Wraps Lark to provide:
- stable parse API for CLI, language server, test harness, and semantic analyzer
- normalized syntax errors with source position tracking
- parse tree introspection
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from lark import Lark, Token, Tree, UnexpectedInput, UnexpectedToken, UnexpectedCharacters


GRAMMAR_PATH = Path(__file__).resolve().parents[1] / "grammar" / "contextql.lark"

# Friendly names for Lark terminal types
_FRIENDLY_NAMES: dict[str, str] = {
    "SELECT": "SELECT",
    "FROM": "FROM",
    "WHERE": "WHERE",
    "CONTEXT": "CONTEXT",
    "IN": "IN",
    "ON": "ON",
    "AS": "AS",
    "CREATE": "CREATE",
    "ORDER": "ORDER",
    "BY": "BY",
    "SEMICOLON": "';'",
    "LPAR": "'('",
    "RPAR": "')'",
    "COMMA": "','",
    "IDENTIFIER": "identifier",
    "STRING": "string literal",
    "INT": "integer",
    "NUMBER": "number",
    "STAR": "'*'",
}


@dataclass(slots=True)
class ParseErrorDetail:
    code: str
    message: str
    line: int
    column: int
    expected: list[str]
    context_snippet: str


class ContextQLSyntaxError(Exception):
    def __init__(self, detail: ParseErrorDetail):
        self.detail = detail
        super().__init__(detail.message)


class ContextQLParser:
    """Lark-based ContextQL parser.

    Use this in:
    - CLI (cql validate, cql parse)
    - language server (diagnostics, completion)
    - test harness
    - semantic analyzer pipeline (linter.py)
    """

    def __init__(self, grammar_path: Optional[Path] = None) -> None:
        grammar_path = grammar_path or GRAMMAR_PATH
        self.grammar_path = grammar_path
        self._parser = Lark.open(
            str(grammar_path),
            parser="earley",
            lexer="dynamic",
            maybe_placeholders=False,
            propagate_positions=True,
            keep_all_tokens=True,
            start="start",
        )

    def parse(self, text: str) -> Tree:
        """Parse ContextQL text and return a Lark Tree.

        Raises ContextQLSyntaxError on parse failure.
        """
        try:
            return self._parser.parse(text)
        except UnexpectedInput as exc:
            raise ContextQLSyntaxError(self._build_error(exc, text)) from exc

    def parse_file(self, path: str | Path) -> Tree:
        """Parse a .cql file from disk."""
        return self.parse(Path(path).read_text(encoding="utf-8"))

    def _build_error(self, exc: UnexpectedInput, text: str) -> ParseErrorDetail:
        line = getattr(exc, "line", 1) or 1
        column = getattr(exc, "column", 1) or 1
        expected_raw = sorted(getattr(exc, "expected", []) or [])
        expected = [_FRIENDLY_NAMES.get(e, e) for e in expected_raw]
        snippet = exc.get_context(text, span=60) if hasattr(exc, "get_context") else ""

        # Build a more descriptive error message
        if isinstance(exc, UnexpectedToken):
            token = getattr(exc, "token", None)
            if token:
                msg = f"Unexpected {token.type} '{token.value}'"
                if expected:
                    msg += f"; expected {', '.join(expected[:5])}"
                    if len(expected) > 5:
                        msg += f" (and {len(expected) - 5} more)"
            else:
                msg = "Unexpected token"
        elif isinstance(exc, UnexpectedCharacters):
            char = getattr(exc, "char", "?")
            msg = f"Unexpected character '{char}'"
            if expected:
                msg += f"; expected {', '.join(expected[:5])}"
        else:
            msg = "Syntax error"
            if expected:
                msg += f"; expected {', '.join(expected[:5])}"

        return ParseErrorDetail(
            code="E001",
            message=msg,
            line=line,
            column=column,
            expected=expected,
            context_snippet=snippet,
        )


def dump_tree(node: Any, indent: int = 0) -> str:
    """Pretty-print a parse tree for debugging."""
    pad = "  " * indent
    if isinstance(node, Tree):
        out = [f"{pad}Tree({node.data})"]
        for child in node.children:
            out.append(dump_tree(child, indent + 1))
        return "\n".join(out)
    if isinstance(node, Token):
        return f"{pad}Token({node.type}, {node.value!r})"
    return f"{pad}{node!r}"
