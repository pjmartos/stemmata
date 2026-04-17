"""JSON loading with position tracking for prompt documents."""
from __future__ import annotations

import json
import re
from typing import Any

from stemmata.errors import SchemaError
from stemmata.yaml_loader import _ScalarStr


_NUMBER_RE = re.compile(r"-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?")


def _offset_to_line_col(text: str, offset: int) -> tuple[int, int]:
    """Convert a character offset to 1-based ``(line, column)``."""
    line = text.count("\n", 0, offset) + 1
    last_nl = text.rfind("\n", 0, offset)
    col = offset - last_nl  # 1-based because rfind returns -1 when absent
    return line, col


class _JsonParser:
    """Recursive-descent JSON parser that wraps strings in ``_ScalarStr``."""

    __slots__ = ("text", "pos", "file", "positions")

    def __init__(self, text: str, file: str):
        self.text = text
        self.pos = 0
        self.file = file
        self.positions: dict[int, tuple[int, int, bool]] = {}

    # ------------------------------------------------------------------
    # public
    # ------------------------------------------------------------------

    def parse(self) -> Any:
        self._skip_ws()
        if self.pos >= len(self.text):
            return None
        value = self._parse_value()
        self._skip_ws()
        if self.pos < len(self.text):
            line, col = _offset_to_line_col(self.text, self.pos)
            raise SchemaError(
                f"JSON parse error in {self.file}: unexpected content after value",
                file=self.file,
                line=line,
                column=col,
                field_name="<json>",
                reason="json_parse_error",
            )
        return value

    # ------------------------------------------------------------------
    # primitives
    # ------------------------------------------------------------------

    def _skip_ws(self) -> None:
        while self.pos < len(self.text) and self.text[self.pos] in " \t\n\r":
            self.pos += 1

    def _peek(self) -> str | None:
        return self.text[self.pos] if self.pos < len(self.text) else None

    def _expect(self, ch: str) -> None:
        if self.pos >= len(self.text) or self.text[self.pos] != ch:
            line, col = _offset_to_line_col(self.text, self.pos)
            raise SchemaError(
                f"JSON parse error in {self.file}: expected '{ch}'",
                file=self.file,
                line=line,
                column=col,
                field_name="<json>",
                reason="json_parse_error",
            )
        self.pos += 1

    # ------------------------------------------------------------------
    # value dispatch
    # ------------------------------------------------------------------

    def _parse_value(self) -> Any:
        self._skip_ws()
        ch = self._peek()
        if ch is None:
            line, col = _offset_to_line_col(self.text, self.pos)
            raise SchemaError(
                f"JSON parse error in {self.file}: unexpected end of input",
                file=self.file,
                line=line,
                column=col,
                field_name="<json>",
                reason="json_parse_error",
            )
        if ch == '"':
            return self._parse_string()
        if ch == "{":
            return self._parse_object()
        if ch == "[":
            return self._parse_array()
        if ch == "t":
            return self._parse_literal("true", True)
        if ch == "f":
            return self._parse_literal("false", False)
        if ch == "n":
            return self._parse_literal("null", None)
        if ch == "-" or ch.isdigit():
            return self._parse_number()
        line, col = _offset_to_line_col(self.text, self.pos)
        raise SchemaError(
            f"JSON parse error in {self.file}: unexpected character {ch!r}",
            file=self.file,
            line=line,
            column=col,
            field_name="<json>",
            reason="json_parse_error",
        )

    # ------------------------------------------------------------------
    # strings (with _ScalarStr wrapping)
    # ------------------------------------------------------------------

    def _parse_string(self) -> _ScalarStr:
        start = self.pos
        line, col = _offset_to_line_col(self.text, start)
        self.pos += 1  # skip opening "

        chars: list[str] = []
        while self.pos < len(self.text):
            ch = self.text[self.pos]
            if ch == '"':
                self.pos += 1  # skip closing "
                break
            if ch == "\\":
                self.pos += 1
                esc = self.text[self.pos]
                if esc == '"':
                    chars.append('"')
                elif esc == "\\":
                    chars.append("\\")
                elif esc == "/":
                    chars.append("/")
                elif esc == "b":
                    chars.append("\b")
                elif esc == "f":
                    chars.append("\f")
                elif esc == "n":
                    chars.append("\n")
                elif esc == "r":
                    chars.append("\r")
                elif esc == "t":
                    chars.append("\t")
                elif esc == "u":
                    code_point = int(self.text[self.pos + 1 : self.pos + 5], 16)
                    self.pos += 4
                    # Handle UTF-16 surrogate pairs.
                    if (
                        0xD800 <= code_point <= 0xDBFF
                        and self.pos + 1 < len(self.text)
                        and self.text[self.pos + 1] == "\\"
                        and self.pos + 2 < len(self.text)
                        and self.text[self.pos + 2] == "u"
                    ):
                        low = int(self.text[self.pos + 3 : self.pos + 7], 16)
                        if 0xDC00 <= low <= 0xDFFF:
                            code_point = (
                                0x10000
                                + (code_point - 0xD800) * 0x400
                                + (low - 0xDC00)
                            )
                            self.pos += 6  # skip \uXXXX of low surrogate
                    chars.append(chr(code_point))
                else:
                    line_e, col_e = _offset_to_line_col(self.text, self.pos)
                    raise SchemaError(
                        f"JSON parse error in {self.file}: invalid escape '\\{esc}'",
                        file=self.file,
                        line=line_e,
                        column=col_e,
                        field_name="<json>",
                        reason="json_parse_error",
                    )
                self.pos += 1
                continue
            chars.append(ch)
            self.pos += 1
        else:
            raise SchemaError(
                f"JSON parse error in {self.file}: unterminated string",
                file=self.file,
                line=line,
                column=col,
                field_name="<json>",
                reason="json_parse_error",
            )

        value = "".join(chars)
        wrapped = _ScalarStr(value)
        wrapped._pcli_line = line
        wrapped._pcli_column = col
        wrapped._pcli_flow = True  # all JSON strings are inline
        self.positions[id(wrapped)] = (line, col, True)
        return wrapped

    # ------------------------------------------------------------------
    # containers
    # ------------------------------------------------------------------

    def _parse_object(self) -> dict[str, Any]:
        self._expect("{")
        result: dict[str, Any] = {}
        self._skip_ws()
        if self._peek() == "}":
            self.pos += 1
            return result
        while True:
            self._skip_ws()
            if self._peek() != '"':
                line, col = _offset_to_line_col(self.text, self.pos)
                raise SchemaError(
                    f"JSON parse error in {self.file}: expected string key",
                    file=self.file,
                    line=line,
                    column=col,
                    field_name="<json>",
                    reason="json_parse_error",
                )
            key = self._parse_string()
            self._skip_ws()
            self._expect(":")
            self._skip_ws()
            value = self._parse_value()
            result[key] = value
            self._skip_ws()
            if self._peek() == ",":
                self.pos += 1
                continue
            break
        self._expect("}")
        return result

    def _parse_array(self) -> list[Any]:
        self._expect("[")
        result: list[Any] = []
        self._skip_ws()
        if self._peek() == "]":
            self.pos += 1
            return result
        while True:
            self._skip_ws()
            result.append(self._parse_value())
            self._skip_ws()
            if self._peek() == ",":
                self.pos += 1
                continue
            break
        self._expect("]")
        return result

    # ------------------------------------------------------------------
    # atoms
    # ------------------------------------------------------------------

    def _parse_literal(self, expected: str, value: Any) -> Any:
        end = self.pos + len(expected)
        if self.text[self.pos : end] != expected:
            line, col = _offset_to_line_col(self.text, self.pos)
            raise SchemaError(
                f"JSON parse error in {self.file}: expected '{expected}'",
                file=self.file,
                line=line,
                column=col,
                field_name="<json>",
                reason="json_parse_error",
            )
        self.pos = end
        return value

    def _parse_number(self) -> int | float:
        m = _NUMBER_RE.match(self.text, self.pos)
        if not m:
            line, col = _offset_to_line_col(self.text, self.pos)
            raise SchemaError(
                f"JSON parse error in {self.file}: invalid number",
                file=self.file,
                line=line,
                column=col,
                field_name="<json>",
                reason="json_parse_error",
            )
        num_str = m.group()
        self.pos = m.end()
        if "." in num_str or "e" in num_str or "E" in num_str:
            return float(num_str)
        return int(num_str)


# ------------------------------------------------------------------
# public API
# ------------------------------------------------------------------


def load_json_with_positions(
    text: str, *, file: str
) -> tuple[Any, dict[int, tuple[int, int, bool]]]:
    """Load a JSON document, wrapping all strings in ``_ScalarStr``.

    Returns ``(data, positions)`` matching the interface of
    :func:`stemmata.yaml_loader.load_with_positions`.
    """
    # Strip UTF-8 BOM if present (common in Windows-generated JSON).
    if text.startswith("\ufeff"):
        text = text[1:]

    parser = _JsonParser(text, file)
    try:
        data = parser.parse()
    except SchemaError:
        raise
    except Exception:
        # Fallback to stdlib json for a better error message.
        try:
            json.loads(text)
        except json.JSONDecodeError as je:
            raise SchemaError(
                f"JSON parse error in {file}: {je.msg}",
                file=file,
                line=je.lineno,
                column=je.colno,
                field_name="<json>",
                reason="json_parse_error",
            )
        raise  # pragma: no cover – parser bug
    return data, parser.positions


def safe_load_json(text: str, *, file: str) -> Any:
    """Load JSON without position tracking."""
    if text.startswith("\ufeff"):
        text = text[1:]
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        raise SchemaError(
            f"JSON parse error in {file}: {e.msg}",
            file=file,
            line=e.lineno,
            column=e.colno,
            field_name="<json>",
            reason="json_parse_error",
        )
