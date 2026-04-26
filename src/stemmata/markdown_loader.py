"""Markdown resource loading and ``${resource:...}`` extraction."""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from stemmata.errors import SchemaError


RESOURCE_RE = re.compile(r"\$\{resource:([^{}]+)\}")
_ESCAPE_RE = re.compile(r"\$\$\{[^{}]*\}")
_BOM_BYTES = b"\xef\xbb\xbf"


def mask_escapes(text: str) -> str:
    """Replace ``$${...}`` runs with NULs so they are not mistaken for refs."""
    return _ESCAPE_RE.sub(lambda m: "\x00" * len(m.group(0)), text)


@dataclass
class MarkdownReference:
    raw: str
    text: str
    line: int
    column: int


@dataclass
class MarkdownDocument:
    file: str
    content: str
    references: list[MarkdownReference] = field(default_factory=list)


def _raise_resource(file: str, line: int | None, column: int | None, *, reason: str, msg: str) -> None:
    raise SchemaError(msg, file=file, line=line, column=column, field_name="<resource>", reason=reason)


def _check_hygiene(raw_bytes: bytes | None, text: str, file: str) -> None:
    has_bom = (raw_bytes is not None and raw_bytes.startswith(_BOM_BYTES)) or text.startswith("﻿")
    if has_bom:
        raise SchemaError(
            f"Markdown file {file} begins with a BOM",
            file=file, line=1, column=1, field_name="<bom>", reason="bom_present",
        )


def parse_markdown(text: str, *, file: str, strict: bool = True, raw_bytes: bytes | None = None) -> MarkdownDocument:
    """Parse a Markdown resource payload.

    Enforces the rule: very ``${resource:...}`` MUST be
    the sole content of its line. Violations raise :class:`SchemaError`.
    """
    if strict:
        _check_hygiene(raw_bytes, text, file)
    references: list[MarkdownReference] = []
    for idx, line in enumerate(text.split("\n"), start=1):
        matches = list(RESOURCE_RE.finditer(mask_escapes(line)))
        if not matches:
            continue
        if len(matches) > 1:
            _raise_resource(file, idx, matches[1].start() + 1,
                            reason="resource_multiple_per_line",
                            msg=f"Markdown line contains multiple ${{resource:...}} references ({file}:{idx})")
        m = matches[0]
        col = m.start() + 1
        if m.start() != 0 or m.end() != len(line):
            _raise_resource(file, idx, col, reason="resource_not_line_exclusive",
                            msg=f"Markdown ${{resource:...}} must occupy a whole line with no surrounding text ({file}:{idx})")
        if not m.group(1).strip():
            _raise_resource(file, idx, col, reason="resource_empty_body",
                            msg=f"Markdown ${{resource:}} has empty body ({file}:{idx})")
        references.append(MarkdownReference(raw=m.group(1), text=m.group(0), line=idx, column=col))
    return MarkdownDocument(file=file, content=text, references=references)


def read_markdown(file_path: str, *, strict: bool = True) -> MarkdownDocument:
    with open(file_path, "rb") as fh:
        raw = fh.read()
    text = raw.decode("utf-8").replace("\r\n", "\n").replace("\r", "\n")
    return parse_markdown(text, file=file_path, strict=strict, raw_bytes=raw)
