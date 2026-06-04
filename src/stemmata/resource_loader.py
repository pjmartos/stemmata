"""Resource payload loading and ``${resource:...}`` extraction.

Resource payloads are read as opaque text regardless of declared ``contentType``
(``markdown``, ``text``, ``xml``, ``json``, ``yaml``). The same hygiene rules
apply across all of them: no UTF-8 BOM, and any ``${resource:...}`` reference
must occupy a whole line on its own.
"""
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
class ResourceReference:
    raw: str
    text: str
    line: int
    column: int


@dataclass
class ResourceDocument:
    file: str
    content: str
    references: list[ResourceReference] = field(default_factory=list)


def _raise_resource(file: str, line: int | None, column: int | None, *, reason: str, msg: str) -> None:
    raise SchemaError(msg, file=file, line=line, column=column, field_name="<resource>", reason=reason)


def _check_hygiene(raw_bytes: bytes | None, text: str, file: str) -> None:
    has_bom = (raw_bytes is not None and raw_bytes.startswith(_BOM_BYTES)) or text.startswith("﻿")
    if has_bom:
        raise SchemaError(
            f"resource file {file} begins with a BOM",
            file=file, line=1, column=1, field_name="<bom>", reason="bom_present",
        )


def parse_resource(text: str, *, file: str, strict: bool = True, raw_bytes: bytes | None = None) -> ResourceDocument:
    """Parse a resource payload.

    Enforces the rule: every ``${resource:...}`` MUST be the sole content of
    its line. Violations raise :class:`SchemaError`. The rule applies to all
    text-based resource ``contentType`` values, not just ``markdown``.
    """
    if strict:
        _check_hygiene(raw_bytes, text, file)
    references: list[ResourceReference] = []
    for idx, line in enumerate(text.split("\n"), start=1):
        matches = list(RESOURCE_RE.finditer(mask_escapes(line)))
        if not matches:
            continue
        if len(matches) > 1:
            _raise_resource(file, idx, matches[1].start() + 1,
                            reason="resource_multiple_per_line",
                            msg=f"resource line contains multiple ${{resource:...}} references ({file}:{idx})")
        m = matches[0]
        col = m.start() + 1
        if m.start() != 0 or m.end() != len(line):
            _raise_resource(file, idx, col, reason="resource_not_line_exclusive",
                            msg=f"${{resource:...}} must occupy a whole line with no surrounding text ({file}:{idx})")
        if not m.group(1).strip():
            _raise_resource(file, idx, col, reason="resource_empty_body",
                            msg=f"${{resource:}} has empty body ({file}:{idx})")
        references.append(ResourceReference(raw=m.group(1), text=m.group(0), line=idx, column=col))
    return ResourceDocument(file=file, content=text, references=references)


def read_resource(file_path: str, *, strict: bool = True) -> ResourceDocument:
    with open(file_path, "rb") as fh:
        raw = fh.read()
    text = raw.decode("utf-8").replace("\r\n", "\n").replace("\r", "\n")
    return parse_resource(text, file=file_path, strict=strict, raw_bytes=raw)
