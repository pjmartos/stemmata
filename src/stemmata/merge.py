from __future__ import annotations

import re
from typing import Any

from stemmata.errors import MergeError


_MISSING = object()
_BARE_ABSTRACT_MARKER_RE = re.compile(r"^\s*\$\{\s*abstract:[^{}]+\}\s*$")


def _type_kind(v: Any) -> str:
    if isinstance(v, dict):
        return "map"
    if isinstance(v, list):
        return "list"
    return "scalar"


def _is_bare_abstract_marker(v: Any) -> bool:
    return isinstance(v, str) and bool(_BARE_ABSTRACT_MARKER_RE.match(v))


def merge_pair(nearer: Any, farther: Any, *, path: str = "") -> Any:
    if nearer is _MISSING:
        return farther
    if farther is _MISSING:
        return nearer
    if isinstance(nearer, dict) and isinstance(farther, dict):
        merged: dict[str, Any] = {}
        for k in nearer.keys():
            child_path = f"{path}.{k}" if path else k
            if k in farther:
                merged[k] = merge_pair(nearer[k], farther[k], path=child_path)
            else:
                merged[k] = nearer[k]
        for k in farther.keys():
            if k not in nearer:
                merged[k] = farther[k]
        return merged
    if nearer is not None and farther is not None:
        nk = _type_kind(nearer)
        fk = _type_kind(farther)
        if nk != fk:
            nearer_is_marker = _is_bare_abstract_marker(nearer)
            farther_is_marker = _is_bare_abstract_marker(farther)
            if nearer_is_marker and not farther_is_marker:
                return farther
            if farther_is_marker and not nearer_is_marker:
                return nearer
            raise MergeError(
                path=path,
                conflict="type_mismatch",
                types=[nk, fk],
                nodes=[],
            )
    return nearer


def _walk(data: Any, parts: list[str]) -> tuple[Any, bool]:
    if not parts:
        return data, True
    cur: Any = data
    for p in parts:
        if not isinstance(cur, dict) or p not in cur:
            return None, False
        cur = cur[p]
    return cur, True


def merge_namespaces(
    layers: list[Any],
    *,
    provenance: list[tuple[str, str]] | None = None,
) -> Any:
    """Merge a BFS-ordered list of ancestor namespaces (nearest first).

    When ``provenance`` is supplied, each ``(canonical_id, file)`` pair must line
    up 1:1 with ``layers``.
    """
    if not layers:
        return {}
    if provenance is not None and len(provenance) != len(layers):
        raise ValueError("provenance length must match layers length")
    result: Any = _MISSING
    for data in layers:
        value = None if data is None else data
        if result is _MISSING:
            result = value
            continue
        try:
            result = merge_pair(result, value)
        except MergeError as err:
            if provenance is None:
                raise
            path_str = err.details.get("path", "") if isinstance(err.details, dict) else ""
            parts = path_str.split(".") if path_str else []
            nodes: list[dict[str, Any]] = []
            for (cid, fle), ldata in zip(provenance, layers):
                _, found = _walk(ldata, parts)
                if found:
                    nodes.append({
                        "file": fle or cid,
                        "line": None,
                        "column": None,
                        "ancestor": cid,
                    })
            raise MergeError(
                path=path_str,
                conflict=err.details.get("conflict", "type_mismatch"),
                types=err.details.get("types", []),
                nodes=nodes,
            ) from err
    return result if result is not _MISSING else {}
