from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from stemmata.errors import (
    AbstractUnfilledError,
    CycleError,
    MergeError,
    ReferenceError_,
    UnresolvableError,
)
from stemmata.yaml_loader import scalar_meta


_PLACEHOLDER_RE = re.compile(r"\$\{(=)?([^{}]+)\}")
_ESCAPE_TOKEN = "\x00PCLI_ESC_DOLLAR\x00"

_RESOURCE_PREFIX = "resource:"
_ABSTRACT_PREFIX = "abstract:"


@dataclass
class Layer:
    canonical_id: str
    data: dict[str, Any]


@dataclass
class ResourceBinding:
    bindings: dict[tuple[str, str], str] = field(default_factory=dict)
    flat_texts: dict[str, str] = field(default_factory=dict)
    prompt_resources: dict[str, list[str]] = field(default_factory=dict)
    resource_children: dict[str, list[str]] = field(default_factory=dict)
    resource_files: dict[str, str] = field(default_factory=dict)


def _resource_body(inner: str) -> str | None:
    stripped = inner.lstrip()
    if not stripped.startswith(_RESOURCE_PREFIX):
        return None
    return stripped[len(_RESOURCE_PREFIX):].strip()


def _abstract_body(inner: str) -> str | None:
    stripped = inner.lstrip()
    if not stripped.startswith(_ABSTRACT_PREFIX):
        return None
    return stripped[len(_ABSTRACT_PREFIX):].strip()


def _is_abstract_marker_value(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    exact, _non_splat, inner = _exact_placeholder(value)
    return bool(exact) and _abstract_body(inner) is not None


def _resource_lookup(
    binding: ResourceBinding | None,
    file: str | None,
    body: str,
    *,
    line: int | None,
    column: int | None,
) -> str:
    from stemmata.resource_resolve import _parse_coordinate_body

    placeholder = f"${{resource:{body}}}"
    coord_parts = _parse_coordinate_body(body)
    searched_in = f"{coord_parts[0]}@{coord_parts[1]}" if coord_parts is not None else "<local>"
    if binding is not None:
        canonical = binding.bindings.get((file or "", body))
        if canonical is not None:
            text = binding.flat_texts.get(canonical)
            if text is not None:
                return text
            searched_in = canonical
    raise ReferenceError_(
        f"unresolved resource reference {placeholder}",
        file=file,
        line=line,
        column=column,
        reference=placeholder,
        searched_in=searched_in,
        kind="resource",
        reason="missing",
    )


_NOT_FOUND = object()
_NULL_SENTINEL = object()


def _walk_path(root: Any, parts: list[str]) -> tuple[object, bool]:
    cur: Any = root
    for i, p in enumerate(parts):
        if cur is None:
            return _NOT_FOUND, False
        if not isinstance(cur, dict):
            return _NOT_FOUND, False
        if p not in cur:
            return _NOT_FOUND, False
        cur = cur[p]
    if cur is None:
        return _NULL_SENTINEL, True
    return cur, True


def lookup_with_provenance(
    namespace: Any,
    layers: list[Layer],
    path: str,
) -> tuple[Any, str, str | None, list[str]]:
    parts = path.split(".")
    for p in parts:
        if not p:
            return _NOT_FOUND, "not_provided", None, [layer.canonical_id for layer in layers]
    value, found = _walk_path(namespace, parts)
    searched = [layer.canonical_id for layer in layers]
    if not found:
        return _NOT_FOUND, "not_provided", None, searched
    if value is _NULL_SENTINEL:
        provider: str | None = None
        for layer in layers:
            v, f = _walk_path(layer.data, parts)
            if f:
                provider = layer.canonical_id
                break
        return _NULL_SENTINEL, "explicit_null", provider, searched
    return value, "ok", None, searched


def _err_unresolvable(path: str, *, file: str | None, line: int | None, column: int | None, reason: str, searched: list[str], provider: str | None) -> UnresolvableError:
    return UnresolvableError(
        path,
        file=file,
        line=line,
        column=column,
        reason=reason,
        ancestors_searched=searched,
        providing_ancestor=provider,
    )


def _stringify_scalar(v: Any) -> str:
    if v is True:
        return "true"
    if v is False:
        return "false"
    if v is None:
        return ""
    if isinstance(v, float):
        if v != v:
            return ".nan"
        return repr(v)
    return str(v)


def _is_scalar(v: Any) -> bool:
    return v is None or isinstance(v, (bool, int, float, str))


def _parse_placeholder_tokens(text: str) -> list[tuple[str, str, int]]:
    tokens: list[tuple[str, str, int]] = []
    i = 0
    while i < len(text):
        start = i
        if text[i] == "$" and i + 1 < len(text) and text[i + 1] == "$":
            if i + 2 < len(text) and text[i + 2] == "{":
                j = text.find("}", i + 3)
                if j == -1:
                    tokens.append(("text", text[i], start))
                    i += 1
                    continue
                tokens.append(("escape", text[i + 2:j + 1], start))
                i = j + 1
                continue
            tokens.append(("text", "$", start))
            i += 2
            continue
        if text[i] == "$" and i + 1 < len(text) and text[i + 1] == "{":
            j = text.find("}", i + 2)
            if j == -1:
                tokens.append(("text", text[i], start))
                i += 1
                continue
            inner = text[i + 2:j]
            tokens.append(("ph", inner, start))
            i = j + 1
            continue
        tokens.append(("text", text[i], start))
        i += 1
    merged: list[tuple[str, str, int]] = []
    for kind, val, off in tokens:
        if merged and merged[-1][0] == "text" and kind == "text":
            merged[-1] = ("text", merged[-1][1] + val, merged[-1][2])
        else:
            merged.append((kind, val, off))
    return merged


def _position_for_offset(
    value: str,
    offset: int,
    meta_line: int | None,
    meta_col: int | None,
) -> tuple[int, int]:
    line_off = value.count("\n", 0, offset)
    col = offset - (value.rfind("\n", 0, offset) + 1)
    base_line = meta_line or 1
    if line_off == 0:
        base_col = meta_col or 1
        column = base_col + col
    else:
        column = col + 1
    return base_line + line_off, column


def _exact_placeholder(text: str) -> tuple[bool, bool, str]:
    trimmed = text.strip()
    if not trimmed.startswith("${") or not trimmed.endswith("}"):
        return False, False, ""
    inner = trimmed[2:-1]
    if "${" in inner or "}" in inner:
        return False, False, ""
    non_splat = inner.startswith("=")
    if non_splat:
        inner = inner[1:]
    if not inner.strip():
        return False, False, ""
    return True, non_splat, inner


def interpolate(
    tree: Any,
    layers: list[Layer],
    *,
    root_file: str,
    resources: ResourceBinding | None = None,
    annotations: dict[str, Any] | None = None,
) -> Any:
    namespace = tree
    return _interp(
        tree,
        namespace,
        layers,
        parent_is_list=False,
        root_file=root_file,
        visiting=(),
        resources=resources,
        annotations=annotations or {},
    )


_SPLAT_MARKER = object()


class _Splat:
    __slots__ = ("items",)

    def __init__(self, items: list[Any]) -> None:
        self.items = items


def _raise_cycle(chain: list[str], path: str, *, file: str | None, line: int | None, column: int | None) -> None:
    cycle_ids = chain + [path]
    raise CycleError(
        nodes=[{"file": file, "line": line, "column": column}],
        cycle_ids=cycle_ids,
    )


def _actual_json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def _abstract_type_mismatch(
    path: str,
    *,
    file: str | None,
    line: int | None,
    column: int | None,
    declared: str,
    actual: Any,
    ancestors_searched: list[str],
) -> AbstractUnfilledError:
    err = AbstractUnfilledError(
        path,
        file=file,
        line=line,
        column=column,
        reason="type_mismatch",
        ancestors_searched=ancestors_searched,
    )
    err.details["declared_type"] = declared
    err.details["actual_type"] = _actual_json_type(actual)
    return err


def _interp(
    node: Any,
    namespace: Any,
    layers: list[Layer],
    *,
    parent_is_list: bool,
    root_file: str,
    visiting: tuple[str, ...],
    resources: ResourceBinding | None = None,
    annotations: dict[str, Any] | None = None,
) -> Any:
    annotations = annotations or {}
    if isinstance(node, dict):
        return {
            k: _interp(
                v,
                namespace,
                layers,
                parent_is_list=False,
                root_file=root_file,
                visiting=visiting,
                resources=resources,
                annotations=annotations,
            )
            for k, v in node.items()
        }
    if isinstance(node, list):
        out: list[Any] = []
        for item in node:
            resolved = _interp(
                item,
                namespace,
                layers,
                parent_is_list=True,
                root_file=root_file,
                visiting=visiting,
                resources=resources,
                annotations=annotations,
            )
            if isinstance(resolved, _Splat):
                out.extend(resolved.items)
            else:
                out.append(resolved)
        return out
    if isinstance(node, str):
        file, line, column, is_flow = scalar_meta(node)
        file = file or root_file
        exact, non_splat, inner_path = _exact_placeholder(node)
        if exact and is_flow:
            body = _resource_body(inner_path)
            if body is not None:
                return _resource_lookup(resources, file, body, line=line, column=column)
            abstract_path = _abstract_body(inner_path)
            if abstract_path is not None:
                if not abstract_path:
                    raise _err_unresolvable(
                        "", file=file, line=line, column=column,
                        reason="not_provided",
                        searched=[layer.canonical_id for layer in layers],
                        provider=None,
                    )
                value, status, _provider, searched = lookup_with_provenance(namespace, layers, abstract_path)
                if status == "not_provided":
                    raise AbstractUnfilledError(abstract_path, file=file, line=line, column=column, reason="not_provided", ancestors_searched=searched)
                if status == "explicit_null":
                    raise AbstractUnfilledError(abstract_path, file=file, line=line, column=column, reason="null_shadow", ancestors_searched=searched)
                if _is_abstract_marker_value(value):
                    raise AbstractUnfilledError(abstract_path, file=file, line=line, column=column, reason="abstract_inherited", ancestors_searched=searched)
                resolved = _interp(
                    value,
                    namespace,
                    layers,
                    parent_is_list=False,
                    root_file=root_file,
                    visiting=visiting + (abstract_path,),
                    resources=resources,
                    annotations=annotations,
                )
                ann = annotations.get(abstract_path)
                declared_type = getattr(ann, "type", "string") if ann is not None else "string"
                if declared_type == "list":
                    if not isinstance(resolved, list):
                        raise _abstract_type_mismatch(
                            abstract_path, file=file, line=line, column=column,
                            declared="list", actual=resolved, ancestors_searched=searched,
                        )
                else:
                    if not _is_scalar(resolved):
                        raise _abstract_type_mismatch(
                            abstract_path, file=file, line=line, column=column,
                            declared="string", actual=resolved, ancestors_searched=searched,
                        )
                if parent_is_list and isinstance(resolved, list) and not non_splat:
                    return _Splat(list(resolved))
                return resolved
            path = inner_path.strip()
            if path in visiting:
                _raise_cycle(list(visiting), path, file=file, line=line, column=column)
            value, status, provider, searched = lookup_with_provenance(namespace, layers, path)
            if status == "not_provided":
                raise _err_unresolvable(path, file=file, line=line, column=column, reason="not_provided", searched=searched, provider=None)
            if status == "explicit_null":
                raise _err_unresolvable(path, file=file, line=line, column=column, reason="explicit_null", searched=searched, provider=provider)
            resolved = _interp(
                value,
                namespace,
                layers,
                parent_is_list=False,
                root_file=root_file,
                visiting=visiting + (path,),
                resources=resources,
                annotations=annotations,
            )
            if parent_is_list and isinstance(resolved, list) and not non_splat:
                return _Splat(list(resolved))
            return resolved
        text = str(node)
        tokens = _parse_placeholder_tokens(text)
        has_placeholder = any(k == "ph" for k, _v, _o in tokens)
        has_escape = any(k == "escape" for k, _v, _o in tokens)
        if not has_placeholder and not has_escape:
            return node
        parts_out: list[str] = []
        for kind, val, offset in tokens:
            if kind == "text":
                parts_out.append(val)
            elif kind == "escape":
                parts_out.append("$" + val)
            else:
                tok_line, tok_col = _position_for_offset(text, offset, line, column)
                body = _resource_body(val)
                if body is not None:
                    parts_out.append(_resource_lookup(resources, file, body, line=tok_line, column=tok_col))
                    continue
                abstract_path = _abstract_body(val)
                if abstract_path is not None:
                    if not abstract_path:
                        raise _err_unresolvable(
                            "", file=file, line=tok_line, column=tok_col,
                            reason="not_provided",
                            searched=[layer.canonical_id for layer in layers],
                            provider=None,
                        )
                    value, status, _provider, searched = lookup_with_provenance(namespace, layers, abstract_path)
                    if status == "not_provided":
                        raise AbstractUnfilledError(abstract_path, file=file, line=tok_line, column=tok_col, reason="not_provided", ancestors_searched=searched)
                    if status == "explicit_null":
                        raise AbstractUnfilledError(abstract_path, file=file, line=tok_line, column=tok_col, reason="null_shadow", ancestors_searched=searched)
                    if _is_abstract_marker_value(value):
                        raise AbstractUnfilledError(abstract_path, file=file, line=tok_line, column=tok_col, reason="abstract_inherited", ancestors_searched=searched)
                    ann = annotations.get(abstract_path)
                    declared_type = getattr(ann, "type", "string") if ann is not None else "string"
                    if declared_type == "list":
                        raise _abstract_type_mismatch(
                            abstract_path, file=file, line=tok_line, column=tok_col,
                            declared="list", actual=value, ancestors_searched=searched,
                        )
                    resolved = _interp(
                        value,
                        namespace,
                        layers,
                        parent_is_list=False,
                        root_file=root_file,
                        visiting=visiting + (abstract_path,),
                        resources=resources,
                        annotations=annotations,
                    )
                    if not _is_scalar(resolved):
                        raise MergeError(
                            path=abstract_path,
                            conflict="non_scalar_abstract",
                            types=[type(resolved).__name__],
                            nodes=[{"file": file, "line": tok_line, "column": tok_col, "ancestor": root_file}],
                        )
                    parts_out.append(_stringify_scalar(resolved))
                    continue
                inner = val
                if inner.startswith("="):
                    inner = inner[1:]
                inner = inner.strip()
                if inner in visiting:
                    _raise_cycle(list(visiting), inner, file=file, line=tok_line, column=tok_col)
                value, status, provider, searched = lookup_with_provenance(namespace, layers, inner)
                if status == "not_provided":
                    raise _err_unresolvable(inner, file=file, line=tok_line, column=tok_col, reason="not_provided", searched=searched, provider=None)
                if status == "explicit_null":
                    raise _err_unresolvable(inner, file=file, line=tok_line, column=tok_col, reason="explicit_null", searched=searched, provider=provider)
                resolved = _interp(
                    value,
                    namespace,
                    layers,
                    parent_is_list=False,
                    root_file=root_file,
                    visiting=visiting + (inner,),
                    resources=resources,
                    annotations=annotations,
                )
                if not _is_scalar(resolved):
                    raise MergeError(
                        path=inner,
                        conflict="non_scalar_in_textual",
                        types=[type(resolved).__name__],
                        nodes=[{"file": file, "line": tok_line, "column": tok_col, "ancestor": root_file}],
                    )
                parts_out.append(_stringify_scalar(resolved))
        return "".join(parts_out)
    return node


@dataclass
class AbstractRef:
    path: str
    file: str | None
    line: int | None
    column: int | None
    is_textual: bool = False


def _iter_abstract_in_scalar(
    value: str, *, file_fallback: str | None, only_declarations: bool,
) -> list[AbstractRef]:
    out: list[AbstractRef] = []
    meta_file, meta_line, meta_col, is_flow = scalar_meta(value)
    src = meta_file or file_fallback
    exact, _non_splat, inner = _exact_placeholder(value)
    if exact and is_flow:
        body = _abstract_body(inner)
        if body is not None:
            out.append(AbstractRef(
                path=body, file=src, line=meta_line, column=meta_col, is_textual=False,
            ))
        return out
    if only_declarations:
        return out
    text = str(value)
    for kind, val, offset in _parse_placeholder_tokens(text):
        if kind != "ph":
            continue
        body = _abstract_body(val)
        if body is not None:
            tok_line, tok_col = _position_for_offset(text, offset, meta_line, meta_col)
            out.append(AbstractRef(
                path=body, file=src, line=tok_line, column=tok_col, is_textual=True,
            ))
    return out


def _walk_abstract_refs(
    namespace: Any, *, file_fallback: str | None, only_declarations: bool,
) -> list[AbstractRef]:
    refs: list[AbstractRef] = []

    def _walk(node: Any) -> None:
        if isinstance(node, dict):
            for v in node.values():
                _walk(v)
            return
        if isinstance(node, list):
            for item in node:
                _walk(item)
            return
        if isinstance(node, str):
            refs.extend(_iter_abstract_in_scalar(
                node, file_fallback=file_fallback, only_declarations=only_declarations,
            ))

    _walk(namespace)
    return refs


def scan_declared_abstracts(
    namespace: Any, *, file_fallback: str | None = None,
) -> list[AbstractRef]:
    return _walk_abstract_refs(namespace, file_fallback=file_fallback, only_declarations=True)


def scan_abstract_references(
    namespace: Any, *, file_fallback: str | None = None,
) -> list[AbstractRef]:
    return _walk_abstract_refs(namespace, file_fallback=file_fallback, only_declarations=False)


def _collect_from_inner(
    inner: str,
    namespace: Any,
    layers: list[Layer],
    *,
    file: str | None,
    line: int | None,
    column: int | None,
    root_file: str,
    out: list[Any],
) -> None:
    abstract_path = _abstract_body(inner)
    if abstract_path is not None:
        if not abstract_path:
            out.append(UnresolvableError(
                "", file=file, line=line, column=column,
                reason="not_provided",
                ancestors_searched=[layer.canonical_id for layer in layers],
                providing_ancestor=None,
            ))
            return
        value, status, _provider, searched = lookup_with_provenance(namespace, layers, abstract_path)
        if status == "not_provided":
            out.append(AbstractUnfilledError(
                abstract_path, file=file, line=line, column=column,
                reason="not_provided", ancestors_searched=searched,
            ))
        elif status == "explicit_null":
            out.append(AbstractUnfilledError(
                abstract_path, file=file, line=line, column=column,
                reason="null_shadow", ancestors_searched=searched,
            ))
        elif _is_abstract_marker_value(value):
            out.append(AbstractUnfilledError(
                abstract_path, file=file, line=line, column=column,
                reason="abstract_inherited", ancestors_searched=searched,
            ))
        return
    value, status, provider, searched = lookup_with_provenance(namespace, layers, inner)
    if status == "not_provided":
        out.append(UnresolvableError(
            inner, file=file, line=line, column=column,
            reason="not_provided", ancestors_searched=searched, providing_ancestor=None,
        ))
    elif status == "explicit_null":
        out.append(UnresolvableError(
            inner, file=file, line=line, column=column,
            reason="explicit_null", ancestors_searched=searched, providing_ancestor=provider,
        ))


@dataclass
class DeclaredAbstract:
    path: str
    file: str | None
    line: int | None
    column: int | None
    annotation_type: str = "string"


def validate_resolved_abstract_types(
    resolved: Any,
    declared: list[DeclaredAbstract],
) -> list[Any]:
    out: list[Any] = []
    for d in declared:
        parts = d.path.split(".")
        cur: Any = resolved
        found = True
        for p in parts:
            if not isinstance(cur, dict) or p not in cur:
                found = False
                break
            cur = cur[p]
        if not found:
            continue
        if d.annotation_type == "list":
            if not isinstance(cur, list):
                out.append(_abstract_type_mismatch(
                    d.path, file=d.file, line=d.line, column=d.column,
                    declared="list", actual=cur, ancestors_searched=[],
                ))
        else:
            if not _is_scalar(cur):
                out.append(_abstract_type_mismatch(
                    d.path, file=d.file, line=d.line, column=d.column,
                    declared="string", actual=cur, ancestors_searched=[],
                ))
    return out


def collect_unfilled_declared_abstracts(
    merged: Any,
    layers: list[Layer],
    declared: list[DeclaredAbstract],
    out: list[Any],
    *,
    already_flagged: set[str] | None = None,
) -> None:
    seen = set(already_flagged or ())
    for d in declared:
        if d.path in seen:
            continue
        value, status, _provider, searched = lookup_with_provenance(
            merged, layers, d.path,
        )
        if status == "not_provided":
            err = AbstractUnfilledError(
                d.path, file=d.file, line=d.line, column=d.column,
                reason="not_provided", ancestors_searched=searched,
            )
        elif status == "explicit_null":
            err = AbstractUnfilledError(
                d.path, file=d.file, line=d.line, column=d.column,
                reason="null_shadow", ancestors_searched=searched,
            )
        elif _is_abstract_marker_value(value):
            err = AbstractUnfilledError(
                d.path, file=d.file, line=d.line, column=d.column,
                reason="abstract_inherited", ancestors_searched=searched,
            )
        else:
            continue
        out.append(err)
        seen.add(d.path)


def collect_placeholder_errors(
    node: Any,
    namespace: Any,
    layers: list[Layer],
    *,
    parent_is_list: bool,
    root_file: str,
    out: list[Any],
) -> None:
    if isinstance(node, dict):
        for v in node.values():
            collect_placeholder_errors(
                v, namespace, layers,
                parent_is_list=False, root_file=root_file, out=out,
            )
        return
    if isinstance(node, list):
        for item in node:
            collect_placeholder_errors(
                item, namespace, layers,
                parent_is_list=True, root_file=root_file, out=out,
            )
        return
    if not isinstance(node, str):
        return

    file, line, column, is_flow = scalar_meta(node)
    file = file or root_file
    exact, _non_splat, inner_path = _exact_placeholder(node)
    if exact and is_flow:
        if _resource_body(inner_path) is not None:
            return
        _collect_from_inner(
            inner_path, namespace, layers,
            file=file, line=line, column=column, root_file=root_file, out=out,
        )
        return

    text = str(node)
    tokens = _parse_placeholder_tokens(text)
    for kind, val, offset in tokens:
        if kind != "ph":
            continue
        if _resource_body(val) is not None:
            continue
        tok_line, tok_col = _position_for_offset(text, offset, line, column)
        abstract_path = _abstract_body(val)
        if abstract_path is not None:
            _collect_from_inner(
                val, namespace, layers,
                file=file, line=tok_line, column=tok_col, root_file=root_file, out=out,
            )
            continue
        inner = val
        if inner.startswith("="):
            inner = inner[1:]
        inner = inner.strip()
        value, status, provider, searched = lookup_with_provenance(namespace, layers, inner)
        if status == "not_provided":
            out.append(UnresolvableError(
                inner, file=file, line=tok_line, column=tok_col,
                reason="not_provided", ancestors_searched=searched, providing_ancestor=None,
            ))
        elif status == "explicit_null":
            out.append(UnresolvableError(
                inner, file=file, line=tok_line, column=tok_col,
                reason="explicit_null", ancestors_searched=searched, providing_ancestor=provider,
            ))
        elif not _is_scalar(value):
            out.append(MergeError(
                path=inner,
                conflict="non_scalar_in_textual",
                types=[type(value).__name__],
                nodes=[{"file": file, "line": tok_line, "column": tok_col, "ancestor": root_file}],
            ))
