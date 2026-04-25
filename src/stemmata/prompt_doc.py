from __future__ import annotations

import posixpath
import re
from dataclasses import dataclass, field
from typing import Any

from stemmata.errors import SchemaError
from stemmata.json_loader import load_json_with_positions
from stemmata.manifest import is_scoped_name, is_semver
from stemmata.markdown_loader import RESOURCE_RE, mask_escapes
from stemmata.yaml_loader import load_with_positions, scalar_meta


_EMPTY_RESOURCE_RE = re.compile(r"\$\{resource:\s*\}")


RESERVED_KEYS = {"ancestors", "$schema", "abstracts"}


_ABSTRACT_TYPES = ("string", "list")
_ANNOTATION_FIELDS = ("description", "type", "example")


def _type_kind(v: Any) -> str:
    if isinstance(v, dict):
        return "map"
    if isinstance(v, list):
        return "list"
    return "scalar"


def _merge_intra_doc(path: str, existing: Any, incoming: Any, *, file: str) -> Any:
    ek, ik = _type_kind(existing), _type_kind(incoming)
    if ek != ik:
        raise SchemaError(
            f"intra-document type conflict at '{path}': {ek} vs {ik}",
            file=file,
            field_name=path,
            reason="intra_doc_type_conflict",
        )
    if isinstance(existing, dict):
        merged: dict[str, Any] = dict(existing)
        for k, v in incoming.items():
            sub = f"{path}.{k}"
            if k in merged:
                merged[k] = _merge_intra_doc(sub, merged[k], v, file=file)
            else:
                merged[k] = v
        return merged
    return incoming


def _expand_dotted_keys(d: dict[str, Any], *, file: str = "", prefix: str = "") -> dict[str, Any]:
    """Expand dotted keys like ``{"a.b.c": 1}`` into ``{"a": {"b": {"c": 1}}}``.

    Intra-document type conflicts (e.g. ``a: 1`` alongside ``a.b: 2``) abort
    with a ``SchemaError``. Same-type duplicates fall back to last-wins in
    declaration order; two maps at the same path are deep-merged recursively.
    """
    result: dict[str, Any] = {}
    for key, value in d.items():
        parts = key.split(".")
        full = f"{prefix}{key}"
        if isinstance(value, dict):
            expanded_value: Any = _expand_dotted_keys(value, file=file, prefix=f"{full}.")
        else:
            expanded_value = value
        if len(parts) == 1:
            if key in result:
                result[key] = _merge_intra_doc(full, result[key], expanded_value, file=file)
            else:
                result[key] = expanded_value
            continue
        cur = result
        walked = prefix
        for part in parts[:-1]:
            walked_path = f"{walked}{part}"
            if part in cur:
                if not isinstance(cur[part], dict):
                    raise SchemaError(
                        f"intra-document type conflict at '{walked_path}': "
                        f"dotted key '{full}' requires a map but existing value is {_type_kind(cur[part])}",
                        file=file,
                        field_name=walked_path,
                        reason="intra_doc_type_conflict",
                    )
                cur = cur[part]
            else:
                cur[part] = {}
                cur = cur[part]
            walked = f"{walked_path}."
        leaf = parts[-1]
        if leaf in cur:
            cur[leaf] = _merge_intra_doc(full, cur[leaf], expanded_value, file=file)
        else:
            cur[leaf] = expanded_value
    return result


@dataclass
class PathRef:
    raw: str
    line: int | None
    column: int | None


@dataclass
class CoordRef:
    package: str
    version: str
    prompt: str
    line: int | None
    column: int | None


AncestorRef = PathRef | CoordRef


@dataclass
class ResourceRefInPrompt:
    body: str
    file: str
    line: int | None
    column: int | None


@dataclass
class AbstractAnnotation:
    path: str
    description: str
    type: str
    example: Any = None
    has_example: bool = False
    line: int | None = None
    column: int | None = None


@dataclass
class PromptDocument:
    file: str
    data: dict[str, Any]
    ancestors: list[AncestorRef]
    schema_uri: str | None
    namespace: dict[str, Any] = field(default_factory=dict)
    resource_refs: list[ResourceRefInPrompt] = field(default_factory=list)
    abstracts: dict[str, AbstractAnnotation] = field(default_factory=dict)
    disk_file: str = ""

    def __post_init__(self) -> None:
        if not self.disk_file:
            self.disk_file = self.file


def _validate_rel_path(raw: str, *, file: str, line: int | None, column: int | None) -> None:
    # Escape-root is context-dependent (depends on the referring file's depth
    # in the package) and is enforced at resolution time; only absolutes are
    # rejectable purely from the raw string.
    if raw.startswith("/"):
        raise SchemaError(
            f"relative ancestor reference must not be absolute: {raw!r}",
            file=file,
            line=line,
            column=column,
            field_name="ancestors",
            reason="absolute_path",
        )


def _is_json_file(file: str) -> bool:
    return file.lower().endswith(".json")


def _raise_resource(file: str, line: int | None, column: int | None, *, reason: str, msg: str) -> None:
    raise SchemaError(msg, file=file, line=line, column=column, field_name="<resource>", reason=reason)


def _iter_scalar_refs(value: str) -> list[tuple[str, int, int]]:
    """Yield (body, line_offset, col_0based) for each unescaped reference."""
    out: list[tuple[str, int, int]] = []
    for m in RESOURCE_RE.finditer(mask_escapes(value)):
        line_off = value.count("\n", 0, m.start())
        col = m.start() - (value.rfind("\n", 0, m.start()) + 1)
        out.append((m.group(1), line_off, col))
    return out


def _validate_resource_positions_in_scalar(value: str, *, file_fallback: str, in_key: bool) -> None:
    if "${resource:" not in value:
        return
    meta_file, meta_line, meta_col, is_flow = scalar_meta(value)
    src = meta_file or file_fallback
    if in_key:
        _raise_resource(src, meta_line, meta_col, reason="resource_in_key",
                        msg=f"${{resource:...}} is not allowed inside mapping keys ({src}:{meta_line})")
    masked = mask_escapes(value)
    empty_match = _EMPTY_RESOURCE_RE.search(masked)
    if empty_match is not None:
        line_off = value.count("\n", 0, empty_match.start())
        abs_line = (meta_line or 1) + line_off
        col = empty_match.start() - (value.rfind("\n", 0, empty_match.start()) + 1) + 1
        _raise_resource(src, abs_line, col, reason="resource_empty_body",
                        msg=f"${{resource:}} has empty body ({src}:{abs_line})")
    matches = list(RESOURCE_RE.finditer(masked))
    if not matches:
        return
    if is_flow:
        if len(matches) != 1:
            _raise_resource(src, meta_line, meta_col, reason="resource_multiple_in_flow",
                            msg=f"flow-style scalar contains more than one ${{resource:...}} reference ({src}:{meta_line})")
        m = matches[0]
        if value.strip() != m.group(0):
            _raise_resource(src, meta_line, meta_col, reason="resource_not_exact_in_flow",
                            msg=f"${{resource:...}} in a flow scalar must be the entire trimmed content ({src}:{meta_line})")
        if not m.group(1).strip():
            _raise_resource(src, meta_line, meta_col, reason="resource_empty_body",
                            msg=f"${{resource:}} has empty body ({src}:{meta_line})")
        return
    base = meta_line or 1
    for line_idx, line in enumerate(value.split("\n")):
        line_matches = list(RESOURCE_RE.finditer(mask_escapes(line)))
        if not line_matches:
            continue
        abs_line = base + line_idx
        if len(line_matches) > 1:
            _raise_resource(src, abs_line, line_matches[1].start() + 1, reason="resource_multiple_per_line",
                            msg=f"multiple ${{resource:...}} on one block-scalar line ({src}:{abs_line})")
        m = line_matches[0]
        col = m.start() + 1
        if m.start() != 0 or m.end() != len(line):
            _raise_resource(src, abs_line, col, reason="resource_not_line_exclusive",
                            msg=f"${{resource:...}} in block scalar must occupy a whole line with no surrounding text ({src}:{abs_line})")
        if not m.group(1).strip():
            _raise_resource(src, abs_line, col, reason="resource_empty_body",
                            msg=f"${{resource:}} has empty body ({src}:{abs_line})")


def _walk_scalars(node: Any, visit) -> None:
    if isinstance(node, dict):
        for k, v in node.items():
            if isinstance(k, str):
                visit(k, in_key=True)
            _walk_scalars(v, visit)
    elif isinstance(node, list):
        for item in node:
            _walk_scalars(item, visit)
    elif isinstance(node, str):
        visit(node, in_key=False)


def _walk_validate_resource_positions(node: Any, *, file_fallback: str) -> None:
    def visit(value: str, *, in_key: bool) -> None:
        _validate_resource_positions_in_scalar(value, file_fallback=file_fallback, in_key=in_key)
    _walk_scalars(node, visit)


def collect_resource_refs(namespace: Any, *, file_fallback: str) -> list[ResourceRefInPrompt]:
    """Walk *namespace* and collect every ``${resource:...}`` reference.

    Called by the resolver after ``attach_file`` has tagged every scalar with
    its runtime file key, so the collected entries carry binding keys that
    match ``scalar_meta(s)[0]`` at interpolation time.
    """
    refs: list[ResourceRefInPrompt] = []

    def visit(value: str, *, in_key: bool) -> None:
        if in_key or "${resource:" not in value:
            return
        meta_file, meta_line, meta_col, _ = scalar_meta(value)
        src = meta_file or file_fallback
        base = meta_line or 1
        for body, line_off, col in _iter_scalar_refs(value):
            refs.append(ResourceRefInPrompt(
                body=body.strip(),
                file=src,
                line=base + line_off,
                column=(col + 1) if line_off > 0 else (meta_col or (col + 1)),
            ))

    _walk_scalars(namespace, visit)
    return refs


def parse_prompt(text: str, *, file: str, strict: bool = True, validate_paths: bool = True) -> PromptDocument:
    if _is_json_file(file):
        data, _positions = load_json_with_positions(text, file=file)
    else:
        data, _positions = load_with_positions(text, file=file, strict=strict)
    if data is None:
        data = {}
    if not isinstance(data, dict):
        fmt = "JSON" if _is_json_file(file) else "YAML"
        raise SchemaError(
            f"prompt {file} must be a {fmt} mapping at the top level",
            file=file,
            field_name="<root>",
            reason="not_mapping",
        )
    ancestors_raw = data.get("ancestors")
    ancestors: list[AncestorRef] = []
    if ancestors_raw is not None:
        if not isinstance(ancestors_raw, list):
            raise SchemaError(
                f"'ancestors' must be a YAML sequence, got {type(ancestors_raw).__name__}",
                file=file,
                field_name="ancestors",
                reason="not_sequence",
            )
        for idx, item in enumerate(ancestors_raw):
            if isinstance(item, str):
                if validate_paths:
                    _validate_rel_path(item, file=file, line=None, column=None)
                ancestors.append(PathRef(raw=item, line=None, column=None))
            elif isinstance(item, dict):
                for k in ("package", "version", "prompt"):
                    if k not in item:
                        raise SchemaError(
                            f"cross-package reference missing '{k}' (ancestors[{idx}])",
                            file=file,
                            field_name=f"ancestors[{idx}].{k}",
                            reason="missing_field",
                        )
                if set(item.keys()) - {"package", "version", "prompt"}:
                    extras = set(item.keys()) - {"package", "version", "prompt"}
                    raise SchemaError(
                        f"cross-package reference has unexpected fields: {sorted(extras)}",
                        file=file,
                        field_name=f"ancestors[{idx}]",
                        reason="extra_fields",
                    )
                pkg = item["package"]
                ver = item["version"]
                pid = item["prompt"]
                if not isinstance(pkg, str) or not is_scoped_name(pkg):
                    raise SchemaError(
                        f"ancestors[{idx}].package must be scoped name, got {pkg!r}",
                        file=file,
                        field_name=f"ancestors[{idx}].package",
                        reason="invalid_package",
                    )
                if not isinstance(ver, str) or not is_semver(ver):
                    raise SchemaError(
                        f"ancestors[{idx}].version must be strict SemVer, got {ver!r}",
                        file=file,
                        field_name=f"ancestors[{idx}].version",
                        reason="invalid_version",
                    )
                if not isinstance(pid, str) or not pid:
                    raise SchemaError(
                        f"ancestors[{idx}].prompt must be a non-empty string",
                        file=file,
                        field_name=f"ancestors[{idx}].prompt",
                        reason="invalid_prompt_id",
                    )
                ancestors.append(CoordRef(package=pkg, version=ver, prompt=pid, line=None, column=None))
            else:
                raise SchemaError(
                    f"ancestors[{idx}] must be a string or mapping, got {type(item).__name__}",
                    file=file,
                    field_name=f"ancestors[{idx}]",
                    reason="invalid_ancestor",
                )
    schema_uri = data.get("$schema")
    if schema_uri is not None and not isinstance(schema_uri, str):
        raise SchemaError(
            "'$schema' must be a string",
            file=file,
            field_name="$schema",
            reason="invalid_schema",
        )
    abstracts = _parse_abstracts_block(data.get("abstracts"), file=file)
    namespace = _expand_dotted_keys(
        {k: v for k, v in data.items() if k not in RESERVED_KEYS},
        file=file,
    )
    _walk_validate_resource_positions(namespace, file_fallback=file)
    if abstracts:
        _check_local_abstract_annotations(
            abstracts, namespace, file=file, has_ancestors=bool(ancestors),
        )
    return PromptDocument(
        file=file,
        data=data,
        ancestors=ancestors,
        schema_uri=schema_uri,
        namespace=namespace,
        abstracts=abstracts,
    )


def _check_local_abstract_annotations(
    abstracts: dict[str, AbstractAnnotation],
    namespace: dict[str, Any],
    *,
    file: str,
    has_ancestors: bool,
) -> None:
    from stemmata.interp import scan_abstract_references

    refs = scan_abstract_references(namespace, file_fallback=file)
    refs_by_path: dict[str, list[Any]] = {}
    for r in refs:
        refs_by_path.setdefault(r.path, []).append(r)
    for path, ann in abstracts.items():
        path_refs = refs_by_path.get(path)
        if not path_refs:
            if has_ancestors:
                continue
            raise SchemaError(
                f"'abstracts.{path}' annotates a path that the prompt body never declares "
                f"(no ${{abstract:{path}}} marker found)",
                file=file,
                line=ann.line,
                column=ann.column,
                field_name=f"abstracts.{path}",
                reason="annotation_without_declaration",
            )
        if ann.type == "list":
            for r in path_refs:
                if getattr(r, "is_textual", False):
                    raise SchemaError(
                        f"${{abstract:{path}}} is annotated as type 'list' but appears "
                        f"in textual position (inside a larger string scalar); "
                        f"list-shaped abstracts must occupy a structural position",
                        file=file,
                        line=r.line,
                        column=r.column,
                        field_name=f"abstracts.{path}.type",
                        reason="list_abstract_in_textual_position",
                    )


def _parse_abstracts_block(raw: Any, *, file: str) -> dict[str, AbstractAnnotation]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise SchemaError(
            f"'abstracts' must be a mapping of dotted paths to annotation objects, got {type(raw).__name__}",
            file=file,
            field_name="abstracts",
            reason="invalid_abstracts",
        )
    annotations: dict[str, AbstractAnnotation] = {}
    for path, entry in raw.items():
        if not isinstance(path, str) or not path.strip():
            raise SchemaError(
                f"'abstracts' keys must be non-empty dotted-path strings, got {path!r}",
                file=file,
                field_name="abstracts",
                reason="invalid_abstract_path",
            )
        clean_path = str(path)
        if any(not seg for seg in clean_path.split(".")):
            raise SchemaError(
                f"'abstracts' key {clean_path!r} has empty dotted segment(s)",
                file=file,
                field_name=f"abstracts.{clean_path}",
                reason="invalid_abstract_path",
            )
        line = getattr(path, "_pcli_line", None)
        column = getattr(path, "_pcli_column", None)
        if not isinstance(entry, dict):
            raise SchemaError(
                f"'abstracts.{clean_path}' must be a mapping of annotation fields, got {type(entry).__name__}",
                file=file,
                line=line,
                column=column,
                field_name=f"abstracts.{clean_path}",
                reason="invalid_abstract_annotation",
            )
        unknown = [k for k in entry if k not in _ANNOTATION_FIELDS]
        if unknown:
            raise SchemaError(
                f"'abstracts.{clean_path}' has unknown field(s): {sorted(unknown)}",
                file=file,
                line=line,
                column=column,
                field_name=f"abstracts.{clean_path}",
                reason="unknown_annotation_field",
            )
        if "description" not in entry:
            raise SchemaError(
                f"'abstracts.{clean_path}' is missing required field 'description'",
                file=file,
                line=line,
                column=column,
                field_name=f"abstracts.{clean_path}.description",
                reason="missing_description",
            )
        description = entry["description"]
        if not isinstance(description, str) or not description.strip():
            raise SchemaError(
                f"'abstracts.{clean_path}.description' must be a non-empty string",
                file=file,
                line=line,
                column=column,
                field_name=f"abstracts.{clean_path}.description",
                reason="empty_description",
            )
        type_val = entry.get("type", "string") if "type" in entry else "string"
        if not isinstance(type_val, str) or type_val not in _ABSTRACT_TYPES:
            raise SchemaError(
                f"'abstracts.{clean_path}.type' must be one of {list(_ABSTRACT_TYPES)}, got {type_val!r}",
                file=file,
                line=line,
                column=column,
                field_name=f"abstracts.{clean_path}.type",
                reason="invalid_annotation_type",
            )
        has_example = "example" in entry
        example = entry.get("example") if has_example else None
        annotations[clean_path] = AbstractAnnotation(
            path=clean_path,
            description=str(description),
            type=str(type_val),
            example=example,
            has_example=has_example,
            line=line,
            column=column,
        )
    return annotations


def resolve_relative(referrer_file: str, raw_rel: str) -> str:
    ref_dir = posixpath.dirname(referrer_file.replace("\\", "/"))
    joined = posixpath.normpath(posixpath.join(ref_dir, raw_rel))
    return joined
