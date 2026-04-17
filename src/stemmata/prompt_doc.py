from __future__ import annotations

import posixpath
from dataclasses import dataclass, field
from typing import Any

from stemmata.errors import SchemaError
from stemmata.json_loader import load_json_with_positions
from stemmata.manifest import is_scoped_name, is_semver
from stemmata.yaml_loader import load_with_positions


RESERVED_KEYS = {"ancestors", "$schema"}


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
class PromptDocument:
    file: str
    data: dict[str, Any]
    ancestors: list[AncestorRef]
    schema_uri: str | None
    namespace: dict[str, Any] = field(default_factory=dict)


def _validate_rel_path(raw: str, *, file: str, line: int | None, column: int | None) -> None:
    if raw.startswith("/"):
        raise SchemaError(
            f"relative ancestor reference must not be absolute: {raw!r}",
            file=file,
            line=line,
            column=column,
            field_name="ancestors",
            reason="absolute_path",
        )
    parts = raw.split("/")
    depth = 0
    for p in parts:
        if p == "..":
            depth -= 1
            if depth < 0:
                raise SchemaError(
                    f"relative ancestor reference escapes package root: {raw!r}",
                    file=file,
                    line=line,
                    column=column,
                    field_name="ancestors",
                    reason="escape_root",
                )
        elif p and p != ".":
            depth += 1


def _is_json_file(file: str) -> bool:
    return file.lower().endswith(".json")


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
    namespace = _expand_dotted_keys(
        {k: v for k, v in data.items() if k not in RESERVED_KEYS},
        file=file,
    )
    return PromptDocument(
        file=file,
        data=data,
        ancestors=ancestors,
        schema_uri=schema_uri,
        namespace=namespace,
    )


def resolve_relative(referrer_file: str, raw_rel: str) -> str:
    ref_dir = posixpath.dirname(referrer_file.replace("\\", "/"))
    joined = posixpath.normpath(posixpath.join(ref_dir, raw_rel))
    return joined
