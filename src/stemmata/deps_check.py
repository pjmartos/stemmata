from __future__ import annotations

import posixpath
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from stemmata.errors import PromptCliError, SchemaError
from stemmata.manifest import Manifest, PromptEntry, ResourceEntry
from stemmata.markdown_loader import read_markdown
from stemmata.prompt_doc import (
    CoordRef,
    PathRef,
    collect_resource_refs,
    parse_prompt,
    resolve_relative,
)
from stemmata.resource_resolve import _parse_coordinate_body


_BOM_BYTES = b"\xef\xbb\xbf"


def _read_normalised(path: Path) -> str:
    raw = path.read_bytes()
    if raw.startswith(_BOM_BYTES):
        raw = raw[len(_BOM_BYTES):]
    if b"\r" in raw:
        raw = raw.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    return raw.decode("utf-8")


@dataclass
class _ResourceUsage:
    entry: PromptEntry | ResourceEntry
    file: Path
    body: str
    line: int | None
    column: int | None
    kind: str  # "prompt" or "resource"


def _iter_resource_usage(manifest: Manifest, package_root: Path) -> Iterator[_ResourceUsage]:
    """Yield every ``${resource:...}`` reference reachable from this package."""
    for entry in manifest.prompts:
        path = package_root / entry.path
        if not path.is_file():
            continue
        try:
            doc = parse_prompt(_read_normalised(path), file=str(path), strict=False, validate_paths=False)
        except SchemaError:
            continue
        for rref in collect_resource_refs(doc.namespace, file_fallback=str(path)):
            yield _ResourceUsage(entry, path, rref.body, rref.line, rref.column, "prompt")

    for entry in manifest.resources:
        path = package_root / entry.path
        if not path.is_file():
            continue
        try:
            md_doc = read_markdown(str(path), strict=False)
        except SchemaError:
            continue
        for mref in md_doc.references:
            yield _ResourceUsage(entry, path, mref.raw, mref.line, mref.column, "resource")


def collect_cross_package_refs(manifest: Manifest, package_root: Path) -> set[tuple[str, str]]:
    """Collect cross-package (name, version) pairs from ancestor and
    ``${resource:...}`` references found in this package's payloads.

    Used by :func:`check_consistency` to diff against ``package.json`` deps.
    """
    refs: set[tuple[str, str]] = set()
    for entry in manifest.prompts:
        path = package_root / entry.path
        if not path.is_file():
            continue
        try:
            doc = parse_prompt(_read_normalised(path), file=str(path), strict=False, validate_paths=False)
        except SchemaError:
            continue
        for ref in doc.ancestors:
            if isinstance(ref, CoordRef):
                refs.add((ref.package, ref.version))
    for usage in _iter_resource_usage(manifest, package_root):
        coord = _parse_coordinate_body(usage.body)
        if coord is not None and coord[0] != manifest.name:
            refs.add((coord[0], coord[1]))
    return refs


def check_consistency(
    manifest: Manifest,
    package_root: Path,
    *,
    manifest_file: str,
) -> list[PromptCliError]:
    """Compare the manifest's ``dependencies`` map against the cross-package
    references discovered in the prompt payloads.

    Returns a list of ``SchemaError`` entries — one per offence — covering:

    - **missing**: a (pkg, version) reference appears in a prompt but the
      package is not declared in ``dependencies`` at all.
    - **version_mismatch**: ``dependencies[pkg]`` is pinned to a different
      version than the one referenced by a prompt.
    - **multiple_versions**: two prompts in the same package reference the
      same dependency at different versions (the manifest can only pin one).
    - **extra**: ``dependencies`` declares a package that no prompt in the
      package actually references.
    """
    errors: list[PromptCliError] = []
    refs = collect_cross_package_refs(manifest, package_root)

    # Group referenced versions per package.
    versions_per_pkg: dict[str, set[str]] = {}
    for pkg, ver in refs:
        versions_per_pkg.setdefault(pkg, set()).add(ver)

    declared = dict(manifest.dependencies)

    for pkg, vers in versions_per_pkg.items():
        if len(vers) > 1:
            errors.append(SchemaError(
                f"package {pkg!r} is referenced at multiple versions {sorted(vers)} across prompts; "
                f"package.json 'dependencies' can only pin one version",
                file=manifest_file,
                field_name=f"dependencies.{pkg}",
                reason="multiple_versions_referenced",
            ))
            continue
        ver = next(iter(vers))
        if pkg not in declared:
            errors.append(SchemaError(
                f"prompts reference {pkg}@{ver} but it is not declared in package.json 'dependencies'",
                file=manifest_file,
                field_name=f"dependencies.{pkg}",
                reason="missing_dependency",
            ))
        elif declared[pkg] != ver:
            errors.append(SchemaError(
                f"package.json 'dependencies[{pkg!r}]' is pinned to {declared[pkg]!r} "
                f"but a prompt references {ver!r}",
                file=manifest_file,
                field_name=f"dependencies.{pkg}",
                reason="version_mismatch",
            ))

    referenced = set(versions_per_pkg.keys())
    for pkg in declared:
        if pkg not in referenced:
            errors.append(SchemaError(
                f"package.json 'dependencies' declares {pkg!r} but no prompt references it",
                file=manifest_file,
                field_name=f"dependencies.{pkg}",
                reason="unused_dependency",
            ))

    errors.extend(check_local_refs(manifest, package_root, manifest_file=manifest_file))

    return errors


def check_local_refs(
    manifest: Manifest,
    package_root: Path,
    *,
    manifest_file: str,
) -> list[PromptCliError]:
    """Ensure every relative-path ancestor and every relative
    ``${resource:...}`` reference in a manifest-declared file resolves to a
    path that is itself declared in the manifest.
    """
    errors: list[PromptCliError] = []
    declared_prompt_paths = {p.path.replace("\\", "/").casefold() for p in manifest.prompts}
    declared_resource_paths = {r.path.replace("\\", "/").casefold() for r in manifest.resources}

    def _resolve_local_resource(entry_path: str, body: str) -> str | None:
        if body.startswith("/"):
            return None
        base_dir = posixpath.dirname(entry_path.replace("\\", "/"))
        joined = posixpath.normpath(posixpath.join(base_dir, body))
        if joined.startswith("..") or joined.startswith("/"):
            return None
        return joined

    for entry in manifest.prompts:
        prompt_file = package_root / entry.path
        if not prompt_file.is_file():
            continue
        try:
            doc = parse_prompt(_read_normalised(prompt_file), file=str(prompt_file), strict=False, validate_paths=False)
        except SchemaError:
            continue
        for ref in doc.ancestors:
            if not isinstance(ref, PathRef):
                continue
            resolved = resolve_relative(entry.path, ref.raw)
            if resolved.casefold() not in declared_prompt_paths:
                errors.append(SchemaError(
                    f"prompt {entry.id!r} references local file {ref.raw!r} "
                    f"(resolved to {resolved!r}) which is not declared in "
                    f"package.json 'prompts'",
                    file=str(prompt_file),
                    field_name="ancestors",
                    reason="undeclared_local_ref",
                ))

    for usage in _iter_resource_usage(manifest, package_root):
        if _parse_coordinate_body(usage.body) is not None:
            continue
        resolved = _resolve_local_resource(usage.entry.path, usage.body)
        if resolved is None:
            errors.append(SchemaError(
                f"{usage.kind} {usage.entry.id!r} has ${{resource:{usage.body}}} that escapes the package root or is absolute",
                file=str(usage.file),
                line=usage.line,
                column=usage.column,
                field_name="<resource>",
                reason="resource_ref_escape",
            ))
            continue
        if resolved.casefold() not in declared_resource_paths:
            errors.append(SchemaError(
                f"{usage.kind} {usage.entry.id!r} references resource {usage.body!r} "
                f"(resolved to {resolved!r}) which is not declared in "
                f"package.json 'resources'",
                file=str(usage.file),
                line=usage.line,
                column=usage.column,
                field_name="<resource>",
                reason="undeclared_local_resource_ref",
            ))
    return errors
