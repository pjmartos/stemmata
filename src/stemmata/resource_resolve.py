"""Resource-graph resolution"""
from __future__ import annotations

import os
import posixpath
from collections import deque
from dataclasses import dataclass
from pathlib import Path

from stemmata.errors import CycleError, ReferenceError_, SchemaError
from stemmata.interp import ResourceBinding
from stemmata.manifest import Manifest, is_scoped_name, is_semver, parse_manifest
from stemmata.resource_loader import ResourceDocument, read_resource
from stemmata.prompt_doc import collect_resource_refs


@dataclass(frozen=True)
class ResourceCoord:
    package: str
    version: str
    resource_id: str

    @property
    def canonical(self) -> str:
        return f"{self.package}@{self.version}#{self.resource_id}"


@dataclass
class _ResourceNode:
    coord: ResourceCoord
    file_path: Path
    doc: ResourceDocument
    children: list[str]


def _parse_coordinate_body(body: str) -> tuple[str, str, str] | None:
    body = body.strip()
    if not body.startswith("@") or body.count("@") < 2 or "#" not in body:
        return None
    pkg, ver_and_id = body.rsplit("@", 1)
    if "#" not in ver_and_id:
        return None
    version, _, resource_id = ver_and_id.partition("#")
    if not is_scoped_name(pkg) or not is_semver(version) or not resource_id:
        return None
    return pkg, version, resource_id


def _ref_error(
    body: str,
    *,
    referring_file: str,
    searched_in: str,
    reason: str,
) -> ReferenceError_:
    placeholder = f"${{resource:{body}}}"
    if reason == "type_mismatch":
        message = f"{placeholder} targets a prompt, not a resource"
    else:
        message = f"unresolved resource reference {placeholder}"
    return ReferenceError_(
        message,
        file=referring_file,
        line=None,
        column=None,
        reference=placeholder,
        searched_in=searched_in,
        kind="resource",
        reason=reason,
    )


def _resolve_body_to_coord(
    body: str,
    *,
    referring_file: str,
    referring_manifest: Manifest | None,
    referring_package_root: Path | None,
    referring_entry_path: str | None,
    session,
) -> tuple[ResourceCoord, Path]:
    coord_parts = _parse_coordinate_body(body)
    if coord_parts is not None:
        pkg, version, resource_id = coord_parts
        effective_version = session.version_overrides.get(pkg, version)
        manifest, pkg_root = session.ensure_package(pkg, effective_version)
        searched = f"{pkg}@{effective_version}"
        entry = manifest.resource_by_id(resource_id)
        if entry is not None:
            return ResourceCoord(pkg, effective_version, resource_id), pkg_root / entry.path
        reason = "type_mismatch" if manifest.prompt_by_id(resource_id) is not None else "missing"
        raise _ref_error(body, referring_file=referring_file, searched_in=searched, reason=reason)

    if body.startswith("/"):
        raise SchemaError(
            f"relative resource reference must not be absolute: {body!r}",
            file=referring_file,
            line=None,
            column=None,
            field_name="<resource>",
            reason="absolute_path",
        )
    if referring_manifest is None or referring_package_root is None or referring_entry_path is None:
        raise _ref_error(body, referring_file=referring_file, searched_in="<local>", reason="missing")
    base_dir = posixpath.dirname(referring_entry_path.replace("\\", "/"))
    joined = posixpath.normpath(posixpath.join(base_dir, body))
    if joined.startswith("..") or joined.startswith("/"):
        raise _ref_error(body, referring_file=referring_file, searched_in="<local>", reason="missing")
    entry = referring_manifest.resource_by_path(joined)
    if entry is not None:
        coord = ResourceCoord(referring_manifest.name, referring_manifest.version, entry.id)
        return coord, referring_package_root / entry.path
    reason = "type_mismatch" if referring_manifest.prompt_by_path(joined) is not None else "missing"
    raise _ref_error(body, referring_file=referring_file, searched_in="<local>", reason=reason)


def _find_package_for_local_file(file_path: str, session) -> tuple[Manifest, Path, str] | None:
    real = os.path.realpath(file_path)
    for (_pkg, _ver), (manifest, pkg_root) in list(session._manifest_by_pkg.items()):
        root_real = os.path.realpath(str(pkg_root))
        if real == root_real or real.startswith(root_real + os.sep):
            rel = os.path.relpath(real, root_real).replace("\\", "/")
            return manifest, pkg_root, rel
    current = Path(real).parent
    while current.parent != current:
        manifest_path = current / "package.json"
        if manifest_path.is_file():
            try:
                manifest = parse_manifest(
                    manifest_path.read_text(encoding="utf-8"),
                    file=str(manifest_path),
                )
            except SchemaError:
                return None
            session._manifest_by_pkg.setdefault(
                (manifest.name, manifest.version), (manifest, current)
            )
            rel = os.path.relpath(real, current).replace("\\", "/")
            return manifest, current, rel
        current = current.parent
    return None


def _prompt_referring_context(
    prompt_node,
    session,
) -> tuple[Manifest | None, Path | None, str | None]:
    if prompt_node.manifest is not None:
        pid = prompt_node.id.prompt_id
        entry = prompt_node.manifest.prompt_by_id(pid) if pid else None
        return prompt_node.manifest, prompt_node.package_root, entry.path if entry else None
    collapsed = _find_package_for_local_file(prompt_node.file, session)
    if collapsed is None:
        return None, None, None
    return collapsed


def _detect_cycles(nodes: dict[str, _ResourceNode]) -> None:
    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[str, int] = {k: WHITE for k in nodes}
    stack: list[str] = []

    def visit(canonical: str) -> None:
        if color[canonical] == GRAY:
            idx = stack.index(canonical)
            cycle = stack[idx:] + [canonical]
            payload = [{"file": nodes[c].coord.canonical, "line": None, "column": None} for c in cycle]
            raise CycleError(payload, cycle, kind="resource")
        if color[canonical] == BLACK:
            return
        color[canonical] = GRAY
        stack.append(canonical)
        for child in nodes[canonical].children:
            visit(child)
        stack.pop()
        color[canonical] = BLACK

    for canonical in list(nodes.keys()):
        if color[canonical] == WHITE:
            visit(canonical)


def _flatten_all(nodes: dict[str, _ResourceNode]) -> dict[str, str]:
    memo: dict[str, str] = {}

    def flatten(canonical: str) -> str:
        if canonical in memo:
            return memo[canonical]
        node = nodes[canonical]
        content = node.doc.content
        for ref, child_canonical in zip(node.doc.references, node.children):
            content = content.replace(ref.text, flatten(child_canonical), 1)
        memo[canonical] = content
        return content

    for canonical in nodes:
        flatten(canonical)
    return memo


def _is_under_cache(file_path: Path, session) -> bool:
    cache_root = getattr(getattr(session, "cache", None), "root", None)
    if cache_root is None:
        return False
    try:
        Path(file_path).resolve().relative_to(Path(cache_root).resolve())
        return True
    except ValueError:
        return False


def build_resource_binding(graph, session) -> ResourceBinding:
    binding = ResourceBinding()
    nodes: dict[str, _ResourceNode] = {}
    to_visit: deque[tuple[ResourceCoord, Path]] = deque()
    seen: set[str] = set()

    def _schedule(coord: ResourceCoord, path: Path) -> None:
        if coord.canonical not in seen:
            to_visit.append((coord, path))
            seen.add(coord.canonical)

    for nid in graph.order:
        prompt_node = graph.nodes[nid]
        ref_manifest, ref_root, ref_entry_path = _prompt_referring_context(prompt_node, session)
        prompt_direct: list[str] = []
        prompt_seen: set[str] = set()
        for ref in collect_resource_refs(prompt_node.doc.namespace, file_fallback=prompt_node.file):
            coord, file_path = _resolve_body_to_coord(
                ref.body,
                referring_file=ref.file,
                referring_manifest=ref_manifest,
                referring_package_root=ref_root,
                referring_entry_path=ref_entry_path,
                session=session,
            )
            binding.bindings[(ref.file, ref.body)] = coord.canonical
            if coord.canonical not in prompt_seen:
                prompt_direct.append(coord.canonical)
                prompt_seen.add(coord.canonical)
            _schedule(coord, file_path)
        if prompt_direct:
            binding.prompt_resources[nid.canonical] = prompt_direct

    while to_visit:
        coord, file_path = to_visit.popleft()
        if not file_path.exists():
            raise _ref_error(
                coord.canonical,
                referring_file=coord.canonical,
                searched_in=f"{coord.package}@{coord.version}",
                reason="missing",
            )
        doc = read_resource(str(file_path), strict=getattr(session, "strict_parse", True))
        manifest, pkg_root = session.ensure_package(coord.package, coord.version)
        entry = manifest.resource_by_id(coord.resource_id)
        entry_path = entry.path if entry is not None else None
        children: list[str] = []
        for ref in doc.references:
            child_coord, child_path = _resolve_body_to_coord(
                ref.raw,
                referring_file=coord.canonical,
                referring_manifest=manifest,
                referring_package_root=pkg_root,
                referring_entry_path=entry_path,
                session=session,
            )
            children.append(child_coord.canonical)
            _schedule(child_coord, child_path)
        nodes[coord.canonical] = _ResourceNode(coord=coord, file_path=file_path, doc=doc, children=children)
        unique_children: list[str] = []
        unique_seen: set[str] = set()
        for c in children:
            if c not in unique_seen:
                unique_children.append(c)
                unique_seen.add(c)
        binding.resource_children[coord.canonical] = unique_children
        binding.resource_files[coord.canonical] = (
            coord.canonical if _is_under_cache(file_path, session) else str(file_path)
        )

    _detect_cycles(nodes)
    binding.flat_texts.update(_flatten_all(nodes))
    return binding
