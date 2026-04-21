"""Resource-graph resolution"""
from __future__ import annotations

import os
import posixpath
from collections import deque
from dataclasses import dataclass
from pathlib import Path

from stemmata.errors import CycleError, ReferenceError_
from stemmata.interp import ResourceBinding
from stemmata.manifest import Manifest, is_scoped_name, is_semver
from stemmata.markdown_loader import MarkdownDocument, read_markdown
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
    doc: MarkdownDocument
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

    if referring_manifest is None or referring_package_root is None or referring_entry_path is None:
        raise _ref_error(body, referring_file=referring_file, searched_in=referring_file, reason="missing")
    searched = f"{referring_manifest.name}@{referring_manifest.version}"
    if body.startswith("/"):
        raise _ref_error(body, referring_file=referring_file, searched_in=searched, reason="missing")
    base_dir = posixpath.dirname(referring_entry_path.replace("\\", "/"))
    joined = posixpath.normpath(posixpath.join(base_dir, body))
    if joined.startswith("..") or joined.startswith("/"):
        raise _ref_error(body, referring_file=referring_file, searched_in=searched, reason="missing")
    entry = referring_manifest.resource_by_path(joined)
    if entry is not None:
        coord = ResourceCoord(referring_manifest.name, referring_manifest.version, entry.id)
        return coord, referring_package_root / entry.path
    reason = "type_mismatch" if referring_manifest.prompt_by_path(joined) is not None else "missing"
    raise _ref_error(body, referring_file=referring_file, searched_in=searched, reason=reason)


def _find_package_for_local_file(file_path: str, session) -> tuple[Manifest, Path, str] | None:
    real = os.path.realpath(file_path)
    for (_pkg, _ver), (manifest, pkg_root) in list(session._manifest_by_pkg.items()):
        root_real = os.path.realpath(str(pkg_root))
        if real == root_real or real.startswith(root_real + os.sep):
            rel = os.path.relpath(real, root_real).replace("\\", "/")
            return manifest, pkg_root, rel
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
            payload = [{"file": str(nodes[c].file_path), "line": None, "column": None} for c in cycle]
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
            _schedule(coord, file_path)

    while to_visit:
        coord, file_path = to_visit.popleft()
        if not file_path.exists():
            raise _ref_error(
                coord.canonical,
                referring_file=str(file_path),
                searched_in=f"{coord.package}@{coord.version}",
                reason="missing",
            )
        doc = read_markdown(str(file_path), strict=getattr(session, "strict_parse", True))
        manifest, pkg_root = session.ensure_package(coord.package, coord.version)
        entry = manifest.resource_by_id(coord.resource_id)
        entry_path = entry.path if entry is not None else None
        children: list[str] = []
        for ref in doc.references:
            child_coord, child_path = _resolve_body_to_coord(
                ref.raw,
                referring_file=str(file_path),
                referring_manifest=manifest,
                referring_package_root=pkg_root,
                referring_entry_path=entry_path,
                session=session,
            )
            children.append(child_coord.canonical)
            _schedule(child_coord, child_path)
        nodes[coord.canonical] = _ResourceNode(coord=coord, file_path=file_path, doc=doc, children=children)

    _detect_cycles(nodes)
    binding.flat_texts.update(_flatten_all(nodes))
    return binding
