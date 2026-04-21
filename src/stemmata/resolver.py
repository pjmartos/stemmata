from __future__ import annotations

import os
import sys
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from stemmata.cache import Cache
from stemmata.errors import (
    CycleError,
    ReferenceError_,
    SchemaError,
)
from stemmata.manifest import Manifest, parse_manifest
from stemmata.prompt_doc import (
    CoordRef,
    PathRef,
    PromptDocument,
    parse_prompt,
    resolve_relative,
)
from stemmata.registry import RegistryClient
from stemmata.yaml_loader import attach_file


@dataclass(frozen=True)
class NodeId:
    coord: tuple[str, str, str] | None
    file_key: str | None

    @staticmethod
    def for_coord(pkg: str, version: str, prompt_id: str) -> "NodeId":
        return NodeId(coord=(pkg, version, prompt_id), file_key=None)

    @staticmethod
    def for_file(abs_path: str) -> "NodeId":
        return NodeId(coord=None, file_key=abs_path)

    @property
    def canonical(self) -> str:
        if self.coord is not None:
            p, v, i = self.coord
            return f"{p}@{v}#{i}"
        return self.file_key or ""

    @property
    def package(self) -> str | None:
        return self.coord[0] if self.coord else None

    @property
    def version(self) -> str | None:
        return self.coord[1] if self.coord else None

    @property
    def prompt_id(self) -> str | None:
        return self.coord[2] if self.coord else None


@dataclass
class Node:
    id: NodeId
    doc: PromptDocument
    file: str
    children: list[NodeId] = field(default_factory=list)
    manifest: Manifest | None = None
    package_root: Path | None = None


@dataclass
class Session:
    cache: Cache
    registry: RegistryClient
    refresh: bool = False
    max_prompts: int = 1000
    max_depth: int = 50
    max_download_bytes: int = 64 * 1024 * 1024
    max_total_bytes: int = 512 * 1024 * 1024
    verbose: bool = False
    stderr: Any = None
    strict_parse: bool = True
    _total_downloaded: int = 0
    _manifest_by_pkg: dict[tuple[str, str], tuple[Manifest, Path]] = field(default_factory=dict)
    _refreshed: set[tuple[str, str]] = field(default_factory=set)
    version_overrides: dict[str, str] = field(default_factory=dict)

    def ensure_package(self, name: str, version: str) -> tuple[Manifest, Path]:
        key = (name, version)
        if key in self._manifest_by_pkg:
            return self._manifest_by_pkg[key]
        pkg_root = self.cache.package_dir(name, version)
        needs_refresh = self.refresh and key not in self._refreshed
        if not self.cache.has_package(name, version) or needs_refresh:
            with self.cache.lock(name, version):
                if not self.cache.has_package(name, version) or needs_refresh:
                    url, data = self.registry.fetch_tarball(name, version)
                    if len(data) > self.max_download_bytes:
                        raise SchemaError(
                            f"package {name}@{version} tarball exceeds size limit ({len(data)} > {self.max_download_bytes})",
                            file=url,
                            field_name="tarball",
                            reason="download_size_limit",
                        )
                    self._total_downloaded += len(data)
                    if self._total_downloaded > self.max_total_bytes:
                        raise SchemaError(
                            f"total downloaded bytes exceed limit ({self._total_downloaded} > {self.max_total_bytes})",
                            file=url,
                            field_name="tarball",
                            reason="total_size_limit",
                        )
                    self.cache.install_tarball(name, version, data, force=needs_refresh)
                    self._refreshed.add(key)
        manifest_file = pkg_root / "package.json"
        if not manifest_file.exists():
            raise SchemaError(
                f"package {name}@{version} missing package.json",
                file=str(manifest_file),
                field_name="package.json",
                reason="missing_manifest",
            )
        manifest = parse_manifest(manifest_file.read_text(encoding="utf-8"), file=str(manifest_file))
        if manifest.name != name:
            raise SchemaError(
                f"package.json 'name' mismatch: expected {name!r} got {manifest.name!r}",
                file=str(manifest_file),
                field_name="name",
                reason="name_mismatch",
            )
        if manifest.version != version:
            raise SchemaError(
                f"package.json 'version' mismatch: expected {version!r} got {manifest.version!r}",
                file=str(manifest_file),
                field_name="version",
                reason="version_mismatch",
            )
        self._manifest_by_pkg[key] = (manifest, pkg_root)
        return manifest, pkg_root


@dataclass
class ResolvedGraph:
    root_id: NodeId
    nodes: dict[NodeId, Node]
    order: list[NodeId]
    distances: dict[NodeId, int]


_BOM_BYTES = b"\xef\xbb\xbf"


def _read_payload_text(file_path: str, *, strict: bool) -> str:
    if strict:
        return Path(file_path).read_text(encoding="utf-8")
    raw = Path(file_path).read_bytes()
    if raw.startswith(_BOM_BYTES):
        raw = raw[len(_BOM_BYTES):]
    if b"\r" in raw:
        raw = raw.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    return raw.decode("utf-8")


def _load_prompt_file(file_path: str, *, strict: bool = True) -> PromptDocument:
    p = Path(file_path)
    if not p.exists():
        raise ReferenceError_(
            f"prompt file does not exist: {file_path}",
            file=file_path,
            line=None,
            column=None,
            reference=file_path,
            searched_in=str(p.parent),
        )
    text = _read_payload_text(file_path, strict=strict)
    try:
        doc = parse_prompt(text, file=file_path, strict=strict, validate_paths=False)
    except SchemaError:
        raise
    attach_file(doc.namespace, file_path)
    return doc


def _load_registry_prompt(session: Session, pkg: str, version: str, prompt_id: str) -> tuple[PromptDocument, Manifest, Path, str]:
    manifest, pkg_root = session.ensure_package(pkg, version)
    entry = manifest.prompt_by_id(prompt_id)
    if entry is None:
        raise ReferenceError_(
            f"package {pkg}@{version} does not contain prompt id {prompt_id!r}",
            file=str(pkg_root / "package.json"),
            line=None,
            column=None,
            reference=f"{pkg}@{version}#{prompt_id}",
            searched_in=f"{pkg}@{version}",
        )
    prompt_file = pkg_root / entry.path
    if not prompt_file.exists():
        raise SchemaError(
            f"prompt file {entry.path} declared by manifest does not exist",
            file=str(prompt_file),
            field_name="path",
            reason="missing_prompt_file",
        )
    text = _read_payload_text(str(prompt_file), strict=session.strict_parse)
    doc = parse_prompt(text, file=str(prompt_file), strict=session.strict_parse)
    canonical = f"{pkg}@{version}#{prompt_id}"
    attach_file(doc.namespace, canonical)
    return doc, manifest, pkg_root, canonical


def _canonical_path(p: str) -> str:
    return os.path.realpath(p)


def _collapse_to_package(abs_path: str, session: Session) -> NodeId | None:
    real = _canonical_path(abs_path)
    for (pkg, ver), (manifest, pkg_root) in list(session._manifest_by_pkg.items()):
        root_real = _canonical_path(str(pkg_root))
        if real == root_real or real.startswith(root_real + os.sep):
            rel = os.path.relpath(real, root_real).replace("\\", "/")
            entry = manifest.prompt_by_path(rel)
            if entry is None:
                return None
            return NodeId.for_coord(pkg, ver, entry.id)
    return None


def _resolve_ancestor_ref(
    ref: Any,
    referring_node: Node,
    session: Session,
) -> tuple[NodeId, PromptDocument, Manifest | None, Path | None, str]:
    if isinstance(ref, PathRef):
        if referring_node.manifest is not None and referring_node.package_root is not None:
            pkg_root = referring_node.package_root
            entry = referring_node.manifest.prompt_by_id(referring_node.id.prompt_id or "")
            if entry is None:
                raise SchemaError(
                    f"internal: prompt id {referring_node.id.prompt_id!r} not found in manifest",
                    file=referring_node.file,
                    field_name="ancestors",
                    reason="manifest_entry_missing",
                )
            joined = resolve_relative(entry.path, ref.raw)
            if joined.startswith("..") or joined.startswith("/"):
                raise SchemaError(
                    f"relative reference escapes package root: {ref.raw!r}",
                    file=referring_node.file,
                    field_name="ancestors",
                    reason="escape_root",
                )
            target_file = pkg_root / joined
            abs_target = str(target_file)
            collapsed = _collapse_to_package(abs_target, session)
            if collapsed is not None:
                pkg, ver, pid = collapsed.coord  # type: ignore[misc]
                doc, manifest, root, canonical = _load_registry_prompt(session, pkg, ver, pid)
                return collapsed, doc, manifest, root, canonical
            raise ReferenceError_(
                f"relative reference {ref.raw!r} does not resolve to a manifest entry",
                file=referring_node.file,
                line=None,
                column=None,
                reference=ref.raw,
                searched_in=f"{referring_node.manifest.name}@{referring_node.manifest.version}",
            )
        base_dir = os.path.dirname(referring_node.file)
        target = os.path.normpath(os.path.join(base_dir, ref.raw))
        collapsed = _collapse_to_package(target, session)
        if collapsed is not None:
            pkg, ver, pid = collapsed.coord  # type: ignore[misc]
            doc, manifest, root, canonical = _load_registry_prompt(session, pkg, ver, pid)
            return collapsed, doc, manifest, root, canonical
        real = _canonical_path(target)
        doc = _load_prompt_file(real, strict=session.strict_parse)
        return NodeId.for_file(real), doc, None, None, real
    assert isinstance(ref, CoordRef)
    effective_version = session.version_overrides.get(ref.package, ref.version)
    doc, manifest, pkg_root, canonical = _load_registry_prompt(session, ref.package, effective_version, ref.prompt)
    return NodeId.for_coord(ref.package, effective_version, ref.prompt), doc, manifest, pkg_root, canonical


def _bfs_build(root_id: NodeId, root_node: Node, session: Session) -> tuple[dict[NodeId, Node], list[NodeId], dict[NodeId, int]]:
    nodes: dict[NodeId, Node] = {root_id: root_node}
    order: list[NodeId] = [root_id]
    distances: dict[NodeId, int] = {root_id: 0}
    queue: deque[NodeId] = deque([root_id])
    while queue:
        cur_id = queue.popleft()
        cur = nodes[cur_id]
        cur_dist = distances[cur_id]
        if cur_dist >= session.max_depth and cur.doc.ancestors:
            raise SchemaError(
                f"ancestor chain exceeds max depth {session.max_depth}",
                file=cur.file,
                field_name="ancestors",
                reason="max_depth",
            )
        resolved_children: list[NodeId] = []
        for ref in cur.doc.ancestors:
            child_id, doc, manifest, pkg_root, canonical = _resolve_ancestor_ref(ref, cur, session)
            resolved_children.append(child_id)
            if child_id not in nodes:
                if len(nodes) >= session.max_prompts:
                    raise SchemaError(
                        f"graph exceeds max prompts {session.max_prompts}",
                        file=canonical,
                        field_name="ancestors",
                        reason="max_prompts",
                    )
                nodes[child_id] = Node(
                    id=child_id,
                    doc=doc,
                    file=canonical,
                    children=[],
                    manifest=manifest,
                    package_root=pkg_root,
                )
                order.append(child_id)
                distances[child_id] = cur_dist + 1
                queue.append(child_id)
        if session.verbose:
            seen_children: set[NodeId] = set()
            stream = session.stderr if session.stderr is not None else sys.stderr
            for child_id in resolved_children:
                if child_id in seen_children:
                    stream.write(
                        f"warning: duplicate direct ancestor {child_id.canonical} in {cur.file}\n"
                    )
                seen_children.add(child_id)
        cur.children = resolved_children
    return nodes, order, distances


def _detect_cycles(nodes: dict[NodeId, Node], root_id: NodeId) -> None:
    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[NodeId, int] = {k: WHITE for k in nodes}
    stack_path: list[NodeId] = []

    def visit(nid: NodeId) -> None:
        if color[nid] == GRAY:
            idx = stack_path.index(nid)
            cycle = stack_path[idx:] + [nid]
            ids = [c.canonical for c in cycle]
            nodes_out = [{"file": nodes[c].file, "line": None, "column": None} for c in cycle]
            raise CycleError(nodes_out, ids)
        if color[nid] == BLACK:
            return
        color[nid] = GRAY
        stack_path.append(nid)
        for child in nodes[nid].children:
            visit(child)
        stack_path.pop()
        color[nid] = BLACK

    visit(root_id)
    for nid in list(nodes.keys()):
        if color[nid] == WHITE:
            visit(nid)


def resolve_graph(
    root_target: str,
    session: Session,
) -> ResolvedGraph:
    for _ in range(32):
        root_id, root_node = _load_root(root_target, session)
        nodes, order, distances = _bfs_build(root_id, root_node, session)
        _detect_cycles(nodes, root_id)
        new_overrides = _compute_version_overrides(nodes, order, distances)
        if new_overrides == session.version_overrides:
            return ResolvedGraph(root_id=root_id, nodes=nodes, order=order, distances=distances)
        session.version_overrides = new_overrides
    raise SchemaError(
        "version conflict resolution did not converge",
        file=root_target,
        field_name="resolution",
        reason="no_convergence",
    )


def resolve_from_document(
    doc: PromptDocument,
    file_path: str,
    session: Session,
) -> ResolvedGraph:
    """Like :func:`resolve_graph` but starts from a pre-parsed document."""
    abs_path = _canonical_path(file_path)
    attach_file(doc.namespace, abs_path)
    root_id = NodeId.for_file(abs_path)
    root_node = Node(id=root_id, doc=doc, file=abs_path)
    for _ in range(32):
        nodes, order, distances = _bfs_build(root_id, root_node, session)
        _detect_cycles(nodes, root_id)
        new_overrides = _compute_version_overrides(nodes, order, distances)
        if new_overrides == session.version_overrides:
            return ResolvedGraph(root_id=root_id, nodes=nodes, order=order, distances=distances)
        session.version_overrides = new_overrides
    raise SchemaError("version conflict resolution did not converge",
                      file=file_path, field_name="resolution", reason="no_convergence")


def _compute_version_overrides(
    nodes: dict[NodeId, Node],
    order: list[NodeId],
    distances: dict[NodeId, int],
) -> dict[str, str]:
    by_pkg: dict[str, dict[str, tuple[int, int]]] = {}
    for i, nid in enumerate(order):
        if not nid.coord:
            continue
        pkg = nid.package or ""
        ver = nid.version or ""
        entry = by_pkg.setdefault(pkg, {})
        if ver not in entry:
            entry[ver] = (distances[nid], i)
    overrides: dict[str, str] = {}
    for pkg, ver_ranks in by_pkg.items():
        if len(ver_ranks) > 1:
            winner = min(ver_ranks, key=lambda v: ver_ranks[v])
            overrides[pkg] = winner
        else:
            overrides[pkg] = next(iter(ver_ranks.keys()))
    return overrides


def _load_root(root_target: str, session: Session) -> tuple[NodeId, Node]:
    if root_target.startswith("@") and "@" in root_target[1:] and "#" in root_target:
        pkg, rest = root_target.rsplit("@", 1)
        version, _, prompt_id = rest.partition("#")
        effective_version = session.version_overrides.get(pkg, version)
        doc, manifest, pkg_root, canonical = _load_registry_prompt(session, pkg, effective_version, prompt_id)
        nid = NodeId.for_coord(pkg, effective_version, prompt_id)
        return nid, Node(id=nid, doc=doc, file=canonical, manifest=manifest, package_root=pkg_root)
    abs_path = _canonical_path(root_target)
    doc = _load_prompt_file(abs_path, strict=session.strict_parse)
    nid = NodeId.for_file(abs_path)
    return nid, Node(id=nid, doc=doc, file=abs_path)


def layer_order(graph: ResolvedGraph) -> list[NodeId]:
    enqueue_pos: dict[NodeId, int] = {nid: i for i, nid in enumerate(graph.order)}
    reachable: set[NodeId] = set()
    queue: deque[NodeId] = deque([graph.root_id])
    while queue:
        cur = queue.popleft()
        if cur in reachable:
            continue
        reachable.add(cur)
        for child in graph.nodes[cur].children:
            if child not in reachable:
                queue.append(child)
    result = sorted(reachable, key=lambda nid: (graph.distances.get(nid, 0), enqueue_pos.get(nid, 0)))
    return result
