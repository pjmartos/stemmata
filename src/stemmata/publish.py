from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from stemmata.bundle import (
    BundleMember,
    build_tarball,
    collect_members,
    integrity_sha512,
    shasum_sha1,
    tarball_filename,
)
from stemmata.cache import Cache, default_cache_dir
from stemmata.deps_check import check_consistency
from stemmata.errors import (
    AggregatedError,
    MergeError,
    NetworkError,
    OfflineError,
    PromptCliError,
    SchemaError,
    UnresolvableError,
)
from stemmata.interp import (
    Layer,
    _exact_placeholder,
    _is_scalar,
    _parse_placeholder_tokens,
    lookup_with_provenance,
)
from stemmata.manifest import Manifest, parse_manifest
from stemmata.merge import merge_namespaces
from stemmata.npmrc import NpmConfig
from stemmata.registry import RegistryClient
from stemmata.resolver import Session, layer_order, resolve_graph
from stemmata.schema_check import SchemaCheckOptions, validate_against_schema
from stemmata.yaml_loader import scalar_meta


@dataclass
class PublishOptions:
    package_root: Path
    dry_run: bool = False
    strict_schema: bool = False
    tarball_out: Path | None = None
    config: NpmConfig | None = None
    offline: bool = False
    refresh: bool = False
    http_timeout: float = 30.0
    cache_root: Path | None = None
    max_prompts: int = 1000
    max_depth: int = 50
    max_download_bytes: int = 64 * 1024 * 1024
    max_total_bytes: int = 512 * 1024 * 1024
    verbose: bool = False
    stderr: Any = None


@dataclass
class PublishResult:
    name: str
    version: str
    tarball_path: str | None
    tarball_size: int
    integrity: str
    shasum: str
    uploaded: bool
    registry_url: str | None
    prompts_checked: list[str] = field(default_factory=list)


def _walk_collect_placeholder_errors(
    node: Any,
    namespace: Any,
    layers: list[Layer],
    *,
    parent_is_list: bool,
    root_file: str,
    out: list[PromptCliError],
) -> None:
    """Mirror of ``interp._interp`` that collects every placeholder failure
    instead of raising on the first one."""
    if isinstance(node, dict):
        for v in node.values():
            _walk_collect_placeholder_errors(
                v, namespace, layers,
                parent_is_list=False, root_file=root_file, out=out,
            )
        return
    if isinstance(node, list):
        for item in node:
            _walk_collect_placeholder_errors(
                item, namespace, layers,
                parent_is_list=True, root_file=root_file, out=out,
            )
        return
    if not isinstance(node, str):
        return

    file, line, column, is_flow = scalar_meta(node)
    file = file or root_file
    exact, non_splat, inner_path = _exact_placeholder(node)
    if exact and is_flow:
        path = inner_path.strip()
        value, status, provider, searched = lookup_with_provenance(namespace, layers, path)
        if status == "not_provided":
            out.append(UnresolvableError(
                path, file=file, line=line, column=column,
                reason="not_provided", ancestors_searched=searched, providing_ancestor=None,
            ))
        elif status == "explicit_null":
            out.append(UnresolvableError(
                path, file=file, line=line, column=column,
                reason="explicit_null", ancestors_searched=searched, providing_ancestor=provider,
            ))
        return

    tokens = _parse_placeholder_tokens(str(node))
    for kind, val in tokens:
        if kind != "ph":
            continue
        inner = val
        if inner.startswith("="):
            inner = inner[1:]
        inner = inner.strip()
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
        elif not _is_scalar(value):
            out.append(MergeError(
                path=inner,
                conflict="non_scalar_in_textual",
                types=[type(value).__name__],
                nodes=[{"file": file, "line": line, "column": column, "ancestor": root_file}],
            ))


def _check_one_prompt(
    prompt_path: Path,
    canonical_id: str,
    opts: PublishOptions,
    schema_opts: SchemaCheckOptions,
    config: NpmConfig,
) -> list[PromptCliError]:
    """Run cycle / type / placeholder / $schema checks on a single prompt.

    Each prompt is resolved against a fresh session so version-override state
    from one prompt's resolution does not bleed into the next.
    """
    errors: list[PromptCliError] = []

    cache_root = opts.cache_root or default_cache_dir()
    cache = Cache(root=cache_root)
    registry = RegistryClient(config=config, offline=opts.offline, http_timeout=opts.http_timeout)
    session = Session(
        cache=cache,
        registry=registry,
        refresh=opts.refresh,
        max_prompts=opts.max_prompts,
        max_depth=opts.max_depth,
        max_download_bytes=opts.max_download_bytes,
        max_total_bytes=opts.max_total_bytes,
        verbose=opts.verbose,
        stderr=opts.stderr,
    )

    try:
        graph = resolve_graph(str(prompt_path), session)
    except PromptCliError as e:
        # Cycles, missing references, schema problems in ancestors, network /
        # offline errors all surface here. Each is fatal for *this* prompt's
        # check; we cannot meaningfully run the placeholder pass without a
        # resolved graph, so we return early with what we have.
        errors.append(e)
        return errors

    order = layer_order(graph)
    layers_data = [graph.nodes[nid].doc.namespace for nid in order]
    provenance = [(nid.canonical, graph.nodes[nid].file) for nid in order]
    try:
        merged = merge_namespaces(layers_data, provenance=provenance)
    except PromptCliError as e:
        errors.append(e)
        return errors

    layers = [Layer(canonical_id=nid.canonical, data=graph.nodes[nid].doc.namespace) for nid in order]
    placeholder_errors: list[PromptCliError] = []
    _walk_collect_placeholder_errors(
        merged, merged, layers,
        parent_is_list=False,
        root_file=graph.nodes[graph.root_id].file,
        out=placeholder_errors,
    )
    errors.extend(placeholder_errors)

    schema_uri = graph.nodes[graph.root_id].doc.schema_uri
    if schema_uri:
        # Schema validation runs against the resolved-but-not-interpolated
        # namespace by design: $schema describes the prompt's content
        # contract, which authors reason about in terms of the merged tree.
        errors.extend(validate_against_schema(
            merged, schema_uri,
            file=str(prompt_path),
            opts=schema_opts,
        ))

    return errors


def run_publish(opts: PublishOptions) -> PublishResult:
    package_root = opts.package_root.resolve()
    manifest_file = package_root / "package.json"
    if not manifest_file.is_file():
        raise SchemaError(
            f"package.json not found at {manifest_file}",
            file=str(manifest_file),
            field_name="package.json",
            reason="missing_manifest",
        )
    manifest_text = manifest_file.read_text(encoding="utf-8")
    manifest = parse_manifest(manifest_text, file=str(manifest_file))
    manifest_data = json.loads(manifest_text)

    config = opts.config if opts.config is not None else NpmConfig(entries={})
    schema_opts = SchemaCheckOptions(
        offline=opts.offline,
        strict=opts.strict_schema,
        refresh=opts.refresh,
        http_timeout=opts.http_timeout,
        cache_root=opts.cache_root,
        stderr=opts.stderr,
    )

    aggregated: list[PromptCliError] = []
    checked_ids: list[str] = []

    for entry in manifest.prompts:
        prompt_path = package_root / entry.path
        canonical = f"{manifest.name}@{manifest.version}#{entry.id}"
        checked_ids.append(canonical)
        per_prompt = _check_one_prompt(prompt_path, canonical, opts, schema_opts, config)
        aggregated.extend(per_prompt)

    aggregated.extend(check_consistency(manifest, package_root, manifest_file=str(manifest_file)))

    if aggregated:
        raise AggregatedError(aggregated, command="publish")

    extra_files: list[str] = ["package.json"]
    for optional in ("README.md", "LICENSE", "LICENSE.md", "LICENSE.txt"):
        if (package_root / optional).is_file():
            extra_files.append(optional)
    yaml_paths = [entry.path for entry in manifest.prompts]
    members = collect_members(package_root, extra_files, yaml_paths)
    tarball_bytes = build_tarball(members)
    integrity = integrity_sha512(tarball_bytes)
    shasum = shasum_sha1(tarball_bytes)

    written_path: str | None = None
    if opts.tarball_out is not None:
        opts.tarball_out.parent.mkdir(parents=True, exist_ok=True)
        opts.tarball_out.write_bytes(tarball_bytes)
        written_path = str(opts.tarball_out)
    elif opts.dry_run:
        # In a dry run with no explicit output path, drop the tarball next to
        # the package so the user can inspect it.
        default_out = package_root / tarball_filename(manifest.name, manifest.version)
        default_out.write_bytes(tarball_bytes)
        written_path = str(default_out)

    uploaded = False
    registry_url: str | None = None
    if not opts.dry_run:
        registry = RegistryClient(config=config, offline=opts.offline, http_timeout=opts.http_timeout)
        url, _ = registry.publish_tarball(
            manifest.name, manifest.version, tarball_bytes,
            manifest=manifest_data,
        )
        uploaded = True
        registry_url = url

    return PublishResult(
        name=manifest.name,
        version=manifest.version,
        tarball_path=written_path,
        tarball_size=len(tarball_bytes),
        integrity=integrity,
        shasum=shasum,
        uploaded=uploaded,
        registry_url=registry_url,
        prompts_checked=checked_ids,
    )
