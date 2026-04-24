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
    AbstractUnfilledError,
    AggregatedError,
    PromptCliError,
    SchemaError,
)
from stemmata.interp import (
    Layer,
    ResourceBinding,
    collect_placeholder_errors,
    interpolate,
)
from stemmata.manifest import Manifest, parse_manifest
from stemmata.merge import merge_namespaces
from stemmata.npmrc import NpmConfig
from stemmata.registry import RegistryClient
from stemmata.resolver import Session, layer_order, resolve_graph
from stemmata.resource_resolve import build_resource_binding
from stemmata.schema_check import SchemaCheckOptions, resolve_schema_uri, validate_against_schema


@dataclass
class PublishOptions:
    package_root: Path
    dry_run: bool = False
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
    abstracts: list[dict[str, Any]] = field(default_factory=list)


def _check_one_prompt(
    prompt_path: Path,
    canonical_id: str,
    opts: PublishOptions,
    schema_opts: SchemaCheckOptions,
    config: NpmConfig,
    publish_package: tuple[Manifest, Path] | None = None,
) -> tuple[list[PromptCliError], list[AbstractUnfilledError]]:
    errors: list[PromptCliError] = []
    abstracts: list[AbstractUnfilledError] = []

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
        strict_parse=False,
    )
    if publish_package is not None:
        manifest, pkg_root = publish_package
        session._manifest_by_pkg[(manifest.name, manifest.version)] = (manifest, pkg_root)

    try:
        graph = resolve_graph(str(prompt_path), session)
    except PromptCliError as e:
        errors.append(e)
        return errors, abstracts

    order = layer_order(graph)
    layers_data = [graph.nodes[nid].doc.namespace for nid in order]
    provenance = [(nid.canonical, graph.nodes[nid].file) for nid in order]
    try:
        merged = merge_namespaces(layers_data, provenance=provenance)
    except PromptCliError as e:
        errors.append(e)
        return errors, abstracts

    layers = [Layer(canonical_id=nid.canonical, data=graph.nodes[nid].doc.namespace) for nid in order]
    diagnostics: list[Any] = []
    collect_placeholder_errors(
        merged, merged, layers,
        parent_is_list=False,
        root_file=graph.nodes[graph.root_id].file,
        out=diagnostics,
    )
    placeholder_errors = [e for e in diagnostics if not isinstance(e, AbstractUnfilledError)]
    abstracts = [e for e in diagnostics if isinstance(e, AbstractUnfilledError)]
    errors.extend(placeholder_errors)

    resources: ResourceBinding | None = None
    resource_errors: list[PromptCliError] = []
    try:
        resources = build_resource_binding(graph, session)
    except PromptCliError as e:
        resource_errors.append(e)
    errors.extend(resource_errors)

    schema_uri = graph.nodes[graph.root_id].doc.schema_uri
    if schema_uri:
        schema_uri = resolve_schema_uri(schema_uri, str(prompt_path))
        position_ns = graph.nodes[graph.root_id].doc.namespace
        if abstracts:
            return errors, abstracts
        if placeholder_errors or resource_errors:
            schema_target = merged
        else:
            root_file = graph.nodes[graph.root_id].file
            try:
                schema_target = interpolate(merged, layers, root_file=root_file, resources=resources)
            except PromptCliError as e:
                errors.append(e)
                schema_target = merged
        errors.extend(validate_against_schema(
            schema_target, schema_uri,
            file=str(prompt_path),
            opts=schema_opts,
            position_instance=position_ns,
        ))

    return errors, abstracts


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
        refresh=opts.refresh,
        http_timeout=opts.http_timeout,
        cache_root=opts.cache_root,
        stderr=opts.stderr,
    )

    aggregated: list[PromptCliError] = []
    checked_ids: list[str] = []
    abstracts_payload: list[dict[str, Any]] = []

    for entry in manifest.prompts:
        prompt_path = package_root / entry.path
        canonical = f"{manifest.name}@{manifest.version}#{entry.id}"
        checked_ids.append(canonical)
        per_errors, per_abstracts = _check_one_prompt(
            prompt_path, canonical, opts, schema_opts, config,
            publish_package=(manifest, package_root),
        )
        aggregated.extend(per_errors)
        if per_abstracts:
            paths = [a.details.get("placeholder") for a in per_abstracts]
            stderr = opts.stderr if opts.stderr is not None else sys.stderr
            stderr.write(
                f"warning: prompt {canonical} has {len(per_abstracts)} unfilled "
                f"abstract placeholder(s): {', '.join(str(p) for p in paths)}\n"
            )
            for a in per_abstracts:
                loc = a.location if isinstance(a.location, dict) else {}
                abstracts_payload.append({
                    "prompt": canonical,
                    "file": str(prompt_path),
                    "path": a.details.get("placeholder"),
                    "line": loc.get("line"),
                    "column": loc.get("column"),
                    "reason": a.details.get("reason"),
                })

    aggregated.extend(check_consistency(manifest, package_root, manifest_file=str(manifest_file)))

    if aggregated:
        raise AggregatedError(aggregated, command="publish")

    extra_files: list[str] = ["package.json"]
    for optional in ("README.md", "LICENSE", "LICENSE.md", "LICENSE.txt"):
        if (package_root / optional).is_file():
            extra_files.append(optional)
    yaml_paths = [entry.path for entry in manifest.prompts]
    markdown_paths = [entry.path for entry in manifest.resources]
    members = collect_members(package_root, extra_files, yaml_paths, markdown_paths)
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
        abstracts=abstracts_payload,
    )
