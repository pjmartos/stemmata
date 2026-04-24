"""``stemmata validate`` — schema validation for prompt files."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from stemmata.errors import AbstractUnfilledError, AggregatedError, PromptCliError, SchemaError
from stemmata.interp import Layer, collect_placeholder_errors, interpolate
from stemmata.merge import merge_namespaces
from stemmata.prompt_doc import RESERVED_KEYS, parse_prompt
from stemmata.resolver import (
    Session,
    layer_order,
    resolve_from_document,
    resolve_graph,
)
from stemmata.resource_resolve import build_resource_binding
from stemmata.schema_check import (
    SchemaCheckOptions,
    resolve_schema_uri,
    validate_against_schema,
)
from stemmata.yaml_loader import _ScalarStr, load_all_with_positions


# ---------------------------------------------------------------------------
# file discovery
# ---------------------------------------------------------------------------

def discover_files(target: str) -> list[Path]:
    p = Path(target)
    if p.is_file():
        return [p]
    if not p.is_dir():
        raise SchemaError(f"validate target does not exist: {target}",
                          file=target, field_name="target", reason="not_found")
    found: list[Path] = []
    for pat in ("**/*.yaml", "**/*.yml", "**/*.json"):
        found.extend(p.glob(pat))
    return sorted(set(found))


# ---------------------------------------------------------------------------
# resolve → merge → interpolate (shared by single-doc YAML & multi-doc)
# ---------------------------------------------------------------------------

@dataclass
class _PipelineResult:
    resolved: Any | None
    position_ns: Any
    abstracts: list[AbstractUnfilledError] = field(default_factory=list)
    placeholder_errors: list[PromptCliError] = field(default_factory=list)


def _resolve_pipeline(graph, session) -> _PipelineResult:
    order = layer_order(graph)
    layers_data = [graph.nodes[nid].doc.namespace for nid in order]
    provenance = [(nid.canonical, graph.nodes[nid].file) for nid in order]
    merged = merge_namespaces(layers_data, provenance=provenance)
    layers = [Layer(canonical_id=nid.canonical, data=graph.nodes[nid].doc.namespace)
              for nid in order]
    root_file = graph.nodes[graph.root_id].file
    position_ns = graph.nodes[graph.root_id].doc.namespace

    diagnostics: list[Any] = []
    collect_placeholder_errors(
        merged, merged, layers,
        parent_is_list=False, root_file=root_file, out=diagnostics,
    )
    abstracts = [e for e in diagnostics if isinstance(e, AbstractUnfilledError)]
    others = [e for e in diagnostics if not isinstance(e, AbstractUnfilledError)]

    resources = build_resource_binding(graph, session)

    if abstracts or others:
        return _PipelineResult(
            resolved=None, position_ns=position_ns,
            abstracts=abstracts, placeholder_errors=others,
        )
    resolved = interpolate(merged, layers, root_file=root_file, resources=resources)
    return _PipelineResult(resolved=resolved, position_ns=position_ns)


def _abstracts_payload(
    file_str: str, abstracts: list[AbstractUnfilledError],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for a in abstracts:
        loc = a.location if isinstance(a.location, dict) else {}
        out.append({
            "file": file_str,
            "path": a.details.get("placeholder"),
            "line": loc.get("line"),
            "column": loc.get("column"),
            "reason": a.details.get("reason"),
        })
    return out


# ---------------------------------------------------------------------------
# per-file validation
# ---------------------------------------------------------------------------

def _validate_yaml_file(
    path: Path,
    session_factory,
    schema_opts: SchemaCheckOptions,
) -> tuple[int, list[PromptCliError], list[dict[str, Any]]]:
    file_str = str(path)
    text = path.read_text(encoding="utf-8")

    try:
        docs = load_all_with_positions(text, file=file_str, strict=False)
    except PromptCliError as e:
        return 0, [e], []

    if not docs:
        return 0, [], []

    if len(docs) == 1:
        data, _ = docs[0]
        if not isinstance(data, dict):
            return 1, [], []
        raw_uri = data.get("$schema")
        has_schema = isinstance(raw_uri, str) and bool(raw_uri)
        try:
            session = session_factory()
            graph = resolve_graph(file_str, session)
            pipe = _resolve_pipeline(graph, session)
        except PromptCliError as e:
            return 1, ([e] if has_schema else []), []
        errors: list[PromptCliError] = list(pipe.placeholder_errors) if has_schema else []
        abstracts_out = _abstracts_payload(file_str, pipe.abstracts)
        if not has_schema or pipe.resolved is None:
            return 1, errors, abstracts_out
        schema_uri = resolve_schema_uri(raw_uri, file_str)
        errors.extend(validate_against_schema(
            pipe.resolved, schema_uri, file=file_str, opts=schema_opts,
            position_instance=pipe.position_ns,
        ))
        return 1, errors, abstracts_out

    all_errors: list[PromptCliError] = []
    all_abstracts: list[dict[str, Any]] = []
    for doc_idx, (data, start_line) in enumerate(docs, 1):
        if not isinstance(data, dict):
            all_errors.append(SchemaError(
                f"{file_str} document {doc_idx} (line {start_line}) must be a YAML mapping",
                file=file_str, line=start_line, field_name="<root>",
                reason="not_mapping"))
            continue
        raw_uri = data.get("$schema")
        has_schema = isinstance(raw_uri, str) and bool(raw_uri)

        try:
            doc = parse_prompt(_dump_for_reparse(data), file=file_str,
                               strict=False, validate_paths=False)
        except PromptCliError as e:
            if has_schema:
                _tag_document(e, doc_idx)
                all_errors.append(e)
            continue

        position_ns = {k: v for k, v in data.items() if k not in RESERVED_KEYS}
        try:
            session = session_factory()
            graph = resolve_from_document(doc, file_str, session)
            pipe = _resolve_pipeline(graph, session)
        except PromptCliError as e:
            if has_schema:
                _tag_document(e, doc_idx)
                all_errors.append(e)
            continue

        if has_schema:
            for e in pipe.placeholder_errors:
                _tag_document(e, doc_idx)
                all_errors.append(e)
        for a in _abstracts_payload(file_str, pipe.abstracts):
            a["document"] = doc_idx
            all_abstracts.append(a)
        if not has_schema or pipe.resolved is None:
            continue

        schema_uri = resolve_schema_uri(raw_uri, file_str)
        errs = validate_against_schema(
            pipe.resolved, schema_uri, file=file_str, opts=schema_opts,
            position_instance=position_ns,
        )
        for e in errs:
            _tag_document(e, doc_idx)
        all_errors.extend(errs)

    return len(docs), all_errors, all_abstracts


def _validate_json_file(
    path: Path,
    session_factory,
    schema_opts: SchemaCheckOptions,
) -> tuple[int, list[PromptCliError], list[dict[str, Any]]]:
    file_str = str(path)
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as e:
        return 0, [SchemaError(f"cannot read {file_str}: {e}",
                               file=file_str, field_name="<io>", reason="io_error")], []

    try:
        doc = parse_prompt(text, file=file_str, strict=False, validate_paths=False)
    except PromptCliError as e:
        return 1, [e], []

    raw_uri = doc.data.get("$schema")
    has_schema = isinstance(raw_uri, str) and bool(raw_uri)

    try:
        session = session_factory()
        graph = resolve_graph(file_str, session)
        pipe = _resolve_pipeline(graph, session)
    except PromptCliError as e:
        return 1, ([e] if has_schema else []), []

    errors: list[PromptCliError] = list(pipe.placeholder_errors) if has_schema else []
    abstracts_out = _abstracts_payload(file_str, pipe.abstracts)
    if not has_schema or pipe.resolved is None:
        return 1, errors, abstracts_out
    schema_uri = resolve_schema_uri(raw_uri, file_str)
    errors.extend(validate_against_schema(
        pipe.resolved, schema_uri, file=file_str, opts=schema_opts,
        position_instance=pipe.position_ns,
    ))
    return 1, errors, abstracts_out


# ---------------------------------------------------------------------------
# orchestrator
# ---------------------------------------------------------------------------

def run_validate(
    target: str,
    session_factory,    # () -> Session  (fresh session per prompt)
    schema_opts: SchemaCheckOptions,
) -> dict[str, int]:
    """Validate *target* and raise ``AggregatedError`` on violations.

    Returns ``{"files_checked": …, "documents_checked": …, "violations_found": 0}``
    on success.
    """
    files = discover_files(target)
    if not files:
        return {"files_checked": 0, "documents_checked": 0, "violations_found": 0}

    total_files = 0
    total_docs = 0
    all_errors: list[PromptCliError] = []
    all_abstracts: list[dict[str, Any]] = []
    for fpath in files:
        ext = fpath.suffix.lower()
        if ext in (".yaml", ".yml"):
            docs, errs, abstracts = _validate_yaml_file(fpath, session_factory, schema_opts)
        elif ext == ".json":
            docs, errs, abstracts = _validate_json_file(fpath, session_factory, schema_opts)
        else:
            continue
        total_files += 1
        total_docs += docs
        all_errors.extend(errs)
        all_abstracts.extend(abstracts)

    if all_errors:
        raise AggregatedError(all_errors, command="validate")
    return {"files_checked": total_files, "documents_checked": total_docs,
            "violations_found": 0,
            "abstracts_found": len(all_abstracts),
            "abstracts": all_abstracts}


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _dump_for_reparse(data: dict[str, Any]) -> str:
    class _D(yaml.SafeDumper):
        pass
    _D.add_representer(_ScalarStr,
        lambda d, v: d.represent_scalar("tag:yaml.org,2002:str", str(v)))
    return yaml.dump(data, Dumper=_D, default_flow_style=False, sort_keys=False)


def _tag_document(err: PromptCliError, doc_idx: int) -> None:
    if isinstance(err.location, dict):
        err.location["document"] = doc_idx
    elif err.location is None:
        err.location = {"document": doc_idx}
