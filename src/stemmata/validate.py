"""``stemmata validate`` — schema validation for prompt files."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from stemmata.errors import AggregatedError, PromptCliError, SchemaError
from stemmata.interp import Layer, interpolate
from stemmata.merge import merge_namespaces
from stemmata.prompt_doc import RESERVED_KEYS, parse_prompt
from stemmata.resolver import (
    Session,
    layer_order,
    resolve_from_document,
    resolve_graph,
)
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

def _resolve_pipeline(
    graph, # ResolvedGraph
) -> tuple[Any, Any]:
    """Run merge + interpolate on a resolved graph.

    Returns ``(resolved_namespace, position_namespace)``.
    """
    order = layer_order(graph)
    layers_data = [graph.nodes[nid].doc.namespace for nid in order]
    provenance = [(nid.canonical, graph.nodes[nid].file) for nid in order]
    merged = merge_namespaces(layers_data, provenance=provenance)
    layers = [Layer(canonical_id=nid.canonical, data=graph.nodes[nid].doc.namespace)
              for nid in order]
    root_file = graph.nodes[graph.root_id].file
    resolved = interpolate(merged, layers, root_file=root_file)
    position_ns = graph.nodes[graph.root_id].doc.namespace
    return resolved, position_ns


# ---------------------------------------------------------------------------
# per-file validation
# ---------------------------------------------------------------------------

def _validate_yaml_file(
    path: Path,
    session_factory,   # () -> Session
    schema_opts: SchemaCheckOptions,
) -> tuple[int, list[PromptCliError]]:
    """Returns ``(documents_checked, errors)``."""
    file_str = str(path)
    text = path.read_text(encoding="utf-8")

    try:
        docs = load_all_with_positions(text, file=file_str, strict=False)
    except PromptCliError as e:
        return 0, [e]

    if not docs:
        return 0, []

    # ---- single-document: use resolve_graph (reads file from disk) ----
    if len(docs) == 1:
        data, _ = docs[0]
        if not isinstance(data, dict):
            return 1, []
        raw_uri = data.get("$schema")
        if not raw_uri or not isinstance(raw_uri, str):
            return 1, []
        schema_uri = resolve_schema_uri(raw_uri, file_str)
        errors: list[PromptCliError] = []
        try:
            graph = resolve_graph(file_str, session_factory())
            resolved, position_ns = _resolve_pipeline(graph)
        except PromptCliError as e:
            return 1, [e]
        errors.extend(validate_against_schema(
            resolved, schema_uri, file=file_str, opts=schema_opts,
            position_instance=position_ns,
        ))
        return 1, errors

    # ---- multi-document ----
    all_errors: list[PromptCliError] = []
    for doc_idx, (data, start_line) in enumerate(docs, 1):
        if not isinstance(data, dict):
            all_errors.append(SchemaError(
                f"{file_str} document {doc_idx} (line {start_line}) must be a YAML mapping",
                file=file_str, line=start_line, field_name="<root>",
                reason="not_mapping"))
            continue
        raw_uri = data.get("$schema")
        if not raw_uri or not isinstance(raw_uri, str):
            continue
        schema_uri = resolve_schema_uri(raw_uri, file_str)

        # Parse the sub-document into a PromptDocument.
        try:
            doc = parse_prompt(_dump_for_reparse(data), file=file_str,
                               strict=False, validate_paths=False)
        except PromptCliError as e:
            all_errors.append(e)
            continue

        position_ns = {k: v for k, v in data.items() if k not in RESERVED_KEYS}
        if doc.ancestors:
            try:
                graph = resolve_from_document(doc, file_str, session_factory())
                resolved, _ = _resolve_pipeline(graph)
                position_ns = graph.nodes[graph.root_id].doc.namespace
            except PromptCliError as e:
                _tag_document(e, doc_idx)
                all_errors.append(e)
                continue
        else:
            resolved = doc.namespace

        errs = validate_against_schema(
            resolved, schema_uri, file=file_str, opts=schema_opts,
            position_instance=position_ns,
        )
        for e in errs:
            _tag_document(e, doc_idx)
        all_errors.extend(errs)

    return len(docs), all_errors


def _validate_json_file(
    path: Path,
    session_factory,    # () -> Session
    schema_opts: SchemaCheckOptions,
) -> tuple[int, list[PromptCliError]]:
    """Returns ``(documents_checked, errors)``."""
    file_str = str(path)
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as e:
        return 0, [SchemaError(f"cannot read {file_str}: {e}",
                               file=file_str, field_name="<io>", reason="io_error")]

    try:
        doc = parse_prompt(text, file=file_str, strict=False, validate_paths=False)
    except PromptCliError as e:
        return 1, [e]

    raw_uri = doc.data.get("$schema")
    if not raw_uri or not isinstance(raw_uri, str):
        return 1, []
    schema_uri = resolve_schema_uri(raw_uri, file_str)

    errors: list[PromptCliError] = []
    if doc.ancestors:
        try:
            graph = resolve_graph(file_str, session_factory())
            resolved, position_ns = _resolve_pipeline(graph)
        except PromptCliError as e:
            return 1, [e]
    else:
        resolved = doc.namespace
        position_ns = doc.namespace

    errors.extend(validate_against_schema(
        resolved, schema_uri, file=file_str, opts=schema_opts,
        position_instance=position_ns,
    ))
    return 1, errors


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
    for fpath in files:
        ext = fpath.suffix.lower()
        if ext in (".yaml", ".yml"):
            docs, errs = _validate_yaml_file(fpath, session_factory, schema_opts)
        elif ext == ".json":
            docs, errs = _validate_json_file(fpath, session_factory, schema_opts)
        else:
            continue
        total_files += 1
        total_docs += docs
        all_errors.extend(errs)

    if all_errors:
        raise AggregatedError(all_errors, command="validate")
    return {"files_checked": total_files, "documents_checked": total_docs,
            "violations_found": 0}


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
