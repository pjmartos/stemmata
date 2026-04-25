from __future__ import annotations

from collections import deque
from typing import Any

from stemmata.errors import PromptCliError, SchemaError
from stemmata.interp import scan_abstract_references
from stemmata.prompt_doc import AbstractAnnotation, PromptDocument


def body_abstract_paths(doc: PromptDocument) -> set[str]:
    return {r.path for r in scan_abstract_references(doc.namespace, file_fallback=doc.file)}


def _ref_for_path(doc: PromptDocument, path: str):
    for r in scan_abstract_references(doc.namespace, file_fallback=doc.file):
        if r.path == path:
            return r
    return None


def _ancestor_closure(graph) -> dict[Any, list[Any]]:
    out: dict[Any, list[Any]] = {}
    for start in graph.nodes:
        seen: set[Any] = set()
        order: list[Any] = []
        queue: deque = deque(graph.nodes[start].children)
        while queue:
            nid = queue.popleft()
            if nid in seen or nid not in graph.nodes:
                continue
            seen.add(nid)
            order.append(nid)
            queue.extend(graph.nodes[nid].children)
        out[start] = order
    return out


def validate_abstract_coupling(graph) -> list[PromptCliError]:
    errors: list[PromptCliError] = []
    body_paths: dict[Any, set[str]] = {}
    annotated_paths: dict[Any, set[str]] = {}
    for nid, node in graph.nodes.items():
        body_paths[nid] = body_abstract_paths(node.doc)
        annotated_paths[nid] = set(node.doc.abstracts.keys())

    closure = _ancestor_closure(graph)

    for nid, node in graph.nodes.items():
        own_body = body_paths[nid]
        own_ann = annotated_paths[nid]
        ancestor_body: set[str] = set()
        ancestor_ann: set[str] = set()
        for anc_nid in closure[nid]:
            ancestor_body |= body_paths[anc_nid]
            ancestor_ann |= annotated_paths[anc_nid]

        for path in sorted(own_ann):
            if path in ancestor_body or path in ancestor_ann:
                ann = node.doc.abstracts[path]
                errors.append(SchemaError(
                    f"'abstracts.{path}' re-annotates an abstract already "
                    f"introduced by an ancestor; annotations belong to the "
                    f"originating declarer",
                    file=node.file,
                    line=ann.line,
                    column=ann.column,
                    field_name=f"abstracts.{path}",
                    reason="abstract_reannotation",
                ))

        for path in sorted(own_body):
            if path in ancestor_body:
                continue
            if path in own_ann:
                continue
            ref = _ref_for_path(node.doc, path)
            line = ref.line if ref is not None else None
            column = ref.column if ref is not None else None
            errors.append(SchemaError(
                f"${{abstract:{path}}} is introduced by this prompt but has "
                f"no matching entry in the top-level 'abstracts' mapping; "
                f"new abstract declarations must be documented",
                file=node.file,
                line=line,
                column=column,
                field_name=f"abstracts.{path}",
                reason="undocumented_abstract",
            ))

    return errors


def annotation_lookup(layers_docs) -> dict[str, AbstractAnnotation]:
    out: dict[str, AbstractAnnotation] = {}
    for doc in layers_docs:
        for path, ann in doc.abstracts.items():
            out[path] = ann
    return out


_SCHEMA_TYPE_TO_ANNOTATION = {
    "string": "string",
    "array": "list",
}


def _schema_constraint_at_path(schema: Any, dotted_path: str) -> str | None:
    if not isinstance(schema, dict):
        return None
    cur: Any = schema
    for segment in dotted_path.split("."):
        if not isinstance(cur, dict):
            return None
        props = cur.get("properties")
        if not isinstance(props, dict) or segment not in props:
            return None
        cur = props[segment]
    if not isinstance(cur, dict):
        return None
    type_field = cur.get("type")
    if isinstance(type_field, str):
        return type_field
    if isinstance(type_field, list):
        if not type_field or any(
            not isinstance(t, str) or t not in _SCHEMA_TYPE_TO_ANNOTATION
            for t in type_field
        ):
            return None
        annotation_types = {_SCHEMA_TYPE_TO_ANNOTATION[t] for t in type_field}
        if len(annotation_types) == 1:
            return next(iter(annotation_types))
    return None


def validate_schema_type_consistency(
    doc: PromptDocument,
    schema: Any,
) -> list[PromptCliError]:
    errors: list[PromptCliError] = []
    for path, ann in doc.abstracts.items():
        schema_type = _schema_constraint_at_path(schema, path)
        if schema_type is None:
            continue
        if schema_type not in _SCHEMA_TYPE_TO_ANNOTATION:
            continue
        expected = _SCHEMA_TYPE_TO_ANNOTATION[schema_type]
        if expected == ann.type:
            continue
        errors.append(SchemaError(
            f"'abstracts.{path}.type' is {ann.type!r} but $schema constrains "
            f"the same path to {schema_type!r}; the annotation contradicts "
            f"the schema",
            file=doc.file,
            line=ann.line,
            column=ann.column,
            field_name=f"abstracts.{path}.type",
            reason="schema_type_mismatch",
        ))
    return errors
