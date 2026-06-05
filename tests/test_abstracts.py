from __future__ import annotations

from pathlib import Path
from textwrap import dedent

from stemmata.abstracts import (
    _schema_constrained_types_at_path,
    annotation_lookup,
    body_abstract_paths,
    validate_abstract_coupling,
    validate_schema_type_consistency,
)
from stemmata.cache import Cache
from stemmata.errors import SchemaError
from stemmata.npmrc import NpmConfig
from stemmata.prompt_doc import parse_prompt
from stemmata.registry import RegistryClient
from stemmata.resolver import Session, resolve_graph


def _session(tmp_path: Path) -> Session:
    return Session(
        cache=Cache(root=tmp_path / "cache"),
        registry=RegistryClient(config=NpmConfig(entries={}), offline=True, http_timeout=5.0),
    )


def _write(p: Path, body: str) -> Path:
    p.write_text(dedent(body), encoding="utf-8")
    return p


def test_body_abstract_paths_collects_all_markers():
    text = dedent("""
        abstracts:
          name:
            description: who
          step:
            description: what
        body: "Hi ${abstract:name}!"
        steps:
          - ${abstract:step}
    """).lstrip()
    doc = parse_prompt(text, file="x.yaml")
    assert body_abstract_paths(doc) == {"name", "step"}


def test_annotation_lookup_unions_layers():
    a = parse_prompt(
        "abstracts:\n  x:\n    description: x\n"
        "x: ${abstract:x}\n",
        file="a.yaml",
    )
    b = parse_prompt(
        "abstracts:\n  y:\n    description: y\n    type: list\n"
        "y: ${abstract:y}\n",
        file="b.yaml",
    )
    table = annotation_lookup([a, b])
    assert set(table) == {"x", "y"}
    assert table["x"].type == "string"
    assert table["y"].type == "list"


def test_schema_constraint_walks_nested_properties():
    schema = {
        "type": "object",
        "properties": {
            "outer": {
                "type": "object",
                "properties": {
                    "inner": {"type": "array"},
                },
            },
        },
    }
    assert _schema_constrained_types_at_path(schema, "outer.inner") == {"array"}


def test_schema_constraint_returns_none_when_unknown():
    schema = {"type": "object", "properties": {"a": {"type": "string"}}}
    assert _schema_constrained_types_at_path(schema, "b") is None
    assert _schema_constrained_types_at_path(schema, "a.deeper") is None


def test_schema_constraint_returns_full_type_union():
    schema = {"type": "object", "properties": {"x": {"type": ["array", "array"]}}}
    assert _schema_constrained_types_at_path(schema, "x") == {"array"}
    schema = {"type": "object", "properties": {"x": {"type": ["string", "integer"]}}}
    assert _schema_constrained_types_at_path(schema, "x") == {"string", "integer"}


def test_schema_type_consistency_ok_when_aligned():
    doc = parse_prompt(
        "abstracts:\n  x:\n    description: x\n    type: list\n"
        "x: ${abstract:x}\n",
        file="x.yaml",
    )
    schema = {"type": "object", "properties": {"x": {"type": "array"}}}
    assert validate_schema_type_consistency(doc, schema) == []


def test_schema_type_consistency_flags_contradiction():
    doc = parse_prompt(
        "abstracts:\n  x:\n    description: x\n    type: list\n"
        "x: ${abstract:x}\n",
        file="x.yaml",
    )
    schema = {"type": "object", "properties": {"x": {"type": "string"}}}
    errors = validate_schema_type_consistency(doc, schema)
    assert len(errors) == 1
    assert errors[0].details["reason"] == "schema_type_mismatch"


def test_schema_type_consistency_flags_string_vs_integer():
    doc = parse_prompt(
        "abstracts:\n  count:\n    description: count\n    type: string\n"
        "count: ${abstract:count}\n",
        file="x.yaml",
    )
    schema = {"type": "object", "properties": {"count": {"type": "integer"}}}
    errors = validate_schema_type_consistency(doc, schema)
    assert len(errors) == 1
    assert errors[0].details["reason"] == "schema_type_mismatch"


def test_schema_type_consistency_flags_list_vs_object():
    doc = parse_prompt(
        "abstracts:\n  payload:\n    description: payload\n    type: list\n"
        "payload: ${abstract:payload}\n",
        file="x.yaml",
    )
    schema = {"type": "object", "properties": {"payload": {"type": "object"}}}
    errors = validate_schema_type_consistency(doc, schema)
    assert len(errors) == 1
    assert errors[0].details["reason"] == "schema_type_mismatch"


def test_schema_type_consistency_silent_when_union_overlaps_annotation():
    doc = parse_prompt(
        "abstracts:\n  who:\n    description: who\n    type: string\n"
        "who: ${abstract:who}\n",
        file="x.yaml",
    )
    schema = {"type": "object", "properties": {"who": {"type": ["string", "null"]}}}
    assert validate_schema_type_consistency(doc, schema) == []


def test_schema_type_consistency_flags_when_union_excludes_annotation():
    doc = parse_prompt(
        "abstracts:\n  count:\n    description: count\n    type: string\n"
        "count: ${abstract:count}\n",
        file="x.yaml",
    )
    schema = {"type": "object", "properties": {"count": {"type": ["integer", "null"]}}}
    errors = validate_schema_type_consistency(doc, schema)
    assert len(errors) == 1
    assert errors[0].details["reason"] == "schema_type_mismatch"


def test_schema_type_consistency_silent_when_path_unconstrained():
    doc = parse_prompt(
        "abstracts:\n  x:\n    description: x\n    type: list\n"
        "x: ${abstract:x}\n",
        file="x.yaml",
    )
    schema = {"type": "object", "properties": {"y": {"type": "string"}}}
    assert validate_schema_type_consistency(doc, schema) == []


def test_coupling_passes_when_descendant_inherits_unfilled(tmp_path):
    base = _write(tmp_path / "base.yaml",
        """
        abstracts:
          who:
            description: addressee
        msg: "Hi ${abstract:who}"
        """,
    )
    child = _write(tmp_path / "child.yaml",
        """
        ancestors:
          - "./base.yaml"
        body: x
        """,
    )
    graph = resolve_graph(str(child), _session(tmp_path))
    assert validate_abstract_coupling(graph) == []


def test_coupling_flags_re_annotation_in_descendant(tmp_path):
    base = _write(tmp_path / "base.yaml",
        """
        abstracts:
          who:
            description: addressee
        msg: "Hi ${abstract:who}"
        """,
    )
    child = _write(tmp_path / "child.yaml",
        """
        ancestors:
          - "./base.yaml"
        abstracts:
          who:
            description: re-annotated
        body: ${abstract:who}
        """,
    )
    graph = resolve_graph(str(child), _session(tmp_path))
    errors = validate_abstract_coupling(graph)
    assert len(errors) == 1
    assert errors[0].details["reason"] == "abstract_reannotation"


def test_coupling_flags_undocumented_introduction(tmp_path):
    f = _write(tmp_path / "x.yaml", "body: ${abstract:foo}\n")
    graph = resolve_graph(str(f), _session(tmp_path))
    errors = validate_abstract_coupling(graph)
    assert len(errors) == 1
    assert errors[0].details["reason"] == "undocumented_abstract"


def test_coupling_still_fires_on_abstract_true_prompt(tmp_path):
    # `abstract: true` does NOT relax coupling rules: an annotation without a
    # matching body marker is still a SchemaError.
    f = _write(tmp_path / "x.yaml",
        """
        abstract: true
        abstracts:
          unused:
            description: nobody references this
        body: hello world
        """,
    )
    import pytest
    from stemmata.errors import SchemaError
    with pytest.raises(SchemaError) as exc:
        resolve_graph(str(f), _session(tmp_path))
    assert exc.value.details["reason"] == "annotation_without_declaration"


def test_coupling_silent_on_descendant_marker_inherited_from_ancestor(tmp_path):
    base = _write(tmp_path / "base.yaml",
        """
        abstracts:
          who:
            description: addressee
        a: ${abstract:who}
        """,
    )
    child = _write(tmp_path / "child.yaml",
        """
        ancestors:
          - "./base.yaml"
        b: ${abstract:who}
        """,
    )
    graph = resolve_graph(str(child), _session(tmp_path))
    assert validate_abstract_coupling(graph) == []
