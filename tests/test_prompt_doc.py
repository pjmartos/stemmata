import pytest

from stemmata.errors import SchemaError
from stemmata.prompt_doc import CoordRef, PathRef, parse_prompt


def test_empty_prompt():
    doc = parse_prompt("", file="x.yaml")
    assert doc.ancestors == []
    assert doc.namespace == {}


def test_plain_content_no_reserved():
    doc = parse_prompt("foo: bar\n", file="x.yaml")
    assert doc.namespace == {"foo": "bar"}
    assert doc.ancestors == []


def test_reserved_keys_stripped():
    text = """
ancestors:
  - "./base.yaml"
$schema: "https://example/s.json"
foo: bar
"""
    doc = parse_prompt(text, file="x.yaml")
    assert "ancestors" not in doc.namespace
    assert "$schema" not in doc.namespace
    assert doc.namespace == {"foo": "bar"}
    assert doc.schema_uri == "https://example/s.json"


def test_relative_ancestor_ref():
    doc = parse_prompt("ancestors:\n  - ../base.yaml\n", file="x.yaml", validate_paths=False)
    assert isinstance(doc.ancestors[0], PathRef)
    assert doc.ancestors[0].raw == "../base.yaml"


def test_coord_ancestor_ref():
    text = """
ancestors:
  - package: "@acme/core"
    version: "1.2.3"
    prompt: base
"""
    doc = parse_prompt(text, file="x.yaml")
    ref = doc.ancestors[0]
    assert isinstance(ref, CoordRef)
    assert ref.package == "@acme/core"
    assert ref.version == "1.2.3"
    assert ref.prompt == "base"


def test_ancestors_must_be_sequence():
    with pytest.raises(SchemaError):
        parse_prompt("ancestors:\n  foo: bar\n", file="x.yaml")


def test_coord_missing_field():
    with pytest.raises(SchemaError):
        parse_prompt("ancestors:\n  - package: '@a/b'\n    version: '1.0.0'\n", file="x.yaml")


def test_coord_invalid_package():
    with pytest.raises(SchemaError):
        parse_prompt(
            "ancestors:\n  - package: 'noscope'\n    version: '1.0.0'\n    prompt: x\n",
            file="x.yaml",
        )


def test_absolute_path_rejected():
    with pytest.raises(SchemaError):
        parse_prompt("ancestors:\n  - /etc/passwd\n", file="x.yaml", validate_paths=True)


def test_root_must_be_mapping():
    with pytest.raises(SchemaError):
        parse_prompt("- just\n- a\n- list\n", file="x.yaml")


def test_mixed_ancestors_list():
    text = """
ancestors:
  - "./base.yaml"
  - package: "@a/b"
    version: "1.0.0"
    prompt: x
"""
    doc = parse_prompt(text, file="x.yaml", validate_paths=False)
    assert isinstance(doc.ancestors[0], PathRef)
    assert isinstance(doc.ancestors[1], CoordRef)


def test_dotted_key_expanded():
    doc = parse_prompt("vars.region: Cartama\n", file="x.yaml")
    assert doc.namespace == {"vars": {"region": "Cartama"}}


def test_dotted_key_deep():
    doc = parse_prompt("a.b.c: 42\n", file="x.yaml")
    assert doc.namespace == {"a": {"b": {"c": 42}}}


def test_dotted_key_merged_with_nested():
    text = "vars:\n  name: foo\nvars.region: bar\n"
    doc = parse_prompt(text, file="x.yaml")
    assert doc.namespace == {"vars": {"name": "foo", "region": "bar"}}


def test_multiple_dotted_keys_same_prefix():
    text = "db.host: localhost\ndb.port: 5432\n"
    doc = parse_prompt(text, file="x.yaml")
    assert doc.namespace == {"db": {"host": "localhost", "port": 5432}}


def test_intra_doc_scalar_then_dotted_is_conflict():
    text = "a: 1\na.b: 2\n"
    with pytest.raises(SchemaError) as exc:
        parse_prompt(text, file="x.yaml")
    assert "intra-document type conflict" in str(exc.value)
    assert exc.value.details["reason"] == "intra_doc_type_conflict"


def test_intra_doc_dotted_then_scalar_is_conflict():
    text = "a.b: 1\na: 2\n"
    with pytest.raises(SchemaError) as exc:
        parse_prompt(text, file="x.yaml")
    assert "intra-document type conflict" in str(exc.value)


def test_intra_doc_list_vs_map_is_conflict():
    text = "a:\n  - 1\n  - 2\na.b: 3\n"
    with pytest.raises(SchemaError):
        parse_prompt(text, file="x.yaml")


def test_intra_doc_same_scalar_type_last_wins():
    text = 'a:\n  b:\n    c: 1\na.b.c: 2\n'
    doc = parse_prompt(text, file="x.yaml")
    assert doc.namespace == {"a": {"b": {"c": 2}}}


def test_intra_doc_both_maps_deep_merge():
    text = "a:\n  b: 1\na.c: 2\n"
    doc = parse_prompt(text, file="x.yaml")
    assert doc.namespace == {"a": {"b": 1, "c": 2}}


def test_intra_doc_conflict_nested_scope_reports_full_path():
    text = "outer:\n  a: 1\n  a.b: 2\n"
    with pytest.raises(SchemaError) as exc:
        parse_prompt(text, file="x.yaml")
    assert "outer.a" in str(exc.value)


def test_plain_key_unchanged():
    doc = parse_prompt("simple: value\n", file="x.yaml")
    assert doc.namespace == {"simple": "value"}


# -- JSON prompt parsing -------------------------------------------------------


def test_json_empty_prompt():
    doc = parse_prompt("{}", file="x.json")
    assert doc.ancestors == []
    assert doc.namespace == {}


def test_json_plain_content():
    import json
    doc = parse_prompt(json.dumps({"foo": "bar"}), file="x.json")
    assert doc.namespace == {"foo": "bar"}
    assert doc.ancestors == []


def test_json_reserved_keys_stripped():
    import json
    text = json.dumps({
        "ancestors": ["./base.json"],
        "$schema": "https://example/s.json",
        "foo": "bar",
    })
    doc = parse_prompt(text, file="x.json", validate_paths=False)
    assert "ancestors" not in doc.namespace
    assert "$schema" not in doc.namespace
    assert doc.namespace == {"foo": "bar"}
    assert doc.schema_uri == "https://example/s.json"


def test_json_relative_ancestor_ref():
    import json
    text = json.dumps({"ancestors": ["../base.json"]})
    doc = parse_prompt(text, file="x.json", validate_paths=False)
    assert isinstance(doc.ancestors[0], PathRef)
    assert doc.ancestors[0].raw == "../base.json"


def test_json_coord_ancestor_ref():
    import json
    text = json.dumps({
        "ancestors": [{"package": "@acme/core", "version": "1.2.3", "prompt": "base"}],
    })
    doc = parse_prompt(text, file="x.json")
    ref = doc.ancestors[0]
    assert isinstance(ref, CoordRef)
    assert ref.package == "@acme/core"
    assert ref.version == "1.2.3"
    assert ref.prompt == "base"


def test_json_root_must_be_mapping():
    with pytest.raises(SchemaError):
        parse_prompt("[1, 2, 3]", file="x.json")


def test_json_dotted_key_expanded():
    import json
    doc = parse_prompt(json.dumps({"vars.region": "Cartama"}), file="x.json")
    assert doc.namespace == {"vars": {"region": "Cartama"}}


def test_json_empty_input():
    doc = parse_prompt("", file="x.json")
    assert doc.ancestors == []
    assert doc.namespace == {}


def test_json_malformed():
    with pytest.raises(SchemaError):
        parse_prompt("{bad", file="x.json")

def test_resource_empty_body_in_yaml_flow_scalar_rejected():
    with pytest.raises(SchemaError) as exc:
        parse_prompt('body: "${resource:}"\n', file="x.yaml")
    assert exc.value.details["reason"] == "resource_empty_body"


def test_resource_whitespace_only_body_in_yaml_flow_scalar_rejected():
    with pytest.raises(SchemaError) as exc:
        parse_prompt('body: "${resource:   }"\n', file="x.yaml")
    assert exc.value.details["reason"] == "resource_empty_body"


def test_resource_empty_body_in_yaml_block_scalar_rejected():
    text = "body: |\n  ${resource:}\n"
    with pytest.raises(SchemaError) as exc:
        parse_prompt(text, file="x.yaml")
    assert exc.value.details["reason"] == "resource_empty_body"


def test_resource_empty_body_in_json_string_rejected():
    import json
    with pytest.raises(SchemaError) as exc:
        parse_prompt(json.dumps({"body": "${resource:}"}), file="x.json")
    assert exc.value.details["reason"] == "resource_empty_body"


def test_resource_non_empty_body_accepted():
    doc = parse_prompt('body: "${resource:a.md}"\n', file="x.yaml")
    assert doc.namespace == {"body": "${resource:a.md}"}


def test_resource_escaped_empty_body_not_rejected():
    doc = parse_prompt('body: "$${resource:}"\n', file="x.yaml")
    assert doc.namespace == {"body": "$${resource:}"}


# -- abstract annotations -----------------------------------------


def test_abstracts_block_basic_string_default():
    text = (
        "abstracts:\n"
        "  greeting:\n"
        "    description: opening line of the message\n"
        "greeting: ${abstract:greeting}\n"
    )
    doc = parse_prompt(text, file="x.yaml")
    assert "abstracts" not in doc.namespace
    assert "greeting" in doc.abstracts
    ann = doc.abstracts["greeting"]
    assert ann.description == "opening line of the message"
    assert ann.type == "string"
    assert ann.has_example is False


def test_abstracts_block_with_explicit_type_and_example():
    text = (
        "abstracts:\n"
        "  steps:\n"
        "    description: ordered list of subroutine names\n"
        "    type: list\n"
        "    example:\n"
        "      - load\n"
        "      - run\n"
        "  body:\n"
        "    description: free-form prose\n"
        "    type: string\n"
        "    example: hi there\n"
        "steps: ${abstract:steps}\n"
        "msg: ${abstract:body}\n"
    )
    doc = parse_prompt(text, file="x.yaml")
    assert doc.abstracts["steps"].type == "list"
    assert doc.abstracts["steps"].has_example is True
    assert doc.abstracts["steps"].example == ["load", "run"]
    assert doc.abstracts["body"].example == "hi there"


def test_abstracts_block_must_be_mapping():
    with pytest.raises(SchemaError) as exc:
        parse_prompt("abstracts:\n  - foo\n", file="x.yaml")
    assert exc.value.details["reason"] == "invalid_abstracts"


def test_abstracts_entry_must_be_mapping():
    text = "abstracts:\n  greeting: just a string\n"
    with pytest.raises(SchemaError) as exc:
        parse_prompt(text, file="x.yaml")
    assert exc.value.details["reason"] == "invalid_abstract_annotation"


def test_abstracts_entry_requires_description():
    text = "abstracts:\n  greeting:\n    type: string\n"
    with pytest.raises(SchemaError) as exc:
        parse_prompt(text, file="x.yaml")
    assert exc.value.details["reason"] == "missing_description"


def test_abstracts_entry_rejects_empty_description():
    text = "abstracts:\n  greeting:\n    description: ''\n"
    with pytest.raises(SchemaError) as exc:
        parse_prompt(text, file="x.yaml")
    assert exc.value.details["reason"] == "empty_description"


def test_abstracts_entry_rejects_unknown_field():
    text = (
        "abstracts:\n"
        "  greeting:\n"
        "    description: foo\n"
        "    bogus: 1\n"
    )
    with pytest.raises(SchemaError) as exc:
        parse_prompt(text, file="x.yaml")
    assert exc.value.details["reason"] == "unknown_annotation_field"


def test_abstracts_entry_rejects_invalid_type():
    text = (
        "abstracts:\n"
        "  greeting:\n"
        "    description: foo\n"
        "    type: object\n"
    )
    with pytest.raises(SchemaError) as exc:
        parse_prompt(text, file="x.yaml")
    assert exc.value.details["reason"] == "invalid_annotation_type"


def test_abstracts_annotation_without_body_marker_rejected():
    text = (
        "abstracts:\n"
        "  greeting:\n"
        "    description: opening line\n"
        "body: hello\n"
    )
    with pytest.raises(SchemaError) as exc:
        parse_prompt(text, file="x.yaml")
    assert exc.value.details["reason"] == "annotation_without_declaration"


def test_abstracts_type_list_in_textual_position_rejected():
    text = (
        "abstracts:\n"
        "  steps:\n"
        "    description: list of steps\n"
        "    type: list\n"
        "body: 'pre ${abstract:steps} post'\n"
    )
    with pytest.raises(SchemaError) as exc:
        parse_prompt(text, file="x.yaml")
    assert exc.value.details["reason"] == "list_abstract_in_textual_position"


def test_abstracts_block_stripped_from_namespace():
    text = (
        "abstracts:\n"
        "  greeting:\n"
        "    description: opening\n"
        "greeting: ${abstract:greeting}\n"
        "other: 1\n"
    )
    doc = parse_prompt(text, file="x.yaml")
    assert "abstracts" not in doc.namespace
    assert doc.namespace == {"greeting": "${abstract:greeting}", "other": 1}
