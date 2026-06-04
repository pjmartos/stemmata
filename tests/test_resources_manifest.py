import json

import pytest

from stemmata.errors import SchemaError
from stemmata.manifest import parse_manifest


def _manifest(**kwargs):
    base = {
        "name": "@acme/p",
        "version": "1.0.0",
        "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
    }
    base.update(kwargs)
    return json.dumps(base)


def test_resources_optional_absent():
    m = parse_manifest(_manifest())
    assert m.resources == []


def test_resources_valid_entry():
    m = parse_manifest(_manifest(resources=[
        {"id": "overview", "path": "resources/overview.md", "contentType": "markdown"},
    ]))
    assert len(m.resources) == 1
    assert m.resources[0].id == "overview"
    assert m.resources[0].path == "resources/overview.md"
    assert m.resources[0].contentType == "markdown"


def test_resource_default_id_derived_from_path():
    m = parse_manifest(_manifest(resources=[
        {"path": "resources/overview.md", "contentType": "markdown"},
    ]))
    assert m.resources[0].id == "overview"


@pytest.mark.parametrize("content_type", ["markdown", "text", "xml", "json", "yaml"])
def test_resource_content_types_accepted(content_type):
    m = parse_manifest(_manifest(resources=[
        {"id": "x", "path": f"resources/x.{content_type}", "contentType": content_type},
    ]))
    assert m.resources[0].contentType == content_type


def test_resource_unknown_content_type_rejected():
    with pytest.raises(SchemaError) as ei:
        parse_manifest(_manifest(resources=[
            {"id": "x", "path": "resources/x.bin", "contentType": "binary"},
        ]))
    assert ei.value.details["reason"] == "invalid_content_type"


def test_resource_missing_content_type_rejected():
    with pytest.raises(SchemaError) as ei:
        parse_manifest(_manifest(resources=[
            {"id": "x", "path": "resources/x.md"},
        ]))
    assert ei.value.details["reason"] == "invalid_content_type"


def test_empty_resources_array_rejected():
    with pytest.raises(SchemaError) as ei:
        parse_manifest(_manifest(resources=[]))
    assert ei.value.details["reason"] == "empty_resources"


def test_resource_id_collides_with_prompt_id():
    with pytest.raises(SchemaError) as ei:
        parse_manifest(_manifest(resources=[
            {"id": "base", "path": "resources/base.md", "contentType": "markdown"},
        ]))
    assert ei.value.details["reason"] == "duplicate_id"


def test_resource_path_collides_with_prompt_path_under_case_folding():
    with pytest.raises(SchemaError) as ei:
        parse_manifest(_manifest(resources=[
            {"id": "x", "path": "Prompts/BASE.yaml", "contentType": "markdown"},
        ]))
    assert ei.value.details["reason"] == "path_case_collision"


def test_duplicate_resource_ids_rejected():
    with pytest.raises(SchemaError) as ei:
        parse_manifest(_manifest(resources=[
            {"id": "dup", "path": "resources/a.md", "contentType": "markdown"},
            {"id": "dup", "path": "resources/b.md", "contentType": "markdown"},
        ]))
    assert ei.value.details["reason"] == "duplicate_id"


def test_duplicate_resource_paths_rejected():
    with pytest.raises(SchemaError) as ei:
        parse_manifest(_manifest(resources=[
            {"id": "a", "path": "resources/x.md", "contentType": "markdown"},
            {"id": "b", "path": "resources/X.md", "contentType": "markdown"},
        ]))
    assert ei.value.details["reason"] == "path_case_collision"


def test_resource_entry_by_id_lookup():
    m = parse_manifest(_manifest(resources=[
        {"id": "overview", "path": "resources/overview.md", "contentType": "markdown"},
    ]))
    assert m.resource_by_id("overview") is not None
    assert m.resource_by_id("overview").path == "resources/overview.md"
    assert m.resource_by_id("missing") is None


def test_resource_entry_by_path_lookup():
    m = parse_manifest(_manifest(resources=[
        {"id": "overview", "path": "resources/overview.md", "contentType": "markdown"},
    ]))
    assert m.resource_by_path("resources/overview.md") is not None
    assert m.resource_by_path("resources/missing.md") is None
