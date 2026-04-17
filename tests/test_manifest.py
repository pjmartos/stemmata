import json

import pytest

from stemmata.errors import SchemaError
from stemmata.manifest import is_scoped_name, is_semver, parse_manifest


def test_valid_manifest():
    raw = json.dumps({
        "name": "@acme/core",
        "version": "1.2.3",
        "prompts": [{"id": "base", "path": "prompts/base.yaml", "contentType": "yaml"}],
    })
    m = parse_manifest(raw)
    assert m.name == "@acme/core"
    assert m.prompts[0].id == "base"


def test_default_id_from_path():
    raw = json.dumps({
        "name": "@acme/core",
        "version": "1.0.0",
        "prompts": [{"path": "prompts/onboarding.yaml"}],
    })
    m = parse_manifest(raw)
    assert m.prompts[0].id == "onboarding"
    assert m.prompts[0].contentType == "yaml"


def test_default_id_with_dot_rejected_due_to_grammar():
    raw = json.dumps({
        "name": "@acme/core",
        "version": "1.0.0",
        "prompts": [{"path": "prompts/foo.v1.yaml"}],
    })
    with pytest.raises(SchemaError):
        parse_manifest(raw)


def test_default_id_strips_last_extension_only():
    raw = json.dumps({
        "name": "@acme/core",
        "version": "1.0.0",
        "prompts": [{"id": "foo-v1", "path": "prompts/foo.v1.yaml"}],
    })
    m = parse_manifest(raw)
    assert m.prompts[0].id == "foo-v1"


def test_default_id_from_nested_path():
    raw = json.dumps({
        "name": "@acme/core",
        "version": "1.0.0",
        "prompts": [{"path": "prompts/subdir/base.yaml"}],
    })
    m = parse_manifest(raw)
    assert m.prompts[0].id == "base"


def test_default_id_rejects_uppercase():
    raw = json.dumps({
        "name": "@acme/core",
        "version": "1.0.0",
        "prompts": [{"path": "prompts/Onboarding.yaml"}],
    })
    with pytest.raises(SchemaError):
        parse_manifest(raw)


def test_empty_prompts_rejected():
    raw = json.dumps({"name": "@acme/core", "version": "1.0.0", "prompts": []})
    with pytest.raises(SchemaError):
        parse_manifest(raw)


def test_invalid_name_rejected():
    raw = json.dumps({"name": "noscope", "version": "1.0.0", "prompts": [{"path": "prompts/a.yaml"}]})
    with pytest.raises(SchemaError):
        parse_manifest(raw)


def test_invalid_version_rejected():
    raw = json.dumps({"name": "@a/b", "version": "1.0", "prompts": [{"path": "prompts/a.yaml"}]})
    with pytest.raises(SchemaError):
        parse_manifest(raw)


def test_path_outside_prompts_dir_allowed():
    raw = json.dumps({"name": "@a/b", "version": "1.0.0", "prompts": [{"path": "other/a.yaml"}]})
    m = parse_manifest(raw)
    assert m.prompts[0].path == "other/a.yaml"


def test_path_sibling_to_manifest_allowed():
    raw = json.dumps({"name": "@a/b", "version": "1.0.0", "prompts": [{"path": "defaults.yaml"}]})
    m = parse_manifest(raw)
    assert m.prompts[0].path == "defaults.yaml"


def test_path_case_collision():
    raw = json.dumps({
        "name": "@a/b",
        "version": "1.0.0",
        "prompts": [
            {"id": "one", "path": "prompts/base.yaml"},
            {"id": "two", "path": "prompts/BASE.yaml"},
        ],
    })
    with pytest.raises(SchemaError):
        parse_manifest(raw)


def test_duplicate_ids_rejected():
    raw = json.dumps({
        "name": "@a/b",
        "version": "1.0.0",
        "prompts": [
            {"id": "x", "path": "prompts/a.yaml"},
            {"id": "x", "path": "prompts/b.yaml"},
        ],
    })
    with pytest.raises(SchemaError):
        parse_manifest(raw)


def test_dependencies_validation():
    raw = json.dumps({
        "name": "@a/b",
        "version": "1.0.0",
        "dependencies": {"@foo/bar": "2.0.0"},
        "prompts": [{"path": "prompts/a.yaml"}],
    })
    m = parse_manifest(raw)
    assert m.dependencies == {"@foo/bar": "2.0.0"}


def test_content_type_json_accepted():
    raw = json.dumps({
        "name": "@a/b",
        "version": "1.0.0",
        "prompts": [{"id": "a", "path": "prompts/a.json", "contentType": "json"}],
    })
    m = parse_manifest(raw)
    assert m.prompts[0].contentType == "json"


def test_content_type_invalid_rejected():
    raw = json.dumps({
        "name": "@a/b",
        "version": "1.0.0",
        "prompts": [{"id": "a", "path": "prompts/a.xml", "contentType": "xml"}],
    })
    with pytest.raises(SchemaError):
        parse_manifest(raw)


def test_diagnostic_identifies_entry_by_path_not_index():
    raw = json.dumps({
        "name": "@a/b",
        "version": "1.0.0",
        "prompts": [
            {"id": "ok", "path": "prompts/ok.yaml"},
            {"id": "Bad-Id", "path": "prompts/bad.yaml"},
        ],
    })
    with pytest.raises(SchemaError) as exc:
        parse_manifest(raw)
    msg = str(exc.value)
    assert "'prompts/bad.yaml'" in msg
    assert "prompts[1]" not in msg
    assert "index=" not in msg


def test_diagnostic_reorder_invariant_for_wellformed_entries():
    a = {"id": "ok", "path": "prompts/ok.yaml"}
    b = {"id": "Bad-Id", "path": "prompts/bad.yaml"}
    with pytest.raises(SchemaError) as exc_ab:
        parse_manifest(json.dumps({"name": "@a/b", "version": "1.0.0", "prompts": [a, b]}))
    with pytest.raises(SchemaError) as exc_ba:
        parse_manifest(json.dumps({"name": "@a/b", "version": "1.0.0", "prompts": [b, a]}))
    assert str(exc_ab.value) == str(exc_ba.value)
    assert exc_ab.value.details == exc_ba.value.details


def test_diagnostic_uses_index_only_when_entry_has_no_identifier():
    raw = json.dumps({
        "name": "@a/b",
        "version": "1.0.0",
        "prompts": ["not-a-mapping"],
    })
    with pytest.raises(SchemaError) as exc:
        parse_manifest(raw)
    assert "index=0" in str(exc.value)


def test_is_scoped_name_boundaries():
    assert is_scoped_name("@a/b")
    assert is_scoped_name("@acme_co/prompts-core")
    assert not is_scoped_name("noscope")
    assert not is_scoped_name("@Acme/core")
    assert not is_scoped_name("@/core")


def test_is_semver_boundaries():
    assert is_semver("1.2.3")
    assert is_semver("1.0.0-alpha.1")
    assert is_semver("1.0.0+build.2")
    assert not is_semver("1.0")
    assert not is_semver("v1.0.0")
    assert not is_semver("1.0.0-SNAPSHOT")
