import json

from stemmata.deps_check import check_consistency
from stemmata.manifest import parse_manifest


def _write_pkg(tmp_path, manifest_data, prompts: dict):
    (tmp_path / "package.json").write_text(json.dumps(manifest_data))
    for rel, content in prompts.items():
        full = tmp_path / rel
        full.parent.mkdir(parents=True, exist_ok=True)
        full.write_text(content)


def _manifest(tmp_path):
    return parse_manifest(
        (tmp_path / "package.json").read_text(),
        file=str(tmp_path / "package.json"),
    )


def test_consistent_dependencies(tmp_path):
    m = {
        "name": "@x/y",
        "version": "1.0.0",
        "dependencies": {"@acme/common": "1.0.4"},
        "prompts": [{"id": "main", "path": "prompts/main.yaml"}],
    }
    _write_pkg(tmp_path, m, {
        "prompts/main.yaml": (
            "ancestors:\n"
            "  - package: '@acme/common'\n"
            "    version: '1.0.4'\n"
            "    prompt: defaults\n"
            "x: 1\n"
        ),
    })
    errors = check_consistency(_manifest(tmp_path), tmp_path, manifest_file=str(tmp_path / "package.json"))
    assert errors == []


def test_missing_dependency_reported(tmp_path):
    m = {
        "name": "@x/y",
        "version": "1.0.0",
        "prompts": [{"id": "main", "path": "prompts/main.yaml"}],
    }
    _write_pkg(tmp_path, m, {
        "prompts/main.yaml": (
            "ancestors:\n"
            "  - package: '@acme/common'\n"
            "    version: '1.0.4'\n"
            "    prompt: defaults\n"
        ),
    })
    errors = check_consistency(_manifest(tmp_path), tmp_path, manifest_file=str(tmp_path / "package.json"))
    assert len(errors) == 1
    assert errors[0].details["reason"] == "missing_dependency"


def test_version_mismatch_reported(tmp_path):
    m = {
        "name": "@x/y",
        "version": "1.0.0",
        "dependencies": {"@acme/common": "1.0.0"},
        "prompts": [{"id": "main", "path": "prompts/main.yaml"}],
    }
    _write_pkg(tmp_path, m, {
        "prompts/main.yaml": (
            "ancestors:\n"
            "  - package: '@acme/common'\n"
            "    version: '1.0.4'\n"
            "    prompt: defaults\n"
        ),
    })
    errors = check_consistency(_manifest(tmp_path), tmp_path, manifest_file=str(tmp_path / "package.json"))
    assert len(errors) == 1
    assert errors[0].details["reason"] == "version_mismatch"


def test_unused_dependency_reported(tmp_path):
    m = {
        "name": "@x/y",
        "version": "1.0.0",
        "dependencies": {"@acme/common": "1.0.4"},
        "prompts": [{"id": "main", "path": "prompts/main.yaml"}],
    }
    _write_pkg(tmp_path, m, {"prompts/main.yaml": "x: 1\n"})
    errors = check_consistency(_manifest(tmp_path), tmp_path, manifest_file=str(tmp_path / "package.json"))
    assert len(errors) == 1
    assert errors[0].details["reason"] == "unused_dependency"


def test_undeclared_local_ref_reported(tmp_path):
    m = {
        "name": "@x/y",
        "version": "1.0.0",
        "prompts": [{"id": "main", "path": "prompts/main.yaml"}],
    }
    _write_pkg(tmp_path, m, {
        "prompts/main.yaml": "ancestors:\n  - ./loose.yaml\n",
        # `loose.yaml` exists on disk but is NOT in the manifest's `prompts`,
        # so it would not be bundled into the published tarball.
        "prompts/loose.yaml": "x: 1\n",
    })
    errors = check_consistency(_manifest(tmp_path), tmp_path, manifest_file=str(tmp_path / "package.json"))
    assert len(errors) == 1
    assert errors[0].details["reason"] == "undeclared_local_ref"


def test_declared_local_ref_accepted(tmp_path):
    m = {
        "name": "@x/y",
        "version": "1.0.0",
        "prompts": [
            {"id": "main", "path": "prompts/main.yaml"},
            {"id": "base", "path": "prompts/base.yaml"},
        ],
    }
    _write_pkg(tmp_path, m, {
        "prompts/main.yaml": "ancestors:\n  - ./base.yaml\n",
        "prompts/base.yaml": "x: 1\n",
    })
    errors = check_consistency(_manifest(tmp_path), tmp_path, manifest_file=str(tmp_path / "package.json"))
    assert errors == []


def test_multiple_versions_of_same_dep_reported(tmp_path):
    m = {
        "name": "@x/y",
        "version": "1.0.0",
        "dependencies": {"@acme/common": "1.0.4"},
        "prompts": [
            {"id": "a", "path": "prompts/a.yaml"},
            {"id": "b", "path": "prompts/b.yaml"},
        ],
    }
    _write_pkg(tmp_path, m, {
        "prompts/a.yaml": (
            "ancestors:\n"
            "  - package: '@acme/common'\n"
            "    version: '1.0.4'\n"
            "    prompt: defaults\n"
        ),
        "prompts/b.yaml": (
            "ancestors:\n"
            "  - package: '@acme/common'\n"
            "    version: '2.0.0'\n"
            "    prompt: defaults\n"
        ),
    })
    errors = check_consistency(_manifest(tmp_path), tmp_path, manifest_file=str(tmp_path / "package.json"))
    assert len(errors) == 1
    assert errors[0].details["reason"] == "multiple_versions_referenced"
