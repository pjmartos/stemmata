import io
import json
from pathlib import Path

import pytest

from stemmata.cli import run
from stemmata.errors import EXIT_OK, EXIT_SCHEMA, EXIT_USAGE


class _Capture:
    def __init__(self):
        self.out = io.StringIO()
        self.err = io.StringIO()


def _read_manifest(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_init_creates_manifest_in_empty_dir(tmp_path):
    target = tmp_path / "my-pkg"
    target.mkdir()
    cap = _Capture()
    code = run(["--output", "json", "init", str(target)], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_OK, cap.err.getvalue()
    env = json.loads(cap.out.getvalue())
    assert env["result"]["created"] is True
    manifest = _read_manifest(target / "package.json")
    assert manifest["name"] == "my-pkg"
    assert manifest["version"] == "0.0.1.dev0"
    assert manifest["license"] == "Apache-2.0"
    assert manifest["prompts"] == []
    assert "resources" not in manifest


def test_init_scans_prompts_and_resources(tmp_path):
    (tmp_path / "prompts").mkdir()
    (tmp_path / "prompts" / "base.yaml").write_text("foo: bar\n")
    (tmp_path / "prompts" / "extras.json").write_text("{}\n")
    (tmp_path / "prompts" / "sub").mkdir()
    (tmp_path / "prompts" / "sub" / "deep.yml").write_text("x: 1\n")
    (tmp_path / "prompts" / "README.txt").write_text("ignored\n")
    (tmp_path / "resources").mkdir()
    (tmp_path / "resources" / "overview.md").write_text("# Overview\n")
    (tmp_path / "resources" / "sections").mkdir()
    (tmp_path / "resources" / "sections" / "deployment.md").write_text("deploy\n")

    cap = _Capture()
    code = run(["--output", "json", "init", str(tmp_path)], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_OK, cap.err.getvalue()

    manifest = _read_manifest(tmp_path / "package.json")
    assert manifest["prompts"] == [
        {"id": "base", "path": "prompts/base.yaml", "contentType": "yaml"},
        {"id": "extras", "path": "prompts/extras.json", "contentType": "json"},
        {"id": "deep", "path": "prompts/sub/deep.yml", "contentType": "yaml"},
    ]
    assert manifest["resources"] == [
        {"id": "overview", "path": "resources/overview.md", "contentType": "markdown"},
        {"id": "deployment", "path": "resources/sections/deployment.md", "contentType": "markdown"},
    ]


def test_init_defaults_to_current_dir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "prompts").mkdir()
    (tmp_path / "prompts" / "a.yaml").write_text("k: v\n")
    cap = _Capture()
    code = run(["--output", "json", "init"], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_OK, cap.err.getvalue()
    manifest = _read_manifest(tmp_path / "package.json")
    assert manifest["name"] == tmp_path.name
    assert manifest["prompts"][0]["path"] == "prompts/a.yaml"


def test_init_preserves_existing_fields_and_merges(tmp_path):
    (tmp_path / "prompts").mkdir()
    (tmp_path / "prompts" / "base.yaml").write_text("foo: bar\n")
    (tmp_path / "prompts" / "newone.yaml").write_text("x: 1\n")
    (tmp_path / "resources").mkdir()
    (tmp_path / "resources" / "a.md").write_text("a\n")

    existing = {
        "name": "@acme/keep",
        "version": "1.2.3",
        "license": "MIT",
        "description": "preserved",
        "dependencies": {"@acme/common": "1.0.0"},
        "prompts": [
            {"id": "custom-base", "path": "prompts/base.yaml", "contentType": "yaml"},
            {"id": "manual", "path": "prompts/manual.yaml", "contentType": "yaml"},
        ],
        "resources": [
            {"id": "a", "path": "resources/a.md", "contentType": "markdown"},
        ],
    }
    (tmp_path / "package.json").write_text(json.dumps(existing, indent=2), encoding="utf-8")

    cap = _Capture()
    code = run(["--output", "json", "init", str(tmp_path)], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_OK, cap.err.getvalue()

    env = json.loads(cap.out.getvalue())
    assert env["result"]["created"] is False

    manifest = _read_manifest(tmp_path / "package.json")
    assert manifest["name"] == "@acme/keep"
    assert manifest["version"] == "1.2.3"
    assert manifest["license"] == "MIT"
    assert manifest["description"] == "preserved"
    assert manifest["dependencies"] == {"@acme/common": "1.0.0"}
    assert manifest["prompts"] == [
        {"id": "custom-base", "path": "prompts/base.yaml", "contentType": "yaml"},
        {"id": "manual", "path": "prompts/manual.yaml", "contentType": "yaml"},
        {"id": "newone", "path": "prompts/newone.yaml", "contentType": "yaml"},
    ]
    assert manifest["resources"] == [
        {"id": "a", "path": "resources/a.md", "contentType": "markdown"},
    ]


def test_init_fills_in_missing_defaults_when_manifest_partial(tmp_path):
    (tmp_path / "package.json").write_text(json.dumps({"description": "x"}), encoding="utf-8")
    cap = _Capture()
    code = run(["--output", "json", "init", str(tmp_path)], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_OK, cap.err.getvalue()
    manifest = _read_manifest(tmp_path / "package.json")
    assert manifest["name"] == tmp_path.name
    assert manifest["version"] == "0.0.1.dev0"
    assert manifest["license"] == "Apache-2.0"
    assert manifest["description"] == "x"


def test_init_is_idempotent(tmp_path):
    (tmp_path / "prompts").mkdir()
    (tmp_path / "prompts" / "b.yaml").write_text("k: v\n")
    (tmp_path / "prompts" / "a.yaml").write_text("k: v\n")
    cap = _Capture()
    assert run(["init", str(tmp_path)], stdout=cap.out, stderr=cap.err) == EXIT_OK
    first = (tmp_path / "package.json").read_text(encoding="utf-8")
    cap2 = _Capture()
    assert run(["init", str(tmp_path)], stdout=cap2.out, stderr=cap2.err) == EXIT_OK
    assert (tmp_path / "package.json").read_text(encoding="utf-8") == first


def test_init_rejects_invalid_json(tmp_path):
    (tmp_path / "package.json").write_text("{not json", encoding="utf-8")
    cap = _Capture()
    code = run(["--output", "json", "init", str(tmp_path)], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_SCHEMA


def test_init_rejects_non_directory(tmp_path):
    missing = tmp_path / "nope"
    cap = _Capture()
    code = run(["init", str(missing)], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_USAGE


def test_init_rejects_invalid_derived_id(tmp_path):
    (tmp_path / "prompts").mkdir()
    (tmp_path / "prompts" / "Bad Name.yaml").write_text("k: v\n")
    cap = _Capture()
    code = run(["--output", "json", "init", str(tmp_path)], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_SCHEMA


def test_init_renders_entries_single_line_with_aligned_fields(tmp_path):
    (tmp_path / "prompts").mkdir()
    (tmp_path / "prompts" / "base.yaml").write_text("k: v\n")
    (tmp_path / "prompts" / "onboarding.yaml").write_text("k: v\n")
    (tmp_path / "resources").mkdir()
    (tmp_path / "resources" / "r.md").write_text("r\n")
    cap = _Capture()
    assert run(["init", str(tmp_path)], stdout=cap.out, stderr=cap.err) == EXIT_OK
    text = (tmp_path / "package.json").read_text(encoding="utf-8")

    assert '    { "id": "base",       "path": "prompts/base.yaml",       "contentType": "yaml" },\n' in text
    assert '    { "id": "onboarding", "path": "prompts/onboarding.yaml", "contentType": "yaml" }\n' in text
    assert '    { "id": "r", "path": "resources/r.md", "contentType": "markdown" }\n' in text

    def _slice(marker: str) -> list[str]:
        lines = text.splitlines()
        start = next(i for i, ln in enumerate(lines) if ln.strip() == f'"{marker}": [')
        end = next(i for i, ln in enumerate(lines[start:], start=start) if ln.strip() in ("]", "],"))
        return [ln for ln in lines[start + 1:end] if ln.startswith('    { "id":')]

    prompt_lines = _slice("prompts")
    assert len(prompt_lines) == 2
    assert len({ln.index('"path":') for ln in prompt_lines}) == 1
    assert len({ln.index('"contentType":') for ln in prompt_lines}) == 1


def test_init_sorts_merged_entries_by_path(tmp_path):
    (tmp_path / "prompts").mkdir()
    (tmp_path / "prompts" / "zeta.yaml").write_text("k: v\n")
    (tmp_path / "prompts" / "alpha.yaml").write_text("k: v\n")
    existing = {
        "name": "@x/y",
        "version": "0.0.1",
        "prompts": [{"id": "mike", "path": "prompts/mike.yaml", "contentType": "yaml"}],
    }
    (tmp_path / "package.json").write_text(json.dumps(existing), encoding="utf-8")
    cap = _Capture()
    assert run(["init", str(tmp_path)], stdout=cap.out, stderr=cap.err) == EXIT_OK
    manifest = _read_manifest(tmp_path / "package.json")
    paths = [p["path"] for p in manifest["prompts"]]
    assert paths == ["prompts/alpha.yaml", "prompts/mike.yaml", "prompts/zeta.yaml"]
