import io
import json
import tarfile

import pytest

from stemmata.errors import (
    AggregatedError,
    EXIT_CYCLE,
    EXIT_SCHEMA,
    EXIT_UNRESOLVABLE,
)
from stemmata.npmrc import NpmConfig
from stemmata.publish import PublishOptions, run_publish
from stemmata.registry import RegistryClient


class _RecordingRegistry(RegistryClient):
    def __init__(self):
        super().__init__(config=NpmConfig(entries={"registry": "https://registry.example.com/"}), offline=False)
        self.published: list[tuple[str, str, bytes, dict]] = []

    def publish_tarball(self, name, version, tarball, *, manifest):
        self.published.append((name, version, tarball, manifest))
        return f"https://registry.example.com/{name}", b'{"ok":true}'


def _write_pkg(tmp_path, manifest_data, files: dict):
    (tmp_path / "package.json").write_text(json.dumps(manifest_data))
    for rel, content in files.items():
        full = tmp_path / rel
        full.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, bytes):
            full.write_bytes(content)
        else:
            full.write_text(content)


def _opts(tmp_path, **overrides):
    base = dict(
        package_root=tmp_path,
        dry_run=True,
        config=NpmConfig(entries={"registry": "https://registry.example.com/"}),
        cache_root=tmp_path / ".cache",
    )
    base.update(overrides)
    return PublishOptions(**base)


def test_dry_run_builds_tarball_no_upload(tmp_path):
    _write_pkg(tmp_path, {
        "name": "@acme/p",
        "version": "1.0.0",
        "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
    }, {
        "prompts/base.yaml": "vars:\n  region: eu\nbody: ${vars.region}\n",
    })
    result = run_publish(_opts(tmp_path))
    assert result.uploaded is False
    assert result.tarball_size > 0
    assert result.integrity.startswith("sha512-")
    assert result.prompts_checked == ["@acme/p@1.0.0#base"]


def test_aggregates_unresolvable_placeholder(tmp_path):
    _write_pkg(tmp_path, {
        "name": "@acme/p",
        "version": "1.0.0",
        "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
    }, {
        "prompts/base.yaml": "body: ${missing.var}\n",
    })
    with pytest.raises(AggregatedError) as ei:
        run_publish(_opts(tmp_path))
    assert ei.value.code == EXIT_UNRESOLVABLE
    assert any(e["code"] == EXIT_UNRESOLVABLE for e in ei.value.details["errors"])


def test_aggregates_multiple_placeholder_failures_in_one_prompt(tmp_path):
    _write_pkg(tmp_path, {
        "name": "@acme/p",
        "version": "1.0.0",
        "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
    }, {
        "prompts/base.yaml": "body: ${a.b} ${c.d} ${e.f}\n",
    })
    with pytest.raises(AggregatedError) as ei:
        run_publish(_opts(tmp_path))
    placeholders = [e for e in ei.value.details["errors"] if e["code"] == EXIT_UNRESOLVABLE]
    assert len(placeholders) == 3


def test_cycle_aggregated_with_higher_priority(tmp_path):
    _write_pkg(tmp_path, {
        "name": "@acme/p",
        "version": "1.0.0",
        "prompts": [
            {"id": "a", "path": "prompts/a.yaml"},
            {"id": "b", "path": "prompts/b.yaml"},
        ],
    }, {
        "prompts/a.yaml": "ancestors:\n  - ./b.yaml\n",
        "prompts/b.yaml": "ancestors:\n  - ./a.yaml\n",
    })
    with pytest.raises(AggregatedError) as ei:
        run_publish(_opts(tmp_path))
    assert ei.value.code == EXIT_CYCLE


def test_intra_document_type_conflict_aggregated_as_schema(tmp_path):
    _write_pkg(tmp_path, {
        "name": "@acme/p",
        "version": "1.0.0",
        "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
    }, {
        # `a: 1` (scalar) alongside `a.b: 2` (implies a is a map)
        "prompts/base.yaml": "a: 1\na.b: 2\n",
    })
    with pytest.raises(AggregatedError) as ei:
        run_publish(_opts(tmp_path))
    assert ei.value.code == EXIT_SCHEMA
    schema_errs = [e for e in ei.value.details["errors"] if e["code"] == EXIT_SCHEMA]
    assert any("intra_doc_type_conflict" in e["details"].get("reason", "") for e in schema_errs)


def test_dependency_consistency_failure_aggregated(tmp_path):
    _write_pkg(tmp_path, {
        "name": "@acme/p",
        "version": "1.0.0",
        # No `dependencies` declared, but the prompt references one.
        "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
    }, {
        "prompts/base.yaml": (
            "ancestors:\n"
            "  - package: '@other/lib'\n"
            "    version: '1.0.0'\n"
            "    prompt: x\n"
        ),
    })
    with pytest.raises(AggregatedError) as ei:
        run_publish(_opts(tmp_path))
    # The prompt resolution itself will network-fail before deps_check runs;
    # one of the aggregated errors is still the network/missing reference.
    # The deps_check error is also present because we always run it.
    reasons = [e["details"].get("reason", "") for e in ei.value.details["errors"] if e["code"] == EXIT_SCHEMA]
    assert "missing_dependency" in reasons


def test_publish_uploads_when_not_dry_run(tmp_path, monkeypatch):
    _write_pkg(tmp_path, {
        "name": "@acme/p",
        "version": "1.0.0",
        "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
    }, {
        "prompts/base.yaml": "vars:\n  x: 1\nbody: ${vars.x}\n",
    })

    recording = _RecordingRegistry()
    # Patch RegistryClient construction inside publish.run_publish so the
    # orchestrator uses our recording double.
    import stemmata.publish as pub
    monkeypatch.setattr(pub, "RegistryClient", lambda **kw: recording)

    opts = _opts(tmp_path, dry_run=False)
    result = run_publish(opts)
    assert result.uploaded is True
    assert recording.published, "publish_tarball was not called"
    name, version, tar, manifest = recording.published[0]
    assert name == "@acme/p"
    assert version == "1.0.0"
    assert manifest["name"] == "@acme/p"
    # Verify the tarball is valid gzip with our package layout.
    with tarfile.open(fileobj=io.BytesIO(tar), mode="r:gz") as tf:
        names = [m.name for m in tf.getmembers()]
    assert "package/package.json" in names
    assert "package/prompts/base.yaml" in names


def test_unfetchable_schema_is_always_error(tmp_path):
    _write_pkg(tmp_path, {
        "name": "@acme/p",
        "version": "1.0.0",
        "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
    }, {
        "prompts/base.yaml": '$schema: "https://example.invalid/schema.json"\nx: 1\n',
    })
    with pytest.raises(AggregatedError) as ei:
        run_publish(_opts(tmp_path, offline=True))
    reasons = [e["details"].get("reason") for e in ei.value.details["errors"]]
    assert "schema_unavailable_offline" in reasons


def test_unfetchable_local_schema_is_always_error(tmp_path):
    _write_pkg(tmp_path, {
        "name": "@acme/p",
        "version": "1.0.0",
        "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
    }, {
        "prompts/base.yaml": '$schema: "./missing.schema.json"\nx: 1\n',
    })
    with pytest.raises(AggregatedError) as ei:
        run_publish(_opts(tmp_path))
    reasons = [e["details"].get("reason") for e in ei.value.details["errors"]]
    assert "schema_file_not_found" in reasons


def _tar_member(tgz_path, endswith):
    with tarfile.open(tgz_path, mode="r:gz") as tf:
        m = next(x for x in tf.getmembers() if x.name.endswith(endswith))
        return tf.extractfile(m).read()


def test_publish_normalises_bom_in_yaml_prompt(tmp_path):
    _write_pkg(tmp_path, {
        "name": "@acme/p",
        "version": "1.0.0",
        "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
    }, {
        "prompts/base.yaml": b"\xef\xbb\xbfa: 1\nb: 2\n",
    })
    result = run_publish(_opts(tmp_path, tarball_out=tmp_path / "out.tgz"))
    assert result.tarball_size > 0
    assert _tar_member(tmp_path / "out.tgz", "base.yaml") == b"a: 1\nb: 2\n"


def test_publish_normalises_crlf_in_yaml_prompt(tmp_path):
    _write_pkg(tmp_path, {
        "name": "@acme/p",
        "version": "1.0.0",
        "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
    }, {
        "prompts/base.yaml": b"a: 1\r\nb: 2\r\n",
    })
    result = run_publish(_opts(tmp_path, tarball_out=tmp_path / "out.tgz"))
    assert result.tarball_size > 0
    assert _tar_member(tmp_path / "out.tgz", "base.yaml") == b"a: 1\nb: 2\n"


def test_publish_normalises_bom_and_crlf_together(tmp_path):
    _write_pkg(tmp_path, {
        "name": "@acme/p",
        "version": "1.0.0",
        "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
    }, {
        "prompts/base.yaml": b"\xef\xbb\xbfa: 1\r\nb: 2\r\n",
    })
    result = run_publish(_opts(tmp_path, tarball_out=tmp_path / "out.tgz"))
    assert result.tarball_size > 0
    assert _tar_member(tmp_path / "out.tgz", "base.yaml") == b"a: 1\nb: 2\n"


def test_publish_normalises_bom_and_crlf_in_markdown_resource(tmp_path):
    _write_pkg(tmp_path, {
        "name": "@acme/p",
        "version": "1.0.0",
        "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
        "resources": [{"id": "f", "path": "resources/f.md", "contentType": "markdown"}],
    }, {
        "prompts/base.yaml": b'body: "${resource:../resources/f.md}"\n',
        "resources/f.md": b"\xef\xbb\xbfhello\r\nworld\r\n",
    })
    result = run_publish(_opts(tmp_path, tarball_out=tmp_path / "out.tgz"))
    assert result.tarball_size > 0
    assert _tar_member(tmp_path / "out.tgz", "f.md") == b"hello\nworld\n"


def test_publish_with_bom_crlf_still_runs_placeholder_checks(tmp_path):
    _write_pkg(tmp_path, {
        "name": "@acme/p",
        "version": "1.0.0",
        "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
    }, {
        "prompts/base.yaml": b'\xef\xbb\xbfbody: "${missing.placeholder}"\r\n',
    })
    with pytest.raises(AggregatedError) as ei:
        run_publish(_opts(tmp_path))
    codes = [e["code"] for e in ei.value.details["errors"]]
    assert EXIT_UNRESOLVABLE in codes
