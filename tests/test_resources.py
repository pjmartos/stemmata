"""End-to-end tests for ``${resource:...}`` Markdown embedding"""
import io
import json
import tarfile

import pytest

from stemmata.cache import Cache
from stemmata.errors import (
    CycleError,
    PromptCliError,
    ReferenceError_,
    SchemaError,
)
from stemmata.interp import Layer, interpolate
from stemmata.merge import merge_namespaces
from stemmata.npmrc import NpmConfig
from stemmata.registry import RegistryClient
from stemmata.resolver import Session, layer_order, resolve_graph
from stemmata.resource_resolve import build_resource_binding


# ---------------------------------------------------------------------------
# fake registry (mirrors the helper in test_resolver.py)
# ---------------------------------------------------------------------------

class _FakeRegistry(RegistryClient):
    def __init__(self, tarballs):
        super().__init__(config=NpmConfig(entries={}), offline=False)
        self.tarballs = tarballs

    def fetch_tarball(self, name, version):
        key = (name, version)
        if key not in self.tarballs:
            from stemmata.errors import NetworkError
            raise NetworkError(f"{name}@{version}", 404, "not found")
        return f"fake://{name}/{version}", self.tarballs[key]


def _pack(manifest: dict, files: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        data = json.dumps(manifest).encode()
        ti = tarfile.TarInfo("package/package.json")
        ti.size = len(data)
        ti.mode = 0o644
        tf.addfile(ti, io.BytesIO(data))
        for relpath, content in files.items():
            ti = tarfile.TarInfo(f"package/{relpath}")
            ti.size = len(content)
            ti.mode = 0o644
            tf.addfile(ti, io.BytesIO(content))
    return buf.getvalue()


def _session(tmp_path, tarballs=None):
    cache = Cache(root=tmp_path / "cache")
    reg = _FakeRegistry(tarballs or {})
    return Session(cache=cache, registry=reg)


def _resolve(tmp_path, target, tarballs=None):
    session = _session(tmp_path, tarballs=tarballs)
    graph = resolve_graph(str(target), session)
    order = layer_order(graph)
    layers_data = [graph.nodes[nid].doc.namespace for nid in order]
    merged = merge_namespaces(layers_data)
    layers = [Layer(canonical_id=nid.canonical, data=graph.nodes[nid].doc.namespace)
              for nid in order]
    root_file = graph.nodes[graph.root_id].file
    resources = build_resource_binding(graph, session)
    return interpolate(merged, layers, root_file=root_file, resources=resources), graph


# ---------------------------------------------------------------------------
# local-file tests
# ---------------------------------------------------------------------------

def test_local_prompt_inside_package_resolves_relative_resource(tmp_path):
    # Simulate an installed-package layout: package.json + prompts/ + resources/.
    pkg_root = tmp_path / "pkg"
    (pkg_root / "prompts").mkdir(parents=True)
    (pkg_root / "resources").mkdir(parents=True)
    (pkg_root / "package.json").write_text(json.dumps({
        "name": "@acme/p",
        "version": "1.0.0",
        "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
        "resources": [{"id": "footer", "path": "resources/footer.md", "contentType": "markdown"}],
    }))
    (pkg_root / "prompts" / "base.yaml").write_bytes(
        b"body: \"${resource:../resources/footer.md}\"\n"
    )
    (pkg_root / "resources" / "footer.md").write_bytes(b"hello from footer\n")

    # Prime the session's package cache with this on-disk package so the
    # resolver can collapse the local file's path to a package.
    session = _session(tmp_path)
    from stemmata.manifest import parse_manifest
    manifest = parse_manifest((pkg_root / "package.json").read_text())
    session._manifest_by_pkg[("@acme/p", "1.0.0")] = (manifest, pkg_root)

    graph = resolve_graph(str(pkg_root / "prompts" / "base.yaml"), session)
    order = layer_order(graph)
    layers_data = [graph.nodes[nid].doc.namespace for nid in order]
    merged = merge_namespaces(layers_data)
    layers = [Layer(canonical_id=nid.canonical, data=graph.nodes[nid].doc.namespace)
              for nid in order]
    resources = build_resource_binding(graph, session)
    resolved = interpolate(merged, layers, root_file=graph.nodes[graph.root_id].file, resources=resources)
    assert resolved["body"] == "hello from footer\n"


def test_local_prompt_resource_resolves_without_preseeding_session(tmp_path):
    pkg_root = tmp_path / "pkg"
    (pkg_root / "prompts").mkdir(parents=True)
    (pkg_root / "resources").mkdir(parents=True)
    (pkg_root / "package.json").write_text(json.dumps({
        "name": "@acme/p",
        "version": "1.0.0",
        "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
        "resources": [{"id": "footer", "path": "resources/footer.md", "contentType": "markdown"}],
    }))
    (pkg_root / "prompts" / "base.yaml").write_bytes(
        b'body: "${resource:../resources/footer.md}"\n'
    )
    (pkg_root / "resources" / "footer.md").write_bytes(b"hello from footer\n")

    session = _session(tmp_path)
    graph = resolve_graph(str(pkg_root / "prompts" / "base.yaml"), session)
    order = layer_order(graph)
    merged = merge_namespaces([graph.nodes[nid].doc.namespace for nid in order])
    layers = [Layer(canonical_id=nid.canonical, data=graph.nodes[nid].doc.namespace)
              for nid in order]
    resources = build_resource_binding(graph, session)
    resolved = interpolate(merged, layers, root_file=graph.nodes[graph.root_id].file, resources=resources)
    assert resolved["body"] == "hello from footer\n"


# ---------------------------------------------------------------------------
# registry-backed tests
# ---------------------------------------------------------------------------

def test_registry_prompt_substitutes_entire_flow_scalar(tmp_path):
    tarballs = {
        ("@acme/p", "1.0.0"): _pack(
            {
                "name": "@acme/p",
                "version": "1.0.0",
                "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
                "resources": [{"id": "body", "path": "resources/body.md", "contentType": "markdown"}],
            },
            {
                "prompts/base.yaml": b"greeting: \"${resource:../resources/body.md}\"\n",
                "resources/body.md": b"hello world\n",
            },
        ),
    }
    resolved, graph = _resolve(tmp_path, "@acme/p@1.0.0#base", tarballs)
    assert resolved["greeting"] == "hello world\n"


def test_registry_prompt_substitutes_inside_block_scalar(tmp_path):
    tarballs = {
        ("@acme/p", "1.0.0"): _pack(
            {
                "name": "@acme/p",
                "version": "1.0.0",
                "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
                "resources": [{"id": "body", "path": "resources/body.md", "contentType": "markdown"}],
            },
            {
                "prompts/base.yaml": b"body: |\n  intro\n  ${resource:../resources/body.md}\n  outro\n",
                "resources/body.md": b"middle\n",
            },
        ),
    }
    resolved, graph = _resolve(tmp_path, "@acme/p@1.0.0#base", tarballs)
    assert resolved["body"] == "intro\nmiddle\n\nouter\n".replace("outer", "outro")


def test_coordinate_reference_across_packages(tmp_path):
    tarballs = {
        ("@acme/common", "1.0.4"): _pack(
            {
                "name": "@acme/common",
                "version": "1.0.4",
                "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
                "resources": [{"id": "footer", "path": "resources/footer.md", "contentType": "markdown"}],
            },
            {
                "prompts/base.yaml": b"x: 1\n",
                "resources/footer.md": b"shared footer\n",
            },
        ),
        ("@acme/p", "1.0.0"): _pack(
            {
                "name": "@acme/p",
                "version": "1.0.0",
                "dependencies": {"@acme/common": "1.0.4"},
                "prompts": [{"id": "root", "path": "prompts/root.yaml"}],
            },
            {
                "prompts/root.yaml": b'footer: "${resource:@acme/common@1.0.4#footer}"\n',
            },
        ),
    }
    resolved, graph = _resolve(tmp_path, "@acme/p@1.0.0#root", tarballs)
    assert resolved["footer"] == "shared footer\n"


def test_markdown_embeds_markdown(tmp_path):
    tarballs = {
        ("@acme/p", "1.0.0"): _pack(
            {
                "name": "@acme/p",
                "version": "1.0.0",
                "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
                "resources": [
                    {"id": "outer", "path": "resources/outer.md", "contentType": "markdown"},
                    {"id": "inner", "path": "resources/inner.md", "contentType": "markdown"},
                ],
            },
            {
                "prompts/base.yaml": b'body: "${resource:../resources/outer.md}"\n',
                "resources/outer.md": b"before\n${resource:inner.md}\nafter\n",
                "resources/inner.md": b"INNER\n",
            },
        ),
    }
    resolved, graph = _resolve(tmp_path, "@acme/p@1.0.0#base", tarballs)
    # The line with ${resource:inner.md} is replaced by the inner flat text.
    # outer content is: "before\n${resource:inner.md}\nafter\n"
    # After substitution: "before\nINNER\n\nafter\n"
    assert "INNER" in resolved["body"]
    assert "before" in resolved["body"]
    assert "after" in resolved["body"]


def test_missing_resource_id_reports_reference_error(tmp_path):
    tarballs = {
        ("@acme/p", "1.0.0"): _pack(
            {
                "name": "@acme/p",
                "version": "1.0.0",
                "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
                "resources": [{"id": "known", "path": "resources/known.md", "contentType": "markdown"}],
            },
            {
                "prompts/base.yaml": b'body: "${resource:@acme/p@1.0.0#unknown}"\n',
                "resources/known.md": b"hello\n",
            },
        ),
    }
    with pytest.raises(ReferenceError_) as ei:
        _resolve(tmp_path, "@acme/p@1.0.0#base", tarballs)
    assert ei.value.details["kind"] == "resource"
    assert ei.value.details["reason"] == "missing"


def test_type_mismatch_when_resource_points_at_prompt(tmp_path):
    tarballs = {
        ("@acme/p", "1.0.0"): _pack(
            {
                "name": "@acme/p",
                "version": "1.0.0",
                "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
            },
            {
                # References itself via resource syntax — target is a prompt, not a resource.
                "prompts/base.yaml": b'body: "${resource:@acme/p@1.0.0#base}"\n',
            },
        ),
    }
    with pytest.raises(ReferenceError_) as ei:
        _resolve(tmp_path, "@acme/p@1.0.0#base", tarballs)
    assert ei.value.details["kind"] == "resource"
    assert ei.value.details["reason"] == "type_mismatch"


def test_resource_cycle_detected(tmp_path):
    tarballs = {
        ("@acme/p", "1.0.0"): _pack(
            {
                "name": "@acme/p",
                "version": "1.0.0",
                "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
                "resources": [
                    {"id": "a", "path": "resources/a.md", "contentType": "markdown"},
                    {"id": "b", "path": "resources/b.md", "contentType": "markdown"},
                ],
            },
            {
                "prompts/base.yaml": b'body: "${resource:../resources/a.md}"\n',
                "resources/a.md": b"${resource:b.md}\n",
                "resources/b.md": b"${resource:a.md}\n",
            },
        ),
    }
    with pytest.raises(CycleError) as ei:
        _resolve(tmp_path, "@acme/p@1.0.0#base", tarballs)
    assert ei.value.details["kind"] == "resource"
    files = [n["file"] for n in ei.value.location]
    assert files == [
        "@acme/p@1.0.0#a",
        "@acme/p@1.0.0#b",
        "@acme/p@1.0.0#a",
    ]
    for f in files:
        assert ".cache" not in f and "AT_acme__p" not in f


def test_position_validation_rejects_midline_in_flow_scalar(tmp_path):
    tarballs = {
        ("@acme/p", "1.0.0"): _pack(
            {
                "name": "@acme/p",
                "version": "1.0.0",
                "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
                "resources": [{"id": "x", "path": "resources/x.md", "contentType": "markdown"}],
            },
            {
                "prompts/base.yaml": b'body: "prefix ${resource:../resources/x.md} suffix"\n',
                "resources/x.md": b"content\n",
            },
        ),
    }
    with pytest.raises(SchemaError) as ei:
        _resolve(tmp_path, "@acme/p@1.0.0#base", tarballs)
    assert "resource" in ei.value.details["reason"]


def test_position_validation_rejects_midline_in_block_scalar(tmp_path):
    tarballs = {
        ("@acme/p", "1.0.0"): _pack(
            {
                "name": "@acme/p",
                "version": "1.0.0",
                "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
                "resources": [{"id": "x", "path": "resources/x.md", "contentType": "markdown"}],
            },
            {
                "prompts/base.yaml": b"body: |\n  prefix ${resource:../resources/x.md} suffix\n",
                "resources/x.md": b"content\n",
            },
        ),
    }
    with pytest.raises(SchemaError) as ei:
        _resolve(tmp_path, "@acme/p@1.0.0#base", tarballs)
    assert ei.value.details["reason"] == "resource_not_line_exclusive"


def test_escaped_resource_placeholder_passes_through_as_literal(tmp_path):
    tarballs = {
        ("@acme/p", "1.0.0"): _pack(
            {
                "name": "@acme/p",
                "version": "1.0.0",
                "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
            },
            {
                "prompts/base.yaml": b'body: "literal $${resource:foo.md} text"\n',
            },
        ),
    }
    resolved, graph = _resolve(tmp_path, "@acme/p@1.0.0#base", tarballs)
    assert resolved["body"] == "literal ${resource:foo.md} text"


def test_resource_placeholder_is_skipped_by_namespace_interp(tmp_path):
    # A namespace placeholder ${foo} should be resolved; a ${resource:...}
    # placeholder alongside should be handled by the resource pass without
    # the namespace interp trying to look up "resource:foo.md".
    tarballs = {
        ("@acme/p", "1.0.0"): _pack(
            {
                "name": "@acme/p",
                "version": "1.0.0",
                "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
                "resources": [{"id": "f", "path": "resources/f.md", "contentType": "markdown"}],
            },
            {
                "prompts/base.yaml": b'foo: value\nbody: "${resource:../resources/f.md}"\n',
                "resources/f.md": b"X\n",
            },
        ),
    }
    resolved, graph = _resolve(tmp_path, "@acme/p@1.0.0#base", tarballs)
    assert resolved["body"] == "X\n"


def test_unresolvable_resource_in_local_file_without_package(tmp_path):
    p = tmp_path / "solo.yaml"
    p.write_bytes(b'body: "${resource:foo.md}"\n')
    with pytest.raises(ReferenceError_) as ei:
        _resolve(tmp_path, p)
    assert ei.value.details["kind"] == "resource"
    assert ei.value.details["reason"] == "missing"
    assert ei.value.details["searched_in"] == "<local>"


def test_searched_in_is_coordinate_for_coord_form_missing(tmp_path):
    tarballs = {
        ("@acme/p", "1.0.0"): _pack(
            {
                "name": "@acme/p",
                "version": "1.0.0",
                "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
                "resources": [{"id": "known", "path": "resources/known.md", "contentType": "markdown"}],
            },
            {
                "prompts/base.yaml": b'body: "${resource:@acme/p@1.0.0#unknown}"\n',
                "resources/known.md": b"hi\n",
            },
        ),
    }
    with pytest.raises(ReferenceError_) as ei:
        _resolve(tmp_path, "@acme/p@1.0.0#base", tarballs)
    assert ei.value.details["searched_in"] == "@acme/p@1.0.0"


def test_searched_in_is_coordinate_for_coord_form_type_mismatch(tmp_path):
    tarballs = {
        ("@acme/p", "1.0.0"): _pack(
            {
                "name": "@acme/p",
                "version": "1.0.0",
                "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
            },
            {
                "prompts/base.yaml": b'body: "${resource:@acme/p@1.0.0#base}"\n',
            },
        ),
    }
    with pytest.raises(ReferenceError_) as ei:
        _resolve(tmp_path, "@acme/p@1.0.0#base", tarballs)
    assert ei.value.details["reason"] == "type_mismatch"
    assert ei.value.details["searched_in"] == "@acme/p@1.0.0"


def test_searched_in_is_local_for_relative_path_missing_in_package(tmp_path):
    tarballs = {
        ("@acme/p", "1.0.0"): _pack(
            {
                "name": "@acme/p",
                "version": "1.0.0",
                "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
                "resources": [{"id": "known", "path": "resources/known.md", "contentType": "markdown"}],
            },
            {
                "prompts/base.yaml": b'body: "${resource:../resources/missing.md}"\n',
                "resources/known.md": b"hi\n",
            },
        ),
    }
    with pytest.raises(ReferenceError_) as ei:
        _resolve(tmp_path, "@acme/p@1.0.0#base", tarballs)
    assert ei.value.details["reason"] == "missing"
    assert ei.value.details["searched_in"] == "<local>"


def test_searched_in_is_local_for_relative_path_type_mismatch(tmp_path):
    tarballs = {
        ("@acme/p", "1.0.0"): _pack(
            {
                "name": "@acme/p",
                "version": "1.0.0",
                "prompts": [
                    {"id": "base",     "path": "prompts/base.yaml"},
                    {"id": "sibling",  "path": "prompts/sibling.yaml"},
                ],
            },
            {
                "prompts/base.yaml":    b'body: "${resource:./sibling.yaml}"\n',
                "prompts/sibling.yaml": b"k: v\n",
            },
        ),
    }
    with pytest.raises(ReferenceError_) as ei:
        _resolve(tmp_path, "@acme/p@1.0.0#base", tarballs)
    assert ei.value.details["reason"] == "type_mismatch"
    assert ei.value.details["searched_in"] == "<local>"


def test_searched_in_is_local_when_relative_escapes_package_root(tmp_path):
    tarballs = {
        ("@acme/p", "1.0.0"): _pack(
            {
                "name": "@acme/p",
                "version": "1.0.0",
                "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
            },
            {
                "prompts/base.yaml": b'body: "${resource:../../escape.md}"\n',
            },
        ),
    }
    with pytest.raises(ReferenceError_) as ei:
        _resolve(tmp_path, "@acme/p@1.0.0#base", tarballs)
    assert ei.value.details["reason"] == "missing"
    assert ei.value.details["searched_in"] == "<local>"


# ---------------------------------------------------------------------------
# tree subcommand surfaces resource errors (no rendering of resources, but
# cycles / missing / type mismatches must still abort)
# ---------------------------------------------------------------------------

def test_tree_surfaces_resource_cycle(tmp_path):
    import io, json as _json
    from stemmata.cli import run as cli_run

    tarballs = {
        ("@acme/p", "1.0.0"): _pack(
            {
                "name": "@acme/p",
                "version": "1.0.0",
                "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
                "resources": [
                    {"id": "a", "path": "resources/a.md", "contentType": "markdown"},
                    {"id": "b", "path": "resources/b.md", "contentType": "markdown"},
                ],
            },
            {
                "prompts/base.yaml": b'body: "${resource:../resources/a.md}"\n',
                "resources/a.md": b"${resource:b.md}\n",
                "resources/b.md": b"${resource:a.md}\n",
            },
        ),
    }
    # Prime the cache with the tarball so tree can read it offline.
    from stemmata.cache import Cache as _Cache
    cache = _Cache(root=tmp_path / "cache")
    for (name, ver), data in tarballs.items():
        cache.install_tarball(name, ver, data)

    out, err = io.StringIO(), io.StringIO()
    rc = cli_run(
        ["--offline", "--cache-dir", str(tmp_path / "cache"), "--output", "json",
         "tree", "@acme/p@1.0.0#base"],
        stdout=out, stderr=err,
    )
    assert rc == 12  # EXIT_CYCLE
    payload = _json.loads(out.getvalue())
    assert payload["error"]["details"]["kind"] == "resource"


def test_tree_surfaces_missing_resource_reference(tmp_path):
    import io, json as _json
    from stemmata.cli import run as cli_run
    from stemmata.cache import Cache as _Cache

    tarballs = {
        ("@acme/p", "1.0.0"): _pack(
            {
                "name": "@acme/p",
                "version": "1.0.0",
                "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
                "resources": [{"id": "exists", "path": "resources/exists.md", "contentType": "markdown"}],
            },
            {
                "prompts/base.yaml": b'body: "${resource:@acme/p@1.0.0#missing}"\n',
                "resources/exists.md": b"hi\n",
            },
        ),
    }
    cache = _Cache(root=tmp_path / "cache")
    for (name, ver), data in tarballs.items():
        cache.install_tarball(name, ver, data)

    out, err = io.StringIO(), io.StringIO()
    rc = cli_run(
        ["--offline", "--cache-dir", str(tmp_path / "cache"), "--output", "json",
         "tree", "@acme/p@1.0.0#base"],
        stdout=out, stderr=err,
    )
    assert rc == 11  # EXIT_REFERENCE
    payload = _json.loads(out.getvalue())
    assert payload["error"]["details"]["kind"] == "resource"
    assert payload["error"]["details"]["reason"] == "missing"


def test_tree_still_renders_without_resource_refs(tmp_path):
    """Tree must keep working for prompts that have zero resource refs."""
    import io
    from stemmata.cli import run as cli_run
    from stemmata.cache import Cache as _Cache

    tarballs = {
        ("@acme/p", "1.0.0"): _pack(
            {
                "name": "@acme/p",
                "version": "1.0.0",
                "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
            },
            {"prompts/base.yaml": b"x: 1\n"},
        ),
    }
    cache = _Cache(root=tmp_path / "cache")
    for (name, ver), data in tarballs.items():
        cache.install_tarball(name, ver, data)

    out, err = io.StringIO(), io.StringIO()
    rc = cli_run(
        ["--offline", "--cache-dir", str(tmp_path / "cache"),
         "tree", "@acme/p@1.0.0#base"],
        stdout=out, stderr=err,
    )
    assert rc == 0
    assert "@acme/p@1.0.0#base" in out.getvalue()


def test_tree_renders_resources_with_prefix(tmp_path):
    """Direct and transitive Markdown resources appear under the prompt that
    references them, prefixed with ``resource:`` to disambiguate from prompts."""
    import io
    from stemmata.cli import run as cli_run
    from stemmata.cache import Cache as _Cache

    tarballs = {
        ("@acme/p", "1.0.0"): _pack(
            {
                "name": "@acme/p",
                "version": "1.0.0",
                "prompts": [{"id": "root", "path": "prompts/root.yaml"}],
                "resources": [
                    {"id": "outer", "path": "resources/outer.md", "contentType": "markdown"},
                    {"id": "inner", "path": "resources/inner.md", "contentType": "markdown"},
                ],
            },
            {
                "prompts/root.yaml": b'body: "${resource:../resources/outer.md}"\n',
                "resources/outer.md": b"head\n${resource:inner.md}\ntail\n",
                "resources/inner.md": b"inner text\n",
            },
        ),
    }
    cache = _Cache(root=tmp_path / "cache")
    for (name, ver), data in tarballs.items():
        cache.install_tarball(name, ver, data)

    out, err = io.StringIO(), io.StringIO()
    rc = cli_run(
        ["--offline", "--cache-dir", str(tmp_path / "cache"),
         "tree", "@acme/p@1.0.0#root"],
        stdout=out, stderr=err,
    )
    assert rc == 0, out.getvalue() + err.getvalue()
    lines = out.getvalue().splitlines()
    assert lines[1] == "@acme/p@1.0.0#root"
    assert any("resource:@acme/p@1.0.0#outer" in line for line in lines[2:])
    assert any("resource:@acme/p@1.0.0#inner" in line for line in lines[2:])


def test_tree_diamond_resource_marks_revisit(tmp_path):
    """A resource reached from two different prompts is expanded once, then
    revisits print ``(seen)``, mirroring ancestor-DAG behaviour."""
    import io
    from stemmata.cli import run as cli_run
    from stemmata.cache import Cache as _Cache

    tarballs = {
        ("@acme/p", "1.0.0"): _pack(
            {
                "name": "@acme/p",
                "version": "1.0.0",
                "prompts": [
                    {"id": "leaf", "path": "prompts/leaf.yaml"},
                    {"id": "a", "path": "prompts/a.yaml"},
                    {"id": "b", "path": "prompts/b.yaml"},
                    {"id": "root", "path": "prompts/root.yaml"},
                ],
                "resources": [
                    {"id": "shared", "path": "resources/shared.md", "contentType": "markdown"},
                ],
            },
            {
                "prompts/leaf.yaml": b"x: 1\n",
                "prompts/a.yaml": (
                    b"ancestors:\n  - ./leaf.yaml\nbody: \"${resource:../resources/shared.md}\"\n"
                ),
                "prompts/b.yaml": (
                    b"ancestors:\n  - ./leaf.yaml\nbody: \"${resource:../resources/shared.md}\"\n"
                ),
                "prompts/root.yaml": b"ancestors:\n  - ./a.yaml\n  - ./b.yaml\n",
                "resources/shared.md": b"shared content\n",
            },
        ),
    }
    cache = _Cache(root=tmp_path / "cache")
    for (name, ver), data in tarballs.items():
        cache.install_tarball(name, ver, data)

    out, err = io.StringIO(), io.StringIO()
    rc = cli_run(
        ["--offline", "--cache-dir", str(tmp_path / "cache"),
         "tree", "@acme/p@1.0.0#root"],
        stdout=out, stderr=err,
    )
    assert rc == 0, out.getvalue() + err.getvalue()
    text = out.getvalue()
    # Shared resource is reached via both a and b — one full expansion, one (seen).
    assert text.count("resource:@acme/p@1.0.0#shared") == 2
    assert text.count("(seen)") == 2  # shared resource + leaf prompt (via diamond)


def test_tree_json_tags_resource_nodes_and_edges(tmp_path):
    """Structured output carries ``kind`` on nodes and edges."""
    import io, json as _json
    from stemmata.cli import run as cli_run
    from stemmata.cache import Cache as _Cache

    tarballs = {
        ("@acme/p", "1.0.0"): _pack(
            {
                "name": "@acme/p",
                "version": "1.0.0",
                "prompts": [{"id": "root", "path": "prompts/root.yaml"}],
                "resources": [
                    {"id": "note", "path": "resources/note.md", "contentType": "markdown"},
                ],
            },
            {
                "prompts/root.yaml": b'body: "${resource:../resources/note.md}"\n',
                "resources/note.md": b"hi\n",
            },
        ),
    }
    cache = _Cache(root=tmp_path / "cache")
    for (name, ver), data in tarballs.items():
        cache.install_tarball(name, ver, data)

    out, err = io.StringIO(), io.StringIO()
    rc = cli_run(
        ["--offline", "--output", "json", "--cache-dir", str(tmp_path / "cache"),
         "tree", "@acme/p@1.0.0#root"],
        stdout=out, stderr=err,
    )
    assert rc == 0, out.getvalue() + err.getvalue()
    env = _json.loads(out.getvalue())
    nodes = {n["id"]: n for n in env["result"]["nodes"]}
    assert nodes["@acme/p@1.0.0#root"]["kind"] == "prompt"
    assert nodes["@acme/p@1.0.0#root"]["distance"] == 0
    assert nodes["@acme/p@1.0.0#note"]["kind"] == "resource"
    assert nodes["@acme/p@1.0.0#note"]["distance"] == 1
    edges = env["result"]["edges"]
    assert {
        "from": "@acme/p@1.0.0#root",
        "to": "@acme/p@1.0.0#note",
        "kind": "resource",
    } in edges


# ---------------------------------------------------------------------------
# validate surfaces resource errors even for prompts with no ancestors
# ---------------------------------------------------------------------------

def test_validate_surfaces_missing_resource_in_standalone_file(tmp_path):
    import io
    from stemmata.cli import run as cli_run

    schema = '{"type": "object"}'
    (tmp_path / "s.json").write_bytes(schema.encode())
    prompt = b'$schema: "s.json"\nbody: "${resource:missing.md}"\n'
    (tmp_path / "p.yaml").write_bytes(prompt)

    out, err = io.StringIO(), io.StringIO()
    rc = cli_run(["--output", "json", "validate", str(tmp_path / "p.yaml")],
                 stdout=out, stderr=err)
    # No ancestors, no package, and a dangling relative resource ref must
    # still be caught at validate time.
    assert rc == 11  # EXIT_REFERENCE


# ---------------------------------------------------------------------------
# absolute-path resource reference rejection (mirrors Fix 7 for ancestors)
# ---------------------------------------------------------------------------

def test_absolute_path_resource_ref_rejected_in_yaml_prompt(tmp_path):
    tarballs = {
        ("@acme/p", "1.0.0"): _pack(
            {
                "name": "@acme/p",
                "version": "1.0.0",
                "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
                "resources": [{"id": "body", "path": "resources/body.md", "contentType": "markdown"}],
            },
            {
                "prompts/base.yaml": b'greeting: "${resource:/abs/body.md}"\n',
                "resources/body.md": b"hello\n",
            },
        ),
    }
    with pytest.raises(SchemaError) as exc:
        _resolve(tmp_path, "@acme/p@1.0.0#base", tarballs)
    assert exc.value.code == 10
    assert exc.value.details["reason"] == "absolute_path"


def test_absolute_path_resource_ref_rejected_inside_markdown(tmp_path):
    tarballs = {
        ("@acme/p", "1.0.0"): _pack(
            {
                "name": "@acme/p",
                "version": "1.0.0",
                "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
                "resources": [
                    {"id": "a", "path": "resources/a.md", "contentType": "markdown"},
                    {"id": "b", "path": "resources/b.md", "contentType": "markdown"},
                ],
            },
            {
                "prompts/base.yaml": b'body: "${resource:../resources/a.md}"\n',
                "resources/a.md": b"header\n${resource:/abs/b.md}\nfooter\n",
                "resources/b.md": b"b\n",
            },
        ),
    }
    with pytest.raises(SchemaError) as exc:
        _resolve(tmp_path, "@acme/p@1.0.0#base", tarballs)
    assert exc.value.code == 10
    assert exc.value.details["reason"] == "absolute_path"


def test_absolute_path_resource_ref_rejected_even_without_package_context(tmp_path):
    root = tmp_path / "root.yaml"
    root.write_text('body: "${resource:/abs/foo.md}"\n')
    session = _session(tmp_path)
    graph = resolve_graph(str(root), session)
    with pytest.raises(SchemaError) as exc:
        build_resource_binding(graph, session)
    assert exc.value.code == 10
    assert exc.value.details["reason"] == "absolute_path"


@pytest.mark.parametrize(
    "content_type,ext,payload",
    [
        ("text", "txt", b"plain text payload\n"),
        ("xml", "xml", b"<root><item>x</item></root>\n"),
        ("json", "json", b'{"k": "v"}\n'),
        ("yaml", "yaml", b"k: v\n"),
    ],
)
def test_non_markdown_resource_substituted_verbatim(tmp_path, content_type, ext, payload):
    rel = f"resources/body.{ext}"
    tarballs = {
        ("@acme/p", "1.0.0"): _pack(
            {
                "name": "@acme/p",
                "version": "1.0.0",
                "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
                "resources": [{"id": "body", "path": rel, "contentType": content_type}],
            },
            {
                "prompts/base.yaml": f'greeting: "${{resource:../{rel}}}"\n'.encode(),
                rel: payload,
            },
        ),
    }
    resolved, _ = _resolve(tmp_path, "@acme/p@1.0.0#base", tarballs)
    assert resolved["greeting"] == payload.decode()
