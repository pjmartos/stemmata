import http.server
import io
import json
import socketserver
import tarfile
import threading
from pathlib import Path

import pytest

from stemmata.cli import run
from stemmata.errors import (
    EXIT_ABSTRACT_UNFILLED,
    EXIT_CYCLE,
    EXIT_GENERIC,
    EXIT_MERGE,
    EXIT_NETWORK,
    EXIT_OFFLINE,
    EXIT_OK,
    EXIT_REFERENCE,
    EXIT_SCHEMA,
    EXIT_UNRESOLVABLE,
    EXIT_USAGE,
)


class _Capture:
    def __init__(self):
        self.out = io.StringIO()
        self.err = io.StringIO()


def _pack(manifest: dict, files: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        data = json.dumps(manifest).encode()
        ti = tarfile.TarInfo("package/package.json")
        ti.size = len(data)
        ti.mode = 0o644
        tf.addfile(ti, io.BytesIO(data))
        for path, content in files.items():
            ti = tarfile.TarInfo(f"package/{path}")
            ti.size = len(content)
            ti.mode = 0o644
            tf.addfile(ti, io.BytesIO(content))
    return buf.getvalue()


class _RegistryServer:
    def __init__(self, tarballs: dict[tuple[str, str], bytes]):
        self.tarballs = tarballs
        handler = self._make_handler()
        self.httpd = socketserver.TCPServer(("127.0.0.1", 0), handler)
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def _make_handler(self):
        tarballs = self.tarballs

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *a, **k):
                pass

            def do_GET(self):
                parts = self.path.strip("/").split("/")
                if len(parts) >= 3 and parts[-2] == "-" and parts[-1].endswith(".tgz"):
                    scope = parts[0]
                    name = parts[1]
                    full = f"{scope}/{name}"
                    filename = parts[-1]
                    version = filename[len(name) + 1:-4]
                    data = tarballs.get((full, version))
                    if data is None:
                        self.send_error(404)
                        return
                    self.send_response(200)
                    self.send_header("Content-Type", "application/octet-stream")
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                else:
                    self.send_error(404)

        return Handler

    def start(self):
        self.thread.start()

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()

    @property
    def url(self):
        return f"http://127.0.0.1:{self.port}/"


@pytest.fixture
def npmrc(tmp_path):
    def _make(entries: dict[str, str]) -> Path:
        p = tmp_path / ".npmrc"
        p.write_text("".join(f"{k}={v}\n" for k, v in entries.items()))
        return p
    return _make


def _run(argv, stdout=None, stderr=None):
    cap = _Capture()
    code = run(argv, stdout=stdout or cap.out, stderr=stderr or cap.err)
    return code, (stdout.getvalue() if stdout else cap.out.getvalue()), (stderr.getvalue() if stderr else cap.err.getvalue())


def test_resolve_local_yaml_output(tmp_path):
    base = tmp_path / "base.yaml"
    base.write_text("vars:\n  x: 1\n")
    child = tmp_path / "child.yaml"
    child.write_text("ancestors:\n  - ./base.yaml\nbody: value=${vars.x}\n")
    cap = _Capture()
    code = run(["--cache-dir", str(tmp_path / "cache"), "resolve", str(child)], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_OK
    assert "body: value=1" in cap.out.getvalue()


def test_resolve_local_json_output(tmp_path):
    child = tmp_path / "child.yaml"
    child.write_text("foo: bar\n")
    cap = _Capture()
    code = run(["--output", "json", "--cache-dir", str(tmp_path / "cache"), "resolve", str(child)], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_OK
    env = json.loads(cap.out.getvalue())
    assert env["status"] == "ok"
    assert env["result"]["content"] == {"foo": "bar"}


def test_resolve_missing_target_usage_error(tmp_path):
    cap = _Capture()
    code = run(["resolve"], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_USAGE


def test_resolve_schema_error_rejects_python_tag(tmp_path):
    child = tmp_path / "bad.yaml"
    child.write_text("foo: !!python/object/apply:os.system ['ls']\n")
    cap = _Capture()
    code = run(["--output", "json", "--cache-dir", str(tmp_path / "cache"), "resolve", str(child)], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_SCHEMA
    env = json.loads(cap.out.getvalue())
    assert env["error"]["category"] == "schema_validation"


def test_resolve_reference_error_missing_file(tmp_path):
    child = tmp_path / "x.yaml"
    child.write_text("ancestors:\n  - ./nowhere.yaml\n")
    cap = _Capture()
    code = run(["--output", "json", "--cache-dir", str(tmp_path / "cache"), "resolve", str(child)], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_REFERENCE


def test_resolve_cycle_detected(tmp_path):
    a = tmp_path / "a.yaml"
    b = tmp_path / "b.yaml"
    a.write_text("ancestors:\n  - ./b.yaml\n")
    b.write_text("ancestors:\n  - ./a.yaml\n")
    cap = _Capture()
    code = run(["--output", "json", "--cache-dir", str(tmp_path / "cache"), "resolve", str(a)], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_CYCLE


def test_resolve_unresolvable_placeholder(tmp_path):
    child = tmp_path / "x.yaml"
    child.write_text("body: ${missing.path}\n")
    cap = _Capture()
    code = run(["--output", "json", "--cache-dir", str(tmp_path / "cache"), "resolve", str(child)], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_UNRESOLVABLE
    env = json.loads(cap.out.getvalue())
    assert env["error"]["details"]["reason"] == "not_provided"


def test_resolve_explicit_null_placeholder(tmp_path):
    base = tmp_path / "base.yaml"
    base.write_text("val: 'something'\n")
    child = tmp_path / "child.yaml"
    child.write_text("ancestors:\n  - ./base.yaml\nval: null\nbody: x=${val}\n")
    cap = _Capture()
    code = run(["--output", "json", "--cache-dir", str(tmp_path / "cache"), "resolve", str(child)], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_UNRESOLVABLE
    env = json.loads(cap.out.getvalue())
    assert env["error"]["details"]["reason"] == "explicit_null"


def test_resolve_merge_error_non_scalar_in_textual(tmp_path):
    base = tmp_path / "b.yaml"
    base.write_text("val:\n  - a\n  - b\n")
    child = tmp_path / "c.yaml"
    child.write_text("ancestors:\n  - ./b.yaml\nbody: prefix ${val} suffix\n")
    cap = _Capture()
    code = run(["--output", "json", "--cache-dir", str(tmp_path / "cache"), "resolve", str(child)], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_MERGE


def test_resolve_registry_fetch_success(tmp_path, npmrc):
    manifest = {"name": "@a/b", "version": "1.0.0", "prompts": [{"id": "base", "path": "prompts/base.yaml"}]}
    tarballs = {("@a/b", "1.0.0"): _pack(manifest, {"prompts/base.yaml": b"vars:\n  x: 42\n"})}
    server = _RegistryServer(tarballs)
    server.start()
    try:
        rc = npmrc({"@a:registry": server.url})
        child = tmp_path / "c.yaml"
        child.write_text(
            "ancestors:\n  - package: '@a/b'\n    version: '1.0.0'\n    prompt: base\n"
            "body: value=${vars.x}\n"
        )
        cap = _Capture()
        code = run([
            "--npmrc", str(rc),
            "--cache-dir", str(tmp_path / "cache"),
            "resolve", str(child),
        ], stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK, cap.out.getvalue() + cap.err.getvalue()
        assert "value=42" in cap.out.getvalue()
    finally:
        server.stop()


def test_resolve_registry_404(tmp_path, npmrc):
    server = _RegistryServer({})
    server.start()
    try:
        rc = npmrc({"@a:registry": server.url})
        child = tmp_path / "c.yaml"
        child.write_text(
            "ancestors:\n  - package: '@a/b'\n    version: '1.0.0'\n    prompt: base\n"
        )
        cap = _Capture()
        code = run([
            "--output", "json",
            "--npmrc", str(rc),
            "--cache-dir", str(tmp_path / "cache"),
            "resolve", str(child),
        ], stdout=cap.out, stderr=cap.err)
        assert code == EXIT_NETWORK
    finally:
        server.stop()


def test_resolve_offline_violation(tmp_path, npmrc):
    rc = npmrc({"@a:registry": "http://127.0.0.1:1/"})
    child = tmp_path / "c.yaml"
    child.write_text(
        "ancestors:\n  - package: '@a/b'\n    version: '1.0.0'\n    prompt: base\n"
    )
    cap = _Capture()
    code = run([
        "--offline",
        "--output", "json",
        "--npmrc", str(rc),
        "--cache-dir", str(tmp_path / "cache"),
        "resolve", str(child),
    ], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_OFFLINE


def test_cache_clear_empty(tmp_path):
    cap = _Capture()
    code = run(["--cache-dir", str(tmp_path / "cache"), "--output", "json", "cache", "clear"], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_OK
    env = json.loads(cap.out.getvalue())
    assert env["result"]["entries_removed"] == 0


def test_cache_clear_after_install(tmp_path):
    from stemmata.cache import Cache
    cache = Cache(root=tmp_path / "cache")
    manifest = {"name": "@a/b", "version": "1.0.0", "prompts": [{"path": "prompts/x.yaml"}]}
    cache.install_tarball("@a/b", "1.0.0", _pack(manifest, {"prompts/x.yaml": b"foo: 1\n"}))
    cap = _Capture()
    code = run(["--cache-dir", str(tmp_path / "cache"), "--output", "json", "cache", "clear"], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_OK
    env = json.loads(cap.out.getvalue())
    assert env["result"]["entries_removed"] == 1


def test_refresh_forces_refetch(tmp_path, npmrc):
    manifest = {"name": "@a/b", "version": "1.0.0", "prompts": [{"id": "base", "path": "prompts/base.yaml"}]}
    tarballs = {("@a/b", "1.0.0"): _pack(manifest, {"prompts/base.yaml": b"vars:\n  x: 1\n"})}
    server = _RegistryServer(tarballs)
    server.start()
    try:
        rc = npmrc({"@a:registry": server.url})
        child = tmp_path / "c.yaml"
        child.write_text(
            "ancestors:\n  - package: '@a/b'\n    version: '1.0.0'\n    prompt: base\n"
            "body: ${vars.x}\n"
        )
        args = [
            "--npmrc", str(rc),
            "--cache-dir", str(tmp_path / "cache"),
            "resolve", str(child),
        ]
        cap1 = _Capture()
        assert run(args, stdout=cap1.out, stderr=cap1.err) == EXIT_OK
        cap2 = _Capture()
        assert run(["--refresh"] + args, stdout=cap2.out, stderr=cap2.err) == EXIT_OK
    finally:
        server.stop()


def test_json_error_envelope_on_default_output(tmp_path):
    child = tmp_path / "x.yaml"
    child.write_text("body: ${missing}\n")
    cap = _Capture()
    code = run(["--cache-dir", str(tmp_path / "cache"), "resolve", str(child)], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_UNRESOLVABLE
    env = json.loads(cap.out.getvalue())
    assert env["status"] == "error"
    assert env["error"]["code"] == EXIT_UNRESOLVABLE


def test_stderr_empty_on_success(tmp_path):
    child = tmp_path / "x.yaml"
    child.write_text("foo: bar\n")
    cap = _Capture()
    code = run(["--cache-dir", str(tmp_path / "cache"), "resolve", str(child)], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_OK
    assert cap.err.getvalue() == ""


def test_stderr_has_summary_on_error(tmp_path):
    child = tmp_path / "x.yaml"
    child.write_text("body: ${nope}\n")
    cap = _Capture()
    code = run(["--cache-dir", str(tmp_path / "cache"), "resolve", str(child)], stdout=cap.out, stderr=cap.err)
    assert code != EXIT_OK
    assert cap.err.getvalue() != ""


def test_no_subcommand_is_usage_error(tmp_path):
    cap = _Capture()
    code = run([], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_USAGE


def test_full_example(tmp_path):
    base = tmp_path / "base.yaml"
    base.write_text(
        "vars:\n  region: \"eu-west-1\"\n  timeout_seconds: 30\n"
        "database:\n  host: \"db-base.internal\"\n  port: 5432\n"
    )
    onboarding = tmp_path / "onboarding.yaml"
    onboarding.write_text(
        "ancestors:\n  - ./base.yaml\n"
        "vars:\n  region: \"us-east-1\"\n"
        "database:\n  ssl: true\n"
        "body: |\n"
        "  Running in ${vars.region} with timeout ${vars.timeout_seconds}.\n"
        "  Connecting to ${database.host}:${database.port} (ssl=${database.ssl}).\n"
    )
    cap = _Capture()
    code = run(["--cache-dir", str(tmp_path / "cache"), "resolve", str(onboarding)], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_OK
    text = cap.out.getvalue()
    assert "Running in us-east-1 with timeout 30." in text
    assert "Connecting to db-base.internal:5432 (ssl=true)." in text


def test_list_splat_integration(tmp_path):
    base = tmp_path / "b.yaml"
    base.write_text("items:\n  - one\n  - two\n  - three\n")
    child = tmp_path / "c.yaml"
    child.write_text(
        "ancestors:\n  - ./b.yaml\n"
        "list:\n"
        "  - head\n"
        "  - ${items}\n"
        "  - tail\n"
    )
    cap = _Capture()
    code = run(["--output", "json", "--cache-dir", str(tmp_path / "cache"), "resolve", str(child)], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_OK
    env = json.loads(cap.out.getvalue())
    assert env["result"]["content"]["list"] == ["head", "one", "two", "three", "tail"]


def test_non_splat_form_integration(tmp_path):
    base = tmp_path / "b.yaml"
    base.write_text("items:\n  - one\n  - two\n")
    child = tmp_path / "c.yaml"
    child.write_text(
        "ancestors:\n  - ./b.yaml\n"
        "list:\n  - ${=items}\n"
    )
    cap = _Capture()
    code = run(["--output", "json", "--cache-dir", str(tmp_path / "cache"), "resolve", str(child)], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_OK
    env = json.loads(cap.out.getvalue())
    assert env["result"]["content"]["list"] == [["one", "two"]]


def test_escape_dollar_integration(tmp_path):
    child = tmp_path / "c.yaml"
    child.write_text('body: "a $${literal} b"\n')
    cap = _Capture()
    code = run(["--output", "json", "--cache-dir", str(tmp_path / "cache"), "resolve", str(child)], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_OK
    env = json.loads(cap.out.getvalue())
    assert env["result"]["content"]["body"] == "a ${literal} b"


def test_diamond_bfs_order_integration(tmp_path):
    x = tmp_path / "x.yaml"
    x.write_text("color: red\n")
    a = tmp_path / "a.yaml"
    a.write_text("ancestors:\n  - ./x.yaml\nshape: square\n")
    b = tmp_path / "b.yaml"
    b.write_text("ancestors:\n  - ./x.yaml\nshape: circle\n")
    root = tmp_path / "root.yaml"
    root.write_text(
        "ancestors:\n  - ./a.yaml\n  - ./b.yaml\n"
        "body: '${color}/${shape}'\n"
    )
    cap = _Capture()
    code = run(["--output", "json", "--cache-dir", str(tmp_path / "cache"), "resolve", str(root)], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_OK
    env = json.loads(cap.out.getvalue())
    assert env["result"]["content"]["body"] == "red/square"


def test_tree_missing_target_usage_error(tmp_path):
    cap = _Capture()
    code = run(["tree"], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_USAGE


def test_tree_local_text_output(tmp_path):
    base = tmp_path / "base.yaml"
    base.write_text("vars:\n  x: 1\n")
    child = tmp_path / "child.yaml"
    child.write_text("ancestors:\n  - ./base.yaml\nbody: ${vars.x}\n")
    cap = _Capture()
    code = run(["--cache-dir", str(tmp_path / "cache"), "tree", str(child)], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_OK, cap.out.getvalue() + cap.err.getvalue()
    out = cap.out.getvalue()
    child_real = str(child.resolve())
    base_real = str(base.resolve())
    assert out.startswith("\n")
    lines = out.splitlines()
    assert lines[0] == ""
    assert lines[1] == child_real
    assert any(base_real in line and "`-- " in line for line in lines[2:])


def test_tree_diamond_marks_revisit(tmp_path):
    x = tmp_path / "x.yaml"
    x.write_text("color: red\n")
    a = tmp_path / "a.yaml"
    a.write_text("ancestors:\n  - ./x.yaml\nshape: square\n")
    b = tmp_path / "b.yaml"
    b.write_text("ancestors:\n  - ./x.yaml\nshape: circle\n")
    root = tmp_path / "root.yaml"
    root.write_text("ancestors:\n  - ./a.yaml\n  - ./b.yaml\nbody: ok\n")
    cap = _Capture()
    code = run(["--cache-dir", str(tmp_path / "cache"), "tree", str(root)], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_OK, cap.out.getvalue() + cap.err.getvalue()
    out = cap.out.getvalue()
    assert out.count("(seen)") == 1


def test_tree_json_envelope(tmp_path):
    base = tmp_path / "base.yaml"
    base.write_text("vars:\n  x: 1\n")
    child = tmp_path / "child.yaml"
    child.write_text("ancestors:\n  - ./base.yaml\nbody: ${vars.x}\n")
    cap = _Capture()
    code = run(["--output", "json", "--cache-dir", str(tmp_path / "cache"), "tree", str(child)], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_OK
    env = json.loads(cap.out.getvalue())
    assert env["command"] == "tree"
    assert env["result"]["root"] == str(child.resolve())
    ids = [n["id"] for n in env["result"]["nodes"]]
    assert str(child.resolve()) in ids
    assert str(base.resolve()) in ids
    assert any(e["from"] == str(child.resolve()) and e["to"] == str(base.resolve()) for e in env["result"]["edges"])


def test_tree_registry_coord(tmp_path, npmrc):
    manifest = {
        "name": "@a/b",
        "version": "1.0.0",
        "prompts": [
            {"id": "base", "path": "prompts/base.yaml"},
            {"id": "child", "path": "prompts/child.yaml"},
        ],
    }
    files = {
        "prompts/base.yaml": b"vars:\n  x: 1\n",
        "prompts/child.yaml": b"ancestors:\n  - ./base.yaml\nbody: ${vars.x}\n",
    }
    tarballs = {("@a/b", "1.0.0"): _pack(manifest, files)}
    server = _RegistryServer(tarballs)
    server.start()
    try:
        rc = npmrc({"@a:registry": server.url})
        cap = _Capture()
        code = run([
            "--npmrc", str(rc),
            "--cache-dir", str(tmp_path / "cache"),
            "tree", "@a/b@1.0.0#child",
        ], stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK, cap.out.getvalue() + cap.err.getvalue()
        out = cap.out.getvalue()
        assert out.startswith("\n")
        lines = out.splitlines()
        assert lines[1] == "@a/b@1.0.0#child"
        assert any("@a/b@1.0.0#base" in line for line in lines[2:])
    finally:
        server.stop()


def test_describe_missing_target_usage_error(tmp_path):
    cap = _Capture()
    code = run(["describe"], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_USAGE


def test_describe_invalid_coord_usage_error(tmp_path):
    cap = _Capture()
    code = run(["describe", "./not-a-coord.yaml"], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_USAGE


def test_describe_single_prompt_yaml_default(tmp_path, npmrc):
    manifest = {
        "name": "@a/b",
        "version": "1.0.0",
        "prompts": [
            {"id": "base", "path": "prompts/base.yaml"},
            {"id": "child", "path": "prompts/child.yaml"},
        ],
    }
    files = {
        "prompts/base.yaml": b"vars:\n  x: 1\n",
        "prompts/child.yaml": (
            b"ancestors:\n  - ./base.yaml\n"
            b"body: value=${vars.x}\n"
        ),
    }
    tarballs = {("@a/b", "1.0.0"): _pack(manifest, files)}
    server = _RegistryServer(tarballs)
    server.start()
    try:
        rc = npmrc({"@a:registry": server.url})
        cap = _Capture()
        code = run([
            "--npmrc", str(rc),
            "--cache-dir", str(tmp_path / "cache"),
            "describe", "@a/b@1.0.0#child",
        ], stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK, cap.out.getvalue() + cap.err.getvalue()
        out = cap.out.getvalue()
        assert "body: value=1" in out
        assert "# @a/b@1.0.0#child" in out
    finally:
        server.stop()


def test_describe_package_multidoc_yaml(tmp_path, npmrc):
    manifest = {
        "name": "@a/b",
        "version": "1.0.0",
        "prompts": [
            {"id": "base", "path": "prompts/base.yaml"},
            {"id": "child", "path": "prompts/child.yaml"},
        ],
    }
    files = {
        "prompts/base.yaml": b"vars:\n  x: 1\nbody: base=${vars.x}\n",
        "prompts/child.yaml": (
            b"ancestors:\n  - ./base.yaml\n"
            b"vars:\n  x: 2\n"
            b"body: child=${vars.x}\n"
        ),
    }
    tarballs = {("@a/b", "1.0.0"): _pack(manifest, files)}
    server = _RegistryServer(tarballs)
    server.start()
    try:
        rc = npmrc({"@a:registry": server.url})
        cap = _Capture()
        code = run([
            "--npmrc", str(rc),
            "--cache-dir", str(tmp_path / "cache"),
            "describe", "@a/b@1.0.0",
        ], stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK, cap.out.getvalue() + cap.err.getvalue()
        out = cap.out.getvalue()
        import yaml as _yaml
        docs = list(_yaml.safe_load_all(out))
        assert len(docs) == 2
        assert docs[0]["body"] == "base=1"
        assert docs[1]["body"] == "child=2"
        assert out.count("---") == 2
        assert "# @a/b@1.0.0#base" in out
        assert "# @a/b@1.0.0#child" in out
    finally:
        server.stop()


def test_describe_package_uses_cache_without_network(tmp_path, npmrc):
    manifest = {
        "name": "@a/b",
        "version": "1.0.0",
        "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
    }
    tarballs = {("@a/b", "1.0.0"): _pack(manifest, {"prompts/base.yaml": b"body: hello\n"})}
    server = _RegistryServer(tarballs)
    server.start()
    try:
        rc = npmrc({"@a:registry": server.url})
        cache_dir = tmp_path / "cache"
        cap1 = _Capture()
        code = run([
            "--npmrc", str(rc),
            "--cache-dir", str(cache_dir),
            "describe", "@a/b@1.0.0",
        ], stdout=cap1.out, stderr=cap1.err)
        assert code == EXIT_OK
    finally:
        server.stop()
    cap2 = _Capture()
    code = run([
        "--offline",
        "--npmrc", str(tmp_path / ".npmrc"),
        "--cache-dir", str(cache_dir),
        "describe", "@a/b@1.0.0",
    ], stdout=cap2.out, stderr=cap2.err)
    assert code == EXIT_OK, cap2.out.getvalue() + cap2.err.getvalue()
    assert "body: hello" in cap2.out.getvalue()


def test_describe_unknown_prompt_id_reference_error(tmp_path, npmrc):
    manifest = {"name": "@a/b", "version": "1.0.0", "prompts": [{"id": "base", "path": "prompts/base.yaml"}]}
    tarballs = {("@a/b", "1.0.0"): _pack(manifest, {"prompts/base.yaml": b"foo: bar\n"})}
    server = _RegistryServer(tarballs)
    server.start()
    try:
        rc = npmrc({"@a:registry": server.url})
        cap = _Capture()
        code = run([
            "--output", "json",
            "--npmrc", str(rc),
            "--cache-dir", str(tmp_path / "cache"),
            "describe", "@a/b@1.0.0#nope",
        ], stdout=cap.out, stderr=cap.err)
        assert code == EXIT_REFERENCE
    finally:
        server.stop()


def test_describe_single_prompt_json_envelope(tmp_path, npmrc):
    manifest = {
        "name": "@a/b",
        "version": "1.0.0",
        "prompts": [
            {"id": "base", "path": "prompts/base.yaml"},
            {"id": "child", "path": "prompts/child.yaml"},
        ],
    }
    files = {
        "prompts/base.yaml": b"vars:\n  x: 1\n",
        "prompts/child.yaml": (
            b"ancestors:\n  - ./base.yaml\n"
            b"body: value=${vars.x}\n"
        ),
    }
    tarballs = {("@a/b", "1.0.0"): _pack(manifest, files)}
    server = _RegistryServer(tarballs)
    server.start()
    try:
        rc = npmrc({"@a:registry": server.url})
        cap = _Capture()
        code = run([
            "--output", "json",
            "--npmrc", str(rc),
            "--cache-dir", str(tmp_path / "cache"),
            "describe", "@a/b@1.0.0#child",
        ], stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK, cap.out.getvalue() + cap.err.getvalue()
        env = json.loads(cap.out.getvalue())
        assert env["status"] == "ok"
        assert env["command"] == "describe"
        assert isinstance(env["result"], list)
        assert len(env["result"]) == 1
        doc = env["result"][0]
        assert doc["root"] == "@a/b@1.0.0#child"
        assert doc["content"]["body"] == "value=1"
        assert any(a["canonical_id"] == "@a/b@1.0.0#base" for a in doc["ancestors"])
    finally:
        server.stop()


def test_describe_package_json_envelope(tmp_path, npmrc):
    manifest = {
        "name": "@a/b",
        "version": "1.0.0",
        "prompts": [
            {"id": "base", "path": "prompts/base.yaml"},
            {"id": "child", "path": "prompts/child.yaml"},
        ],
    }
    files = {
        "prompts/base.yaml": b"body: base\n",
        "prompts/child.yaml": b"ancestors:\n  - ./base.yaml\nbody: child\n",
    }
    tarballs = {("@a/b", "1.0.0"): _pack(manifest, files)}
    server = _RegistryServer(tarballs)
    server.start()
    try:
        rc = npmrc({"@a:registry": server.url})
        cap = _Capture()
        code = run([
            "--output", "json",
            "--npmrc", str(rc),
            "--cache-dir", str(tmp_path / "cache"),
            "describe", "@a/b@1.0.0",
        ], stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK, cap.out.getvalue() + cap.err.getvalue()
        env = json.loads(cap.out.getvalue())
        prompts = env["result"]
        assert isinstance(prompts, list)
        assert [p["root"] for p in prompts] == ["@a/b@1.0.0#base", "@a/b@1.0.0#child"]
        assert prompts[0]["content"]["body"] == "base"
        assert prompts[1]["content"]["body"] == "child"
    finally:
        server.stop()


# --- Abstract placeholders ---------------------------------------------------


def test_resolve_exits_16_on_unfilled_abstract(tmp_path):
    child = tmp_path / "x.yaml"
    child.write_text(
        "abstracts:\n"
        "  greeting:\n"
        "    description: opening line\n"
        "body: ${abstract:greeting}\n"
    )
    cap = _Capture()
    code = run(["--output", "json", "--cache-dir", str(tmp_path / "cache"),
                "resolve", str(child)],
               stdout=cap.out, stderr=cap.err)
    assert code == EXIT_ABSTRACT_UNFILLED
    env = json.loads(cap.out.getvalue())
    assert env["error"]["code"] == 16
    assert env["error"]["category"] == "abstract_unfilled"
    assert env["error"]["details"]["placeholder"] == "greeting"
    assert env["error"]["details"]["reason"] == "not_provided"


def test_resolve_succeeds_when_abstract_filled_by_descendant(tmp_path):
    base = tmp_path / "base.yaml"
    base.write_text(
        "abstracts:\n"
        "  greeting:\n"
        "    description: opening line\n"
        "greeting: ${abstract:greeting}\n"
    )
    child = tmp_path / "child.yaml"
    child.write_text(
        "ancestors:\n  - ./base.yaml\n"
        "greeting: hello\n"
        "body: ${greeting} world\n"
    )
    cap = _Capture()
    code = run(["--cache-dir", str(tmp_path / "cache"), "resolve", str(child)],
               stdout=cap.out, stderr=cap.err)
    assert code == EXIT_OK, cap.out.getvalue() + cap.err.getvalue()
    assert "hello world" in cap.out.getvalue()


def test_resolve_abstract_inherited_from_ancestor_fails(tmp_path):
    # Ancestor declares the abstract; child inherits without filling it.
    base = tmp_path / "base.yaml"
    base.write_text(
        "abstracts:\n"
        "  greeting:\n"
        "    description: opening line\n"
        "greeting: ${abstract:greeting}\n"
    )
    child = tmp_path / "child.yaml"
    child.write_text("ancestors:\n  - ./base.yaml\nbody: ${greeting}\n")
    cap = _Capture()
    code = run(["--output", "json", "--cache-dir", str(tmp_path / "cache"),
                "resolve", str(child)],
               stdout=cap.out, stderr=cap.err)
    assert code == EXIT_ABSTRACT_UNFILLED
    env = json.loads(cap.out.getvalue())
    assert env["error"]["details"]["reason"] == "abstract_inherited"


def test_tree_text_annotates_abstract_holes(tmp_path):
    base = tmp_path / "base.yaml"
    base.write_text(
        "abstracts:\n"
        "  greeting:\n"
        "    description: opening line\n"
        "greeting: ${abstract:greeting}\n"
    )
    child = tmp_path / "child.yaml"
    child.write_text("ancestors:\n  - ./base.yaml\nbody: x\n")
    cap = _Capture()
    code = run(["--cache-dir", str(tmp_path / "cache"), "tree", str(child)],
               stdout=cap.out, stderr=cap.err)
    assert code == EXIT_OK, cap.out.getvalue() + cap.err.getvalue()
    out = cap.out.getvalue()
    assert "[abstracts: greeting: string]" in out


def test_tree_json_includes_abstracts_per_node(tmp_path):
    base = tmp_path / "base.yaml"
    base.write_text(
        "abstracts:\n"
        "  greeting:\n"
        "    description: opening line\n"
        "  farewell:\n"
        "    description: closing line\n"
        "greeting: ${abstract:greeting}\nfarewell: ${abstract:farewell}\n"
    )
    child = tmp_path / "child.yaml"
    child.write_text("ancestors:\n  - ./base.yaml\nbody: x\n")
    cap = _Capture()
    code = run(["--output", "json", "--cache-dir", str(tmp_path / "cache"),
                "tree", str(child)],
               stdout=cap.out, stderr=cap.err)
    assert code == EXIT_OK, cap.out.getvalue() + cap.err.getvalue()
    env = json.loads(cap.out.getvalue())
    by_id = {n["id"]: n for n in env["result"]["nodes"]}
    base_real = str(base.resolve())
    base_paths = sorted(e["path"] for e in by_id[base_real]["abstracts"])
    assert base_paths == ["farewell", "greeting"]
    child_real = str(child.resolve())
    assert by_id[child_real]["abstracts"] == []


def test_describe_lists_declared_and_inherited_abstracts(tmp_path, npmrc):
    # Package with a base prompt that declares abstracts and a child prompt
    # that fills some but not all.
    manifest = {
        "name": "@a/b",
        "version": "1.0.0",
        "prompts": [
            {"id": "base", "path": "prompts/base.yaml"},
            {"id": "child", "path": "prompts/child.yaml"},
        ],
    }
    files = {
        "prompts/base.yaml": (
            b"abstracts:\n"
            b"  greeting:\n"
            b"    description: opening line\n"
            b"  farewell:\n"
            b"    description: closing line\n"
            b"greeting: ${abstract:greeting}\n"
            b"farewell: ${abstract:farewell}\n"
        ),
        # child declares its OWN abstract on `nickname` and fills `greeting`
        # but inherits `farewell` without filling.
        "prompts/child.yaml": (
            b"ancestors:\n  - ./base.yaml\n"
            b"abstracts:\n"
            b"  nickname:\n"
            b"    description: addressee's nickname\n"
            b"greeting: hi\n"
            b"nickname: ${abstract:nickname}\n"
        ),
    }
    tarballs = {("@a/b", "1.0.0"): _pack(manifest, files)}
    server = _RegistryServer(tarballs)
    server.start()
    try:
        rc = npmrc({"@a:registry": server.url})
        cap = _Capture()
        code = run([
            "--output", "json",
            "--npmrc", str(rc),
            "--cache-dir", str(tmp_path / "cache"),
            "describe", "@a/b@1.0.0#child",
        ], stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK, cap.out.getvalue() + cap.err.getvalue()
        env = json.loads(cap.out.getvalue())
        prompts = env["result"]
        assert len(prompts) == 1
        abstr = prompts[0]["abstracts"]
        assert sorted(r["path"] for r in abstr["declared"]) == ["nickname"]
        assert sorted(r["path"] for r in abstr["inherited"]) == ["farewell"]
    finally:
        server.stop()


def test_describe_yaml_emits_abstracts_comment_header(tmp_path, npmrc):
    manifest = {
        "name": "@a/b",
        "version": "1.0.0",
        "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
    }
    files = {
        "prompts/base.yaml": (
            b"abstracts:\n"
            b"  greeting:\n"
            b"    description: opening line\n"
            b"greeting: ${abstract:greeting}\nbody: x\n"
        ),
    }
    tarballs = {("@a/b", "1.0.0"): _pack(manifest, files)}
    server = _RegistryServer(tarballs)
    server.start()
    try:
        rc = npmrc({"@a:registry": server.url})
        cap = _Capture()
        code = run([
            "--npmrc", str(rc),
            "--cache-dir", str(tmp_path / "cache"),
            "describe", "@a/b@1.0.0#base",
        ], stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK, cap.out.getvalue() + cap.err.getvalue()
        out = cap.out.getvalue()
        assert "abstracts.declared: greeting" in out
    finally:
        server.stop()


def test_describe_permissive_on_textual_only_abstract_usage(tmp_path, npmrc):
    """Regression: a prompt whose abstracts appear only inside a block scalar
    (no `x: ${abstract:x}` self-declaration) must still be describe-able —
    describe MUST defer interpolation and surface the holes as declared."""
    manifest = {
        "name": "@a/b",
        "version": "1.0.0",
        "prompts": [{"id": "tpl", "path": "prompts/tpl.yaml"}],
    }
    files = {
        # No structural `x: ${abstract:x}` anywhere — every marker is a
        # textual usage inside a block scalar.
        "prompts/tpl.yaml": (
            b"abstracts:\n"
            b"  name:\n"
            b"    description: addressee\n"
            b"  place:\n"
            b"    description: destination\n"
            b"body: |\n"
            b"  Hi ${abstract:name}, welcome to ${abstract:place}.\n"
        ),
    }
    tarballs = {("@a/b", "1.0.0"): _pack(manifest, files)}
    server = _RegistryServer(tarballs)
    server.start()
    try:
        rc = npmrc({"@a:registry": server.url})
        cap = _Capture()
        code = run([
            "--output", "json",
            "--npmrc", str(rc),
            "--cache-dir", str(tmp_path / "cache"),
            "describe", "@a/b@1.0.0#tpl",
        ], stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK, cap.out.getvalue() + cap.err.getvalue()
        env = json.loads(cap.out.getvalue())
        declared_paths = sorted(r["path"] for r in env["result"][0]["abstracts"]["declared"])
        assert declared_paths == ["name", "place"]
        # Content is the merged (pre-interpolation) namespace, so markers
        # are still visible where the holes are.
        assert "${abstract:name}" in env["result"][0]["content"]["body"]
    finally:
        server.stop()


def test_describe_reports_inherited_not_declared_when_child_is_bare(tmp_path, npmrc):
    """A child prompt that inherits from an abstract ancestor without filling
    the holes must surface them as `inherited`, not `declared` — and it must
    not fail."""
    manifest = {
        "name": "@a/b",
        "version": "1.0.0",
        "prompts": [
            {"id": "tpl", "path": "prompts/tpl.yaml"},
            {"id": "bare", "path": "prompts/bare.yaml"},
        ],
    }
    files = {
        "prompts/tpl.yaml": (
            b"abstracts:\n"
            b"  name:\n"
            b"    description: addressee\n"
            b"body: |\n"
            b"  Hi ${abstract:name}.\n"
        ),
        "prompts/bare.yaml": (
            b"ancestors:\n  - ./tpl.yaml\n"
        ),
    }
    tarballs = {("@a/b", "1.0.0"): _pack(manifest, files)}
    server = _RegistryServer(tarballs)
    server.start()
    try:
        rc = npmrc({"@a:registry": server.url})
        cap = _Capture()
        code = run([
            "--output", "json",
            "--npmrc", str(rc),
            "--cache-dir", str(tmp_path / "cache"),
            "describe", "@a/b@1.0.0#bare",
        ], stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK, cap.out.getvalue() + cap.err.getvalue()
        env = json.loads(cap.out.getvalue())
        abstr = env["result"][0]["abstracts"]
        assert [r["path"] for r in abstr["declared"]] == []
        assert [r["path"] for r in abstr["inherited"]] == ["name"]
    finally:
        server.stop()


def test_describe_clean_when_child_fills_textual_holes(tmp_path, npmrc):
    """When a descendant fills every hole the ancestor uses textually, describe
    MUST report no abstracts and interpolate normally."""
    manifest = {
        "name": "@a/b",
        "version": "1.0.0",
        "prompts": [
            {"id": "tpl", "path": "prompts/tpl.yaml"},
            {"id": "concrete", "path": "prompts/concrete.yaml"},
        ],
    }
    files = {
        "prompts/tpl.yaml": (
            b"abstracts:\n"
            b"  name:\n"
            b"    description: addressee\n"
            b"body: |\n"
            b"  Hi ${abstract:name}.\n"
        ),
        "prompts/concrete.yaml": (
            b"ancestors:\n  - ./tpl.yaml\n"
            b"name: Ada\n"
        ),
    }
    tarballs = {("@a/b", "1.0.0"): _pack(manifest, files)}
    server = _RegistryServer(tarballs)
    server.start()
    try:
        rc = npmrc({"@a:registry": server.url})
        cap = _Capture()
        code = run([
            "--output", "json",
            "--npmrc", str(rc),
            "--cache-dir", str(tmp_path / "cache"),
            "describe", "@a/b@1.0.0#concrete",
        ], stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK, cap.out.getvalue() + cap.err.getvalue()
        env = json.loads(cap.out.getvalue())
        item = env["result"][0]
        assert item["abstracts"] == {"declared": [], "inherited": []}
        # Fully interpolated — marker gone.
        assert "${abstract:" not in item["content"]["body"]
        assert "Hi Ada." in item["content"]["body"]
    finally:
        server.stop()


# --- Abstract annotations ----------------------------------------


def test_resolve_rejects_undocumented_abstract_with_exit_10(tmp_path):
    """A prompt that introduces a marker without an abstracts entry is
    malformed."""
    f = tmp_path / "x.yaml"
    f.write_text("body: ${abstract:foo}\n")
    cap = _Capture()
    code = run(["--output", "json", "--cache-dir", str(tmp_path / "cache"),
                "resolve", str(f)],
               stdout=cap.out, stderr=cap.err)
    assert code == EXIT_SCHEMA
    env = json.loads(cap.out.getvalue())
    assert env["error"]["details"]["reason"] == "undocumented_abstract"


def test_resolve_type_list_splat_when_filled(tmp_path):
    """A list-typed abstract resolved with a list value participates in
    list splat at structural position."""
    base = tmp_path / "base.yaml"
    base.write_text(
        "abstracts:\n"
        "  steps:\n"
        "    description: pipeline steps\n"
        "    type: list\n"
        "items:\n"
        "  - ${abstract:steps}\n"
    )
    child = tmp_path / "child.yaml"
    child.write_text(
        "ancestors:\n  - ./base.yaml\n"
        "steps:\n  - load\n  - run\n  - save\n"
    )
    cap = _Capture()
    code = run(["--output", "json", "--cache-dir", str(tmp_path / "cache"),
                "resolve", str(child)],
               stdout=cap.out, stderr=cap.err)
    assert code == EXIT_OK, cap.out.getvalue() + cap.err.getvalue()
    env = json.loads(cap.out.getvalue())
    assert env["result"]["content"]["items"] == ["load", "run", "save"]


def test_resolve_type_list_with_non_list_value_is_type_mismatch(tmp_path):
    """A list-typed abstract resolved with a string value MUST abort
    exit 16 reason='type_mismatch'."""
    base = tmp_path / "base.yaml"
    base.write_text(
        "abstracts:\n"
        "  steps:\n"
        "    description: pipeline steps\n"
        "    type: list\n"
        "value: ${abstract:steps}\n"
    )
    child = tmp_path / "child.yaml"
    child.write_text(
        "ancestors:\n  - ./base.yaml\n"
        "steps: just-a-string\n"
    )
    cap = _Capture()
    code = run(["--output", "json", "--cache-dir", str(tmp_path / "cache"),
                "resolve", str(child)],
               stdout=cap.out, stderr=cap.err)
    assert code == EXIT_ABSTRACT_UNFILLED
    env = json.loads(cap.out.getvalue())
    details = env["error"]["details"]
    assert details["reason"] == "type_mismatch"
    assert details["declared_type"] == "list"
    assert details["actual_type"] == "string"


def test_publish_rejects_schema_type_contradiction(tmp_path):
    """At publish, a $schema constraint that contradicts the annotation type
    fires gate 1 (exit 10 schema_type_mismatch)."""
    schema = tmp_path / "s.json"
    schema.write_text(json.dumps({
        "type": "object",
        "properties": {"name": {"type": "string"}},
    }))
    pkg = tmp_path / "pkg"
    pkg.mkdir()
    (pkg / "package.json").write_text(json.dumps({
        "name": "@a/b",
        "version": "1.0.0",
        "prompts": [{"id": "p", "path": "prompts/p.yaml"}],
    }))
    (pkg / "prompts").mkdir()
    (pkg / "prompts" / "p.yaml").write_text(
        f'$schema: "{schema.as_uri()}"\n'
        f'abstracts:\n  name:\n    description: filler\n    type: list\n'
        f'name: ${{abstract:name}}\n'
    )
    cap = _Capture()
    code = run([
        "--output", "json",
        "--cache-dir", str(tmp_path / "cache"),
        "publish", "--dry-run", str(pkg),
    ], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_SCHEMA, cap.out.getvalue()
    env = json.loads(cap.out.getvalue())
    errors = env["error"].get("details", {}).get("errors", [env["error"]])
    assert any(
        e.get("details", {}).get("reason") == "schema_type_mismatch"
        for e in errors
    )


def test_describe_payload_carries_annotation(tmp_path, npmrc):
    """Describe surfaces each declared/inherited abstract with its
    originating declarer's annotation object verbatim."""
    manifest = {
        "name": "@a/b",
        "version": "1.0.0",
        "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
    }
    files = {
        "prompts/base.yaml": (
            b"abstracts:\n"
            b"  greeting:\n"
            b"    description: opening line of the message\n"
            b"  steps:\n"
            b"    description: ordered subroutine names\n"
            b"    type: list\n"
            b"greeting: ${abstract:greeting}\n"
            b"steps: ${abstract:steps}\n"
        ),
    }
    tarballs = {("@a/b", "1.0.0"): _pack(manifest, files)}
    server = _RegistryServer(tarballs)
    server.start()
    try:
        rc = npmrc({"@a:registry": server.url})
        cap = _Capture()
        code = run([
            "--output", "json",
            "--npmrc", str(rc),
            "--cache-dir", str(tmp_path / "cache"),
            "describe", "@a/b@1.0.0#base",
        ], stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK, cap.out.getvalue() + cap.err.getvalue()
        env = json.loads(cap.out.getvalue())
        declared = env["result"][0]["abstracts"]["declared"]
        by_path = {entry["path"]: entry for entry in declared}
        assert by_path["greeting"]["annotation"] == {
            "description": "opening line of the message",
            "type": "string",
        }
        assert by_path["steps"]["annotation"] == {
            "description": "ordered subroutine names",
            "type": "list",
        }
    finally:
        server.stop()


def test_describe_yaml_emits_per_abstract_comment_lines(tmp_path, npmrc):
    """The default YAML output gains one `# abstract <path> (<type>): <desc>`
    comment line per surfaced abstract."""
    manifest = {
        "name": "@a/b",
        "version": "1.0.0",
        "prompts": [{"id": "base", "path": "prompts/base.yaml"}],
    }
    files = {
        "prompts/base.yaml": (
            b"abstracts:\n"
            b"  greeting:\n"
            b"    description: opening line\n"
            b"greeting: ${abstract:greeting}\n"
        ),
    }
    tarballs = {("@a/b", "1.0.0"): _pack(manifest, files)}
    server = _RegistryServer(tarballs)
    server.start()
    try:
        rc = npmrc({"@a:registry": server.url})
        cap = _Capture()
        code = run([
            "--npmrc", str(rc),
            "--cache-dir", str(tmp_path / "cache"),
            "describe", "@a/b@1.0.0#base",
        ], stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK, cap.out.getvalue() + cap.err.getvalue()
        out = cap.out.getvalue()
        assert "# abstract greeting (string): opening line" in out
    finally:
        server.stop()


def test_tree_text_label_includes_type(tmp_path):
    base = tmp_path / "base.yaml"
    base.write_text(
        "abstracts:\n"
        "  steps:\n"
        "    description: pipeline steps\n"
        "    type: list\n"
        "items:\n  - ${abstract:steps}\n"
    )
    child = tmp_path / "child.yaml"
    child.write_text("ancestors:\n  - ./base.yaml\n")
    cap = _Capture()
    code = run(["--cache-dir", str(tmp_path / "cache"), "tree", str(child)],
               stdout=cap.out, stderr=cap.err)
    assert code == EXIT_OK, cap.out.getvalue() + cap.err.getvalue()
    assert "[abstracts: steps: list]" in cap.out.getvalue()


def test_validate_payload_includes_annotation(tmp_path):
    f = tmp_path / "a.yaml"
    f.write_text(
        "abstracts:\n"
        "  who:\n"
        "    description: addressee of the greeting\n"
        "greeting: ${abstract:who}\n"
    )
    cap = _Capture()
    code = run(["--output", "json", "validate", str(f)],
               stdout=cap.out, stderr=cap.err)
    assert code == EXIT_OK
    payload = json.loads(cap.out.getvalue())["result"]
    assert payload["abstracts"][0]["annotation"] == {
        "description": "addressee of the greeting",
        "type": "string",
    }
