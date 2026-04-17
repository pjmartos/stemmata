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
