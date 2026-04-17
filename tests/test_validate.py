"""Tests for the ``validate`` subcommand."""
from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from stemmata.cli import run

EXIT_OK = 0
EXIT_SCHEMA = 10


class _Capture:
    def __init__(self):
        self.out = io.StringIO()
        self.err = io.StringIO()


def _write(p: Path, content: str) -> Path:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")
    return p


def _schema(tmp: Path, props=None, required=None, name="schema.json"):
    s: dict = {"type": "object"}
    if props:
        s["properties"] = props
    if required:
        s["required"] = required
    p = tmp / name
    p.write_text(json.dumps(s), encoding="utf-8")
    return p.as_uri()


# -- single YAML file -------------------------------------------------------

class TestSingleYaml:
    def test_valid(self, tmp_path):
        uri = _schema(tmp_path, props={"name": {"type": "string"}})
        _write(tmp_path / "a.yaml", f'$schema: "{uri}"\nname: hello\n')
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "a.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK
        assert json.loads(cap.out.getvalue())["result"]["files_checked"] == 1

    def test_violation(self, tmp_path):
        uri = _schema(tmp_path, props={"name": {"type": "string"}}, required=["name"])
        _write(tmp_path / "a.yaml", f'$schema: "{uri}"\nage: 42\n')
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "a.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_SCHEMA
        errs = json.loads(cap.out.getvalue())["error"]["details"]["errors"]
        assert any("name" in e["message"] for e in errs)

    def test_multiple_violations(self, tmp_path):
        uri = _schema(tmp_path, props={"a": {"type": "string"}, "b": {"type": "string"}},
                       required=["a", "b"])
        _write(tmp_path / "a.yaml", f'$schema: "{uri}"\n')
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "a.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_SCHEMA
        assert len(json.loads(cap.out.getvalue())["error"]["details"]["errors"]) >= 2

    def test_no_schema_skipped(self, tmp_path):
        _write(tmp_path / "a.yaml", "name: hello\n")
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "a.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK


# -- ancestors ---------------------------------------------------------------

class TestAncestors:
    def test_inherited_values_satisfy_schema(self, tmp_path):
        uri = _schema(tmp_path, props={"name": {"type": "string"}, "region": {"type": "string"}},
                       required=["name", "region"])
        _write(tmp_path / "base.yaml", "region: us-east-1\n")
        _write(tmp_path / "child.yaml",
               f'$schema: "{uri}"\nancestors:\n  - "./base.yaml"\nname: hello\n')
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "child.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK

    def test_inherited_value_type_mismatch(self, tmp_path):
        uri = _schema(tmp_path, props={"region": {"type": "integer"}})
        _write(tmp_path / "base.yaml", "region: us-east-1\n")
        _write(tmp_path / "child.yaml",
               f'$schema: "{uri}"\nancestors:\n  - "./base.yaml"\n')
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "child.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_SCHEMA


# -- multi-document YAML -----------------------------------------------------

class TestMultiDoc:
    def test_two_valid_docs(self, tmp_path):
        uri = _schema(tmp_path, props={"x": {"type": "integer"}})
        _write(tmp_path / "m.yaml",
               f'$schema: "{uri}"\nx: 1\n---\n$schema: "{uri}"\nx: 2\n')
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "m.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK
        assert json.loads(cap.out.getvalue())["result"]["documents_checked"] == 2

    def test_violation_in_second_doc(self, tmp_path):
        uri = _schema(tmp_path, props={"x": {"type": "integer"}}, required=["x"])
        _write(tmp_path / "m.yaml",
               f'$schema: "{uri}"\nx: 1\n---\n$schema: "{uri}"\ny: hello\n')
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "m.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_SCHEMA

    def test_different_schemas(self, tmp_path):
        s1 = _schema(tmp_path, props={"a": {"type": "string"}}, required=["a"], name="s1.json")
        s2 = _schema(tmp_path, props={"b": {"type": "integer"}}, required=["b"], name="s2.json")
        _write(tmp_path / "m.yaml",
               f'$schema: "{s1}"\na: hello\n---\n$schema: "{s2}"\nb: 42\n')
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "m.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK


# -- JSON files --------------------------------------------------------------

class TestJson:
    def test_valid(self, tmp_path):
        uri = _schema(tmp_path, props={"n": {"type": "integer"}})
        _write(tmp_path / "a.json", json.dumps({"$schema": uri, "n": 42}))
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "a.json")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK

    def test_violation(self, tmp_path):
        uri = _schema(tmp_path, props={"n": {"type": "integer"}}, required=["n"])
        _write(tmp_path / "a.json", json.dumps({"$schema": uri}))
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "a.json")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_SCHEMA

    def test_no_schema_skipped(self, tmp_path):
        _write(tmp_path / "a.json", '{"x": 1}')
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "a.json")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK

    def test_malformed(self, tmp_path):
        _write(tmp_path / "a.json", "{bad")
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "a.json")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_SCHEMA


# -- JSON prompts with ancestors ---------------------------------------------

class TestJsonAncestors:
    def test_inherited_values_satisfy_schema(self, tmp_path):
        uri = _schema(tmp_path, props={"name": {"type": "string"}, "region": {"type": "string"}},
                       required=["name", "region"])
        _write(tmp_path / "base.yaml", "region: us-east-1\n")
        _write(tmp_path / "child.json",
               json.dumps({"$schema": uri, "ancestors": ["./base.yaml"], "name": "hello"}))
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "child.json")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK

    def test_json_inherits_from_json(self, tmp_path):
        uri = _schema(tmp_path, props={"name": {"type": "string"}, "region": {"type": "string"}},
                       required=["name", "region"])
        _write(tmp_path / "base.json", json.dumps({"region": "us-east-1"}))
        _write(tmp_path / "child.json",
               json.dumps({"$schema": uri, "ancestors": ["./base.json"], "name": "hello"}))
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "child.json")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK

    def test_inherited_value_type_mismatch(self, tmp_path):
        uri = _schema(tmp_path, props={"region": {"type": "integer"}})
        _write(tmp_path / "base.json", json.dumps({"region": "us-east-1"}))
        _write(tmp_path / "child.json",
               json.dumps({"$schema": uri, "ancestors": ["./base.json"]}))
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "child.json")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_SCHEMA


# -- JSON line numbers --------------------------------------------------------

class TestJsonLineNumbers:
    def test_json_string_value_line(self, tmp_path):
        uri = _schema(tmp_path, props={"name": {"type": "integer"}})
        content = json.dumps({"$schema": uri, "ignored": True, "name": "hello"}, indent=2)
        _write(tmp_path / "a.json", content)
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "a.json")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_SCHEMA
        loc = json.loads(cap.out.getvalue())["error"]["details"]["errors"][0]["location"]
        assert loc["line"] is not None
        assert loc["line"] >= 1


# -- JSON resolve (via CLI) --------------------------------------------------

class TestJsonResolve:
    def test_resolve_json_prompt(self, tmp_path):
        _write(tmp_path / "a.json", json.dumps({"greeting": "hello"}))
        cap = _Capture()
        code = run(["--output", "json", "resolve", str(tmp_path / "a.json")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK
        result = json.loads(cap.out.getvalue())
        assert result["result"]["content"]["greeting"] == "hello"

    def test_resolve_json_with_yaml_ancestor(self, tmp_path):
        _write(tmp_path / "base.yaml", "region: us-east-1\n")
        _write(tmp_path / "child.json",
               json.dumps({"ancestors": ["./base.yaml"], "name": "hello"}))
        cap = _Capture()
        code = run(["--output", "json", "resolve", str(tmp_path / "child.json")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK
        content = json.loads(cap.out.getvalue())["result"]["content"]
        assert content["name"] == "hello"
        assert content["region"] == "us-east-1"


# -- directory scanning ------------------------------------------------------

class TestDirectory:
    def test_discovers_all(self, tmp_path):
        uri = _schema(tmp_path, props={"x": {"type": "integer"}})
        d = tmp_path / "prompts"
        _write(d / "a.yaml", f'$schema: "{uri}"\nx: 1\n')
        _write(d / "b.yml", f'$schema: "{uri}"\nx: 2\n')
        _write(d / "c.json", json.dumps({"$schema": uri, "x": 3}))
        cap = _Capture()
        code = run(["--output", "json", "validate", str(d)],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK
        assert json.loads(cap.out.getvalue())["result"]["files_checked"] == 3

    def test_recursive(self, tmp_path):
        uri = _schema(tmp_path, props={"x": {"type": "integer"}})
        _write(tmp_path / "d" / "a.yaml", f'$schema: "{uri}"\nx: 1\n')
        _write(tmp_path / "d" / "sub" / "b.yaml", f'$schema: "{uri}"\nx: 2\n')
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "d")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK
        assert json.loads(cap.out.getvalue())["result"]["files_checked"] == 2

    def test_empty_dir(self, tmp_path):
        d = tmp_path / "empty"
        d.mkdir()
        cap = _Capture()
        code = run(["--output", "json", "validate", str(d)],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK

    def test_violations_across_files(self, tmp_path):
        uri = _schema(tmp_path, props={"x": {"type": "integer"}}, required=["x"])
        d = tmp_path / "d"
        _write(d / "ok.yaml", f'$schema: "{uri}"\nx: 1\n')
        _write(d / "bad.yaml", f'$schema: "{uri}"\ny: hello\n')
        cap = _Capture()
        code = run(["--output", "json", "validate", str(d)],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_SCHEMA


# -- line numbers ------------------------------------------------------------

class TestLineNumbers:
    def test_string_value(self, tmp_path):
        uri = _schema(tmp_path, props={"name": {"type": "integer"}})
        _write(tmp_path / "a.yaml",
               f'$schema: "{uri}"\nignored: true\nname: hello\n')
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "a.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_SCHEMA
        loc = json.loads(cap.out.getvalue())["error"]["details"]["errors"][0]["location"]
        assert loc["line"] == 3

    def test_non_string_yaml_value_uses_key_line(self, tmp_path):
        uri = _schema(tmp_path, props={"count": {"type": "string"}})
        _write(tmp_path / "a.yaml",
               f'$schema: "{uri}"\nother: x\ncount: 42\n')
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "a.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_SCHEMA
        assert json.loads(cap.out.getvalue())["error"]["details"]["errors"][0]["location"]["line"] == 3

    def test_json_line_number(self, tmp_path):
        s = {"type": "object", "properties": {"enabled": {"type": "boolean"}}}
        (tmp_path / "s.json").write_text(json.dumps(s))
        _write(tmp_path / "d.json", json.dumps({"$schema": "s.json", "enabled": "yes"}, indent=2))
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "d.json")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_SCHEMA
        assert json.loads(cap.out.getvalue())["error"]["details"]["errors"][0]["location"]["line"] is not None

    def test_multi_doc_non_string(self, tmp_path):
        s = {"type": "object", "properties": {"n": {"type": "string"}}}
        (tmp_path / "s.json").write_text(json.dumps(s))
        _write(tmp_path / "m.yaml", '$schema: "s.json"\nn: ok\n---\n$schema: "s.json"\nn: 99\n')
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "m.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_SCHEMA
        assert json.loads(cap.out.getvalue())["error"]["details"]["errors"][0]["location"]["line"] == 5


# -- relative $schema paths -------------------------------------------------

class TestRelativePaths:
    def test_relative_yaml(self, tmp_path):
        s = {"type": "object", "properties": {"x": {"type": "integer"}}}
        (tmp_path / "schemas").mkdir()
        (tmp_path / "schemas" / "s.json").write_text(json.dumps(s))
        _write(tmp_path / "prompts" / "a.yaml", '$schema: "../schemas/s.json"\nx: 1\n')
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "prompts" / "a.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK

    def test_relative_json(self, tmp_path):
        s = {"type": "object", "properties": {"n": {"type": "integer"}}}
        (tmp_path / "schemas").mkdir()
        (tmp_path / "schemas" / "s.json").write_text(json.dumps(s))
        _write(tmp_path / "data" / "d.json",
               json.dumps({"$schema": "../schemas/s.json", "n": 42}))
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "data" / "d.json")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK

    def test_relative_violation(self, tmp_path):
        s = {"type": "object", "required": ["x"], "properties": {"x": {"type": "integer"}}}
        (tmp_path / "schemas").mkdir()
        (tmp_path / "schemas" / "s.json").write_text(json.dumps(s))
        _write(tmp_path / "a.yaml", '$schema: "schemas/s.json"\nx: "nope"\n')
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "a.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_SCHEMA


# -- CLI errors --------------------------------------------------------------

class TestCliErrors:
    def test_missing_target(self):
        cap = _Capture()
        code = run(["validate"], stdout=cap.out, stderr=cap.err)
        assert code != EXIT_OK

    def test_nonexistent_target(self, tmp_path):
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "nope.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code != EXIT_OK


# -- default output mode -----------------------------------------------------

class TestDefaultOutput:
    def test_default_is_yaml(self, tmp_path):
        uri = _schema(tmp_path, props={"x": {"type": "integer"}})
        _write(tmp_path / "a.yaml", f'$schema: "{uri}"\nx: 1\n')
        cap = _Capture()
        code = run(["validate", str(tmp_path / "a.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK
        out = cap.out.getvalue()
        assert not out.strip().startswith("{")
        assert "files_checked" in out
