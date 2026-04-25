"""Tests for the ``validate`` subcommand."""
from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from stemmata.cli import run

EXIT_OK = 0
EXIT_SCHEMA = 10
EXIT_REFERENCE = 11


class _Capture:
    def __init__(self):
        self.out = io.StringIO()
        self.err = io.StringIO()


def _write(p: Path, content: str) -> Path:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")
    return p


def _collect_reasons(err: dict) -> set[str]:
    out: set[str] = set()
    details = err.get("details") or {}
    if isinstance(details, dict):
        if isinstance(details.get("reason"), str):
            out.add(details["reason"])
        for sub in details.get("errors", []) or []:
            out |= _collect_reasons(sub)
    return out


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


# -- unresolvable $schema ---------------------------------------------------

class TestUnresolvableSchema:
    def test_missing_relative_schema_reports_prompt_location(self, tmp_path):
        prompt = tmp_path / "a.yaml"
        _write(prompt, '$schema: "./does-not-exist.json"\nx: 1\n')
        cap = _Capture()
        code = run(["--output", "json", "validate", str(prompt)],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_SCHEMA
        errs = json.loads(cap.out.getvalue())["error"]["details"]["errors"]
        assert len(errs) == 1
        assert errs[0]["location"]["file"] == str(prompt)
        assert errs[0]["details"]["reason"] == "schema_file_not_found"
        assert errs[0]["details"]["field"] == "$schema"
        assert "does-not-exist.json" in errs[0]["message"]

    def test_missing_file_uri_schema_reports_prompt_location(self, tmp_path):
        missing = (tmp_path / "nowhere.json").as_uri()
        prompt = tmp_path / "a.yaml"
        _write(prompt, f'$schema: "{missing}"\nx: 1\n')
        cap = _Capture()
        code = run(["--output", "json", "validate", str(prompt)],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_SCHEMA
        errs = json.loads(cap.out.getvalue())["error"]["details"]["errors"]
        assert errs[0]["location"]["file"] == str(prompt)
        assert errs[0]["details"]["reason"] == "schema_file_not_found"


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


# -- abstract placeholders --------------------------------------------------

class TestAbstractPlaceholders:
    def test_validate_permissive_on_abstracts(self, tmp_path):
        uri = _schema(tmp_path, props={"name": {"type": "string", "pattern": "^concrete "}},
                      required=["name"])
        # The merged value "concrete ${abstract:x}" would fail the pattern after
        # naive stringification, but because abstracts are unfilled the schema
        # check must be deferred entirely — validate MUST succeed with exit 0.
        _write(
            tmp_path / "a.yaml",
            f'$schema: "{uri}"\n'
            f'abstracts:\n  x:\n    description: filler\n'
            f'name: "concrete ${{abstract:x}}"\n',
        )
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "a.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK
        payload = json.loads(cap.out.getvalue())["result"]
        assert payload["abstracts_found"] == 1
        assert payload["abstracts"][0]["path"] == "x"

    def test_validate_runs_schema_when_concrete(self, tmp_path):
        uri = _schema(tmp_path, props={"name": {"type": "string", "pattern": "^concrete "}},
                      required=["name"])
        _write(tmp_path / "a.yaml", f'$schema: "{uri}"\nname: "not-concrete value"\n')
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "a.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_SCHEMA

    def test_validate_still_reports_real_placeholder_errors(self, tmp_path):
        uri = _schema(tmp_path, props={"x": {"type": "string"}})
        _write(
            tmp_path / "a.yaml",
            f'$schema: "{uri}"\n'
            f'abstracts:\n  a:\n    description: filler\n'
            f'x: "${{abstract:a}} and ${{missing}}"\n',
        )
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "a.yaml")],
                    stdout=cap.out, stderr=cap.err)
        # Real placeholder error beats schema-deferred abstracts.
        assert code == 14  # EXIT_UNRESOLVABLE

    def test_validate_surfaces_abstracts_count_in_payload(self, tmp_path):
        uri = _schema(tmp_path, props={"x": {"type": "string"}})
        _write(
            tmp_path / "a.yaml",
            f'$schema: "{uri}"\n'
            f'abstracts:\n'
            f'  one:\n    description: first\n'
            f'  two:\n    description: second\n'
            f'body: |\n  ${{abstract:one}}\n  ${{abstract:two}}\n',
        )
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "a.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK
        payload = json.loads(cap.out.getvalue())["result"]
        paths = sorted(a["path"] for a in payload["abstracts"])
        assert paths == ["one", "two"]

    def test_validate_reports_abstracts_without_schema(self, tmp_path):
        _write(
            tmp_path / "a.yaml",
            'abstracts:\n'
            '  alpha:\n    description: first\n'
            '  beta:\n    description: second\n'
            'body: |\n  ${abstract:alpha}\n  ${abstract:beta}\n',
        )
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "a.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK
        payload = json.loads(cap.out.getvalue())["result"]
        assert payload["abstracts_found"] == 2
        assert sorted(a["path"] for a in payload["abstracts"]) == ["alpha", "beta"]

    def test_validate_reports_inherited_abstracts_without_schema(self, tmp_path):
        _write(
            tmp_path / "base.yaml",
            'abstracts:\n  who:\n    description: addressee\n'
            'greeting: "Hi ${abstract:who}."\n',
        )
        _write(tmp_path / "child.yaml", 'ancestors:\n  - "./base.yaml"\n')
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "child.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK
        payload = json.loads(cap.out.getvalue())["result"]
        assert payload["abstracts_found"] == 1
        assert payload["abstracts"][0]["path"] == "who"

    def test_validate_json_reports_abstracts_without_schema(self, tmp_path):
        _write(
            tmp_path / "a.json",
            json.dumps({
                "abstracts": {"foo": {"description": "filler"}},
                "body": "${abstract:foo}",
            }),
        )
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "a.json")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK
        payload = json.loads(cap.out.getvalue())["result"]
        assert payload["abstracts_found"] == 1
        assert payload["abstracts"][0]["path"] == "foo"

    def test_validate_multi_doc_abstracts_mixed_schema(self, tmp_path):
        uri = _schema(tmp_path, props={"x": {"type": "integer"}})
        _write(
            tmp_path / "m.yaml",
            f'$schema: "{uri}"\nx: 1\n---\n'
            f'abstracts:\n  here:\n    description: filler\n'
            f'body: "${{abstract:here}}"\n',
        )
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "m.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK
        payload = json.loads(cap.out.getvalue())["result"]
        assert payload["abstracts_found"] == 1
        entry = payload["abstracts"][0]
        assert entry["path"] == "here"
        assert entry["document"] == 2

    def test_validate_surfaces_resolver_errors_without_schema(self, tmp_path):
        _write(tmp_path / "a.yaml", 'ancestors:\n  - "./missing.yaml"\n')
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "a.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_REFERENCE
        env = json.loads(cap.out.getvalue())
        assert env["error"]["category"] == "reference_error"

    def test_validate_rejects_annotation_without_marker(self, tmp_path):
        _write(
            tmp_path / "a.yaml",
            'abstracts:\n  shared:\n    description: x\n'
            'body: hello\n',
        )
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "a.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_SCHEMA
        env = json.loads(cap.out.getvalue())
        reasons = _collect_reasons(env["error"])
        assert "annotation_without_declaration" in reasons

    def test_validate_rejects_re_annotation_of_inherited_abstract(self, tmp_path):
        _write(
            tmp_path / "base.yaml",
            'abstracts:\n  shared:\n    description: introduced here\n'
            'value: "${abstract:shared}"\n',
        )
        _write(
            tmp_path / "child.yaml",
            'ancestors:\n  - "./base.yaml"\n'
            'abstracts:\n  shared:\n    description: re-annotated\n',
        )
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "child.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_SCHEMA
        env = json.loads(cap.out.getvalue())
        reasons = _collect_reasons(env["error"])
        assert "abstract_reannotation" in reasons
        assert "annotation_without_declaration" not in reasons

    def test_validate_orphan_annotation_with_ancestors_uses_graph_check(self, tmp_path):
        _write(tmp_path / "base.yaml", 'value: 1\n')
        _write(
            tmp_path / "child.yaml",
            'ancestors:\n  - "./base.yaml"\n'
            'abstracts:\n  ghost:\n    description: nobody declares this\n'
            'body: hello\n',
        )
        cap = _Capture()
        code = run(["--output", "json",
                    "--cache-dir", str(tmp_path / "cache"),
                    "validate", str(tmp_path / "child.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_SCHEMA
        env = json.loads(cap.out.getvalue())
        reasons = _collect_reasons(env["error"])
        assert "annotation_without_declaration" in reasons
        assert "abstract_reannotation" not in reasons

    def test_validate_re_annotation_of_inherited_annotation_only(self, tmp_path):
        _write(
            tmp_path / "base.yaml",
            'abstracts:\n  shared:\n    description: introduced here\n'
            'value: "${abstract:shared}"\n',
        )
        _write(
            tmp_path / "child.yaml",
            'ancestors:\n  - "./base.yaml"\n'
            'abstracts:\n  shared:\n    description: re-annotated\n'
            'shared: "filled"\n',
        )
        cap = _Capture()
        code = run(["--output", "json",
                    "--cache-dir", str(tmp_path / "cache"),
                    "validate", str(tmp_path / "child.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_SCHEMA
        env = json.loads(cap.out.getvalue())
        reasons = _collect_reasons(env["error"])
        assert "abstract_reannotation" in reasons
        assert "annotation_without_declaration" not in reasons

    def test_validate_standalone_orphan_annotation_still_parse_time(self, tmp_path):
        _write(
            tmp_path / "p.yaml",
            'abstracts:\n  ghost:\n    description: orphan\n'
            'body: hello\n',
        )
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "p.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_SCHEMA
        env = json.loads(cap.out.getvalue())
        reasons = _collect_reasons(env["error"])
        assert "annotation_without_declaration" in reasons

    def test_validate_abstracts_file_is_absolute_for_relative_cli_path(self, tmp_path, monkeypatch):
        _write(
            tmp_path / "p.yaml",
            'abstracts:\n  greet:\n    description: opening line\n'
            'body: "${abstract:greet}"\n',
        )
        monkeypatch.chdir(tmp_path)
        cap = _Capture()
        code = run(["--output", "json", "validate", "p.yaml"],
                   stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK, cap.err.getvalue()
        payload = json.loads(cap.out.getvalue())["result"]
        assert payload["abstracts_found"] == 1
        emitted = payload["abstracts"][0]["file"]
        assert Path(emitted).is_absolute()
        assert Path(emitted).resolve() == (tmp_path / "p.yaml").resolve()

    def test_validate_abstracts_file_is_stable_across_cwds(self, tmp_path, monkeypatch):
        _write(
            tmp_path / "p.yaml",
            'abstracts:\n  greet:\n    description: opening line\n'
            'body: "${abstract:greet}"\n',
        )
        cap1 = _Capture()
        run(["--output", "json", "validate", str(tmp_path / "p.yaml")],
            stdout=cap1.out, stderr=cap1.err)
        out_abs = json.loads(cap1.out.getvalue())["result"]["abstracts"][0]["file"]

        monkeypatch.chdir(tmp_path)
        cap2 = _Capture()
        run(["--output", "json", "validate", "p.yaml"],
            stdout=cap2.out, stderr=cap2.err)
        out_rel = json.loads(cap2.out.getvalue())["result"]["abstracts"][0]["file"]

        cap3 = _Capture()
        run(["--output", "json", "validate", "./p.yaml"],
            stdout=cap3.out, stderr=cap3.err)
        out_dot = json.loads(cap3.out.getvalue())["result"]["abstracts"][0]["file"]

        assert out_abs == out_rel == out_dot

    def test_validate_directory_emits_absolute_paths_for_abstracts(self, tmp_path, monkeypatch):
        sub = tmp_path / "pkg"
        _write(
            sub / "p.yaml",
            'abstracts:\n  greet:\n    description: opening line\n'
            'body: "${abstract:greet}"\n',
        )
        monkeypatch.chdir(tmp_path)
        cap = _Capture()
        code = run(["--output", "json", "validate", "pkg"],
                   stdout=cap.out, stderr=cap.err)
        assert code == EXIT_OK, cap.err.getvalue()
        payload = json.loads(cap.out.getvalue())["result"]
        emitted = payload["abstracts"][0]["file"]
        assert Path(emitted).is_absolute()
        assert Path(emitted).resolve() == (sub / "p.yaml").resolve()

    def test_validate_rejects_intra_doc_type_conflict_without_schema(self, tmp_path):
        _write(tmp_path / "a.yaml", 'a: 1\na.b: 2\n')
        cap = _Capture()
        code = run(["--output", "json", "validate", str(tmp_path / "a.yaml")],
                    stdout=cap.out, stderr=cap.err)
        assert code == EXIT_SCHEMA
        env = json.loads(cap.out.getvalue())
        reasons = _collect_reasons(env["error"])
        assert "intra_doc_type_conflict" in reasons
