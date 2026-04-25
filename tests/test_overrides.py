from __future__ import annotations

import io
import json
from pathlib import Path

from stemmata.cli import run
from stemmata.errors import EXIT_MERGE, EXIT_OK, EXIT_UNRESOLVABLE, EXIT_USAGE


class _Capture:
    def __init__(self):
        self.out = io.StringIO()
        self.err = io.StringIO()


def _write(p: Path, content: str) -> Path:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")
    return p


def _resolve_json(target: Path, *extra: str) -> tuple[int, _Capture]:
    cap = _Capture()
    code = run(
        ["--output", "json", "resolve", str(target), *extra],
        stdout=cap.out,
        stderr=cap.err,
    )
    return code, cap


def _content(cap: _Capture):
    return json.loads(cap.out.getvalue())["result"]["content"]


def _err_details(cap: _Capture):
    return json.loads(cap.out.getvalue())["error"]["details"]


def test_set_scalar_override_wins_over_root(tmp_path):
    f = _write(tmp_path / "a.yaml", "region: us-east-1\n")
    code, cap = _resolve_json(f, "--set", "region=eu-west-1")
    assert code == EXIT_OK, cap.out.getvalue()
    assert _content(cap)["region"] == "eu-west-1"


def test_set_typed_scalar_parses_as_yaml(tmp_path):
    f = _write(tmp_path / "a.yaml", "port: 1\nenabled: false\npi: 0\n")
    code, cap = _resolve_json(
        f,
        "--set", "port=5432",
        "--set", "enabled=true",
        "--set", "pi=3.14",
    )
    assert code == EXIT_OK
    c = _content(cap)
    assert c["port"] == 5432 and isinstance(c["port"], int)
    assert c["enabled"] is True
    assert c["pi"] == 3.14


def test_set_nested_path_builds_map(tmp_path):
    f = _write(tmp_path / "a.yaml", "database:\n  host: old\n  port: 1\n")
    code, cap = _resolve_json(
        f, "--set", "database.host=new", "--set", "database.port=5432"
    )
    assert code == EXIT_OK
    assert _content(cap)["database"] == {"host": "new", "port": 5432}


def test_set_list_value(tmp_path):
    f = _write(tmp_path / "a.yaml", "tags: [x]\n")
    code, cap = _resolve_json(f, "--set", "tags=[a, b, c]")
    assert code == EXIT_OK
    assert _content(cap)["tags"] == ["a", "b", "c"]


def test_set_mapping_value(tmp_path):
    f = _write(tmp_path / "a.yaml", "cfg:\n  k: 0\n")
    code, cap = _resolve_json(f, "--set", "cfg={k: v, n: 1}")
    assert code == EXIT_OK
    assert _content(cap)["cfg"] == {"k": "v", "n": 1}


def test_set_null_via_empty_value(tmp_path):
    f = _write(tmp_path / "a.yaml", "body: hi\n")
    code, cap = _resolve_json(f, "--set", "body=")
    assert code == EXIT_OK
    assert _content(cap)["body"] is None


def test_set_last_wins_on_duplicate_paths(tmp_path):
    f = _write(tmp_path / "a.yaml", "x: original\n")
    code, cap = _resolve_json(f, "--set", "x=first", "--set", "x=second")
    assert code == EXIT_OK
    assert _content(cap)["x"] == "second"


def test_set_wins_over_ancestor(tmp_path):
    _write(tmp_path / "base.yaml", "region: base\n")
    child = _write(tmp_path / "child.yaml", 'ancestors:\n  - "./base.yaml"\n')
    code, cap = _resolve_json(child, "--set", "region=from-set")
    assert code == EXIT_OK
    assert _content(cap)["region"] == "from-set"


def test_set_satisfies_abstract(tmp_path):
    f = _write(
        tmp_path / "a.yaml",
        'abstracts:\n  who:\n    description: addressee\n'
        'greeting: "Hi ${abstract:who}."\n',
    )
    code, cap = _resolve_json(f, "--set", "who=Ada")
    assert code == EXIT_OK
    assert _content(cap)["greeting"] == "Hi Ada."


def test_set_can_fill_nested_abstract(tmp_path):
    f = _write(
        tmp_path / "a.yaml",
        'abstracts:\n  persona.name:\n    description: addressee\n'
        'msg: "I am ${abstract:persona.name}"\n',
    )
    code, cap = _resolve_json(f, "--set", "persona.name=Ada")
    assert code == EXIT_OK
    assert _content(cap)["msg"] == "I am Ada"


def test_set_appears_in_ancestors_payload_with_distance_minus_one(tmp_path):
    f = _write(tmp_path / "a.yaml", "x: 1\n")
    code, cap = _resolve_json(f, "--set", "x=2")
    assert code == EXIT_OK
    ancestors = json.loads(cap.out.getvalue())["result"]["ancestors"]
    assert ancestors[0] == {"canonical_id": "<overrides>", "distance": -1}


def test_no_set_means_no_override_entry_in_ancestors(tmp_path):
    f = _write(tmp_path / "a.yaml", "x: 1\n")
    code, cap = _resolve_json(f)
    assert code == EXIT_OK
    ancestors = json.loads(cap.out.getvalue())["result"]["ancestors"]
    assert all(a["canonical_id"] != "<overrides>" for a in ancestors)


def test_set_missing_equals_is_usage_error(tmp_path):
    f = _write(tmp_path / "a.yaml", "x: 1\n")
    code, cap = _resolve_json(f, "--set", "no_equals")
    assert code == EXIT_USAGE
    assert _err_details(cap)["reason"] == "missing_equals"


def test_set_invalid_path_is_usage_error(tmp_path):
    f = _write(tmp_path / "a.yaml", "x: 1\n")
    code, cap = _resolve_json(f, "--set", "1bad.path=x")
    assert code == EXIT_USAGE
    assert _err_details(cap)["reason"] == "invalid_path"


def test_set_empty_segment_is_usage_error(tmp_path):
    f = _write(tmp_path / "a.yaml", "x: 1\n")
    code, cap = _resolve_json(f, "--set", "a..b=1")
    assert code == EXIT_USAGE
    assert _err_details(cap)["reason"] == "invalid_path"


def test_set_reserved_key_is_usage_error(tmp_path):
    f = _write(tmp_path / "a.yaml", "x: 1\n")
    code, cap = _resolve_json(f, "--set", "ancestors=[]")
    assert code == EXIT_USAGE
    assert _err_details(cap)["reason"] == "reserved_key"


def test_set_intra_override_conflict_is_usage_error(tmp_path):
    f = _write(tmp_path / "a.yaml", "x: 1\n")
    code, cap = _resolve_json(f, "--set", "a=scalar", "--set", "a.b=1")
    assert code == EXIT_USAGE
    assert _err_details(cap)["reason"] == "intra_override_conflict"


def test_set_type_conflict_with_root_is_merge_error(tmp_path):
    f = _write(tmp_path / "a.yaml", "foo:\n  bar: 1\n")
    code, cap = _resolve_json(f, "--set", "foo=scalar")
    assert code == EXIT_MERGE
    details = _err_details(cap)
    assert details["path"] == "foo"
    assert details["conflict"] == "type_mismatch"


def test_set_null_shadow_surfaces_explicit_null(tmp_path):
    f = _write(tmp_path / "a.yaml", 'x: hello\nbody: "${x}"\n')
    code, cap = _resolve_json(f, "--set", "x=")
    assert code == EXIT_UNRESOLVABLE
    details = _err_details(cap)
    assert details["reason"] == "explicit_null"
    assert details["providing_ancestor"] == "<overrides>"


def test_set_value_is_interpolated_against_namespace(tmp_path):
    f = _write(tmp_path / "a.yaml", 'name: World\nbody: ""\n')
    code, cap = _resolve_json(f, "--set", "body=Hi ${name}")
    assert code == EXIT_OK
    assert _content(cap)["body"] == "Hi World"


def test_set_rejected_on_validate_subcommand(tmp_path):
    f = _write(tmp_path / "a.yaml", "x: 1\n")
    cap = _Capture()
    code = run(["validate", str(f), "--set", "x=2"], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_USAGE


def test_set_rejected_on_describe_subcommand(tmp_path):
    cap = _Capture()
    code = run(
        ["describe", "@x/y@1.0.0#z", "--set", "x=2"],
        stdout=cap.out,
        stderr=cap.err,
    )
    assert code == EXIT_USAGE


def test_set_rejected_on_tree_subcommand(tmp_path):
    f = _write(tmp_path / "a.yaml", "x: 1\n")
    cap = _Capture()
    code = run(["tree", str(f), "--set", "x=2"], stdout=cap.out, stderr=cap.err)
    assert code == EXIT_USAGE


def test_set_default_yaml_output_has_overridden_value(tmp_path):
    f = _write(tmp_path / "a.yaml", "region: old\n")
    cap = _Capture()
    code = run(
        ["resolve", str(f), "--set", "region=new"],
        stdout=cap.out,
        stderr=cap.err,
    )
    assert code == EXIT_OK
    assert "region: new" in cap.out.getvalue()


def test_set_multiple_overrides_compose_deterministically(tmp_path):
    _write(tmp_path / "base.yaml", "a:\n  b: base-b\n  c: base-c\nname: base\n")
    child = _write(
        tmp_path / "child.yaml",
        'ancestors:\n  - "./base.yaml"\nname: child\n',
    )
    code, cap = _resolve_json(
        child,
        "--set", "a.b=override-b",
        "--set", "name=override-name",
    )
    assert code == EXIT_OK
    c = _content(cap)
    assert c["a"] == {"b": "override-b", "c": "base-c"}
    assert c["name"] == "override-name"
