import pytest

from stemmata.errors import AbstractUnfilledError, CycleError, MergeError, UnresolvableError
from stemmata.interp import Layer, interpolate
from stemmata.yaml_loader import attach_file, load_with_positions


def _load(text, file="x.yaml"):
    data, _ = load_with_positions(text, file=file)
    attach_file(data, file)
    return data


def _interp(text, layers_data, file="x.yaml"):
    data = _load(text, file=file)
    merged = data
    for layer in layers_data:
        from stemmata.merge import merge_pair
        merged = merge_pair(merged, layer)
    layers = [Layer(canonical_id=f"layer{i}", data=l) for i, l in enumerate([data] + layers_data)]
    return interpolate(merged, layers, root_file=file)


def test_simple_textual():
    result = _interp("body: hello ${name}\n", [{"name": "world"}])
    assert result["body"] == "hello world"


def test_exact_structural_scalar():
    result = _interp("x: ${val}\n", [{"val": 42}])
    assert result["x"] == 42


def test_exact_structural_map():
    result = _interp("x: ${cfg}\n", [{"cfg": {"a": 1}}])
    assert result["x"] == {"a": 1}


def test_list_splat_structural():
    result = _interp("xs:\n  - ${items}\n  - tail\n", [{"items": [1, 2, 3]}])
    assert result["xs"] == [1, 2, 3, "tail"]


def test_non_splat_form():
    result = _interp("xs:\n  - ${=items}\n", [{"items": [1, 2, 3]}])
    assert result["xs"] == [[1, 2, 3]]


def test_empty_list_splat_vanishes():
    result = _interp("xs:\n  - ${items}\n  - tail\n", [{"items": []}])
    assert result["xs"] == ["tail"]


def test_block_scalar_is_textual():
    result = _interp("body: |\n  ${val}\n", [{"val": "abc"}])
    assert result["body"] == "abc\n"


def test_dollar_escape():
    result = _interp('body: "$${literal}"\n', [])
    assert result["body"] == "${literal}"


def test_not_provided_raises():
    with pytest.raises(UnresolvableError) as exc:
        _interp("x: ${missing.path}\n", [])
    assert exc.value.details["reason"] == "not_provided"


def test_explicit_null_raises_with_provider():
    with pytest.raises(UnresolvableError) as exc:
        _interp("x: ${val}\n", [{"val": None}])
    assert exc.value.details["reason"] == "explicit_null"


def test_null_intermediate_is_not_provided():
    with pytest.raises(UnresolvableError) as exc:
        _interp("x: ${a.b}\n", [{"a": None}])
    assert exc.value.details["reason"] == "not_provided"


def test_non_scalar_in_textual_raises():
    with pytest.raises(MergeError):
        _interp("x: prefix ${val} suffix\n", [{"val": [1, 2, 3]}])


def test_multiple_placeholders_in_one_string():
    result = _interp("x: ${a}/${b}\n", [{"a": "one", "b": "two"}])
    assert result["x"] == "one/two"


def test_dotted_path():
    result = _interp("x: ${a.b.c}\n", [{"a": {"b": {"c": 7}}}])
    assert result["x"] == 7


def test_boolean_stringified_textual():
    result = _interp("x: ssl=${flag}\n", [{"flag": True}])
    assert result["x"] == "ssl=true"


def test_null_stringified_textual_errors():
    with pytest.raises(UnresolvableError):
        _interp("x: value=${val}\n", [{"val": None}])


def test_chained_textual():
    result = _interp("x: ${a}\n", [{"a": "hello ${b}", "b": "world"}])
    assert result["x"] == "hello world"


def test_chained_structural_scalar():
    result = _interp("x: ${a}\n", [{"a": "${b}", "b": 42}])
    assert result["x"] == 42


def test_chained_structural_to_list_splat():
    result = _interp(
        "xs:\n  - ${a}\n  - tail\n",
        [{"a": "${b}", "b": [1, 2, 3]}],
    )
    assert result["xs"] == [1, 2, 3, "tail"]


def test_chained_map_with_inner_placeholder():
    result = _interp(
        "x: ${a}\n",
        [{"a": {"name": "${b}"}, "b": "ok"}],
    )
    assert result["x"] == {"name": "ok"}


def test_chained_list_splat_inside_resolved_list():
    result = _interp(
        "xs: ${outer}\n",
        [{"outer": ["head", "${inner}", "tail"], "inner": [1, 2]}],
    )
    assert result["xs"] == ["head", 1, 2, "tail"]


def test_cycle_direct_self_reference():
    with pytest.raises(CycleError) as exc:
        _interp("x: ${a}\n", [{"a": "${a}"}])
    assert exc.value.code == 12
    assert "a" in exc.value.details["cycle"]


def test_cycle_two_step():
    with pytest.raises(CycleError) as exc:
        _interp("x: ${a}\n", [{"a": "${b}", "b": "${a}"}])
    assert exc.value.details["cycle"] == ["a", "b", "a"]


def test_cycle_via_map_value():
    with pytest.raises(CycleError):
        _interp(
            "x: ${a}\n",
            [{"a": {"loop": "${a}"}}],
        )


def test_cycle_in_textual_context():
    with pytest.raises(CycleError):
        _interp(
            "x: hi ${a}\n",
            [{"a": "ho ${a}"}],
        )


def test_non_scalar_in_textual_via_chain():
    with pytest.raises(MergeError):
        _interp(
            "x: prefix ${a} suffix\n",
            [{"a": "${b}", "b": [1, 2, 3]}],
        )


def test_abstract_resolved_by_descendant():
    result = _interp(
        "msg: ${abstract:greeting}\n",
        [{"greeting": "hello"}],
    )
    assert result["msg"] == "hello"


def test_abstract_unfilled_raises_exit16():
    with pytest.raises(AbstractUnfilledError) as exc:
        _interp("msg: ${abstract:greeting}\n", [])
    assert exc.value.code == 16
    assert exc.value.details["reason"] == "not_provided"
    assert exc.value.details["placeholder"] == "greeting"


def test_abstract_filled_through_intermediate_layer():
    result = _interp(
        "msg: ${abstract:greeting}\n",
        [{"other": 1}, {"greeting": "hola"}],
    )
    assert result["msg"] == "hola"


def test_abstract_in_block_scalar():
    result = _interp(
        "body: |\n  prefix ${abstract:name} suffix\n",
        [{"name": "world"}],
    )
    assert result["body"] == "prefix world suffix\n"


def test_abstract_null_shadow_raises_with_null_shadow_reason():
    with pytest.raises(AbstractUnfilledError) as exc:
        _interp("msg: ${abstract:greeting}\n", [{"greeting": None}])
    assert exc.value.code == 16
    assert exc.value.details["reason"] == "null_shadow"


def test_abstract_does_not_satisfy_abstract_exact_flow():
    with pytest.raises(AbstractUnfilledError) as exc:
        _interp(
            "msg: ${abstract:greeting}\n",
            [{"greeting": "${abstract:greeting}"}],
        )
    assert exc.value.details["reason"] == "abstract_inherited"


def test_abstract_does_not_satisfy_abstract_textual():
    with pytest.raises(AbstractUnfilledError) as exc:
        _interp(
            "msg: hello ${abstract:greeting} world\n",
            [{"greeting": "${abstract:greeting}"}],
        )
    assert exc.value.details["reason"] == "abstract_inherited"


def test_abstract_cross_path_inherited_still_unfilled():
    with pytest.raises(AbstractUnfilledError):
        _interp(
            "msg: ${abstract:foo}\n",
            [{"foo": "${abstract:bar}"}],
        )


def test_abstract_exact_structural_scalar_returns_concrete():
    result = _interp("x: ${abstract:val}\n", [{"val": 42}])
    assert result["x"] == 42


def test_abstract_non_scalar_in_textual_raises_merge():
    with pytest.raises(MergeError):
        _interp(
            "x: prefix ${abstract:val} suffix\n",
            [{"val": [1, 2, 3]}],
        )


def test_abstract_empty_body_in_flow_raises():
    with pytest.raises(UnresolvableError):
        _interp("x: ${abstract:}\n", [])


def test_abstract_empty_body_in_textual_raises():
    with pytest.raises(UnresolvableError):
        _interp("x: hi ${abstract:} bye\n", [])


def test_abstract_filled_at_nested_path():
    result = _interp(
        "x: ${abstract:db.host}\n",
        [{"db": {"host": "localhost"}}],
    )
    assert result["x"] == "localhost"


def test_collect_placeholder_errors_splits_abstracts_from_real():
    from stemmata.errors import AbstractUnfilledError as AU, UnresolvableError as UE
    from stemmata.interp import collect_placeholder_errors

    data = _load("body: hello ${abstract:name} and ${missing}\n")
    layers = [Layer(canonical_id="l0", data=data)]
    out: list = []
    collect_placeholder_errors(data, data, layers, parent_is_list=False, root_file="x.yaml", out=out)
    abstracts = [e for e in out if isinstance(e, AU)]
    others = [e for e in out if isinstance(e, UE)]
    assert len(abstracts) == 1
    assert abstracts[0].details["placeholder"] == "name"
    assert len(others) == 1
    assert others[0].details["placeholder"] == "missing"


def test_collect_placeholder_errors_enumerates_all_abstracts():
    from stemmata.errors import AbstractUnfilledError as AU
    from stemmata.interp import collect_placeholder_errors

    data = _load(
        "one: ${abstract:a}\n"
        "two: ${abstract:b}\n"
        "body: |\n"
        "  ${abstract:c} and ${abstract:d}\n"
    )
    layers = [Layer(canonical_id="l0", data=data)]
    out: list = []
    collect_placeholder_errors(data, data, layers, parent_is_list=False, root_file="x.yaml", out=out)
    paths = sorted(e.details["placeholder"] for e in out if isinstance(e, AU))
    assert paths == ["a", "b", "c", "d"]


def test_scan_abstract_references_reports_per_occurrence_positions():
    from stemmata.interp import scan_abstract_references

    data = _load(
        "system_message: |\n"
        "  You are ${abstract:persona.name}, a ${abstract:persona.role}.\n"
        "  Always answer in a ${abstract:persona.tone} tone.\n"
    )
    refs = scan_abstract_references(data, file_fallback="x.yaml")
    by_path = {r.path: (r.line, r.column) for r in refs}
    assert len(by_path) == 3
    assert len({pos for pos in by_path.values()}) == 3
    assert by_path["persona.name"][0] == by_path["persona.role"][0]
    assert by_path["persona.name"][1] < by_path["persona.role"][1]
    assert by_path["persona.tone"][0] > by_path["persona.name"][0]


def test_collect_placeholder_errors_reports_per_occurrence_positions():
    from stemmata.errors import AbstractUnfilledError as AU
    from stemmata.interp import collect_placeholder_errors

    data = _load(
        "body: |\n"
        "  alpha ${abstract:one} beta ${abstract:two}\n"
        "  gamma ${abstract:three}\n"
    )
    layers = [Layer(canonical_id="l0", data=data)]
    out: list = []
    collect_placeholder_errors(data, data, layers, parent_is_list=False, root_file="x.yaml", out=out)
    positions = {e.details["placeholder"]: (e.location["line"], e.location["column"])
                 for e in out if isinstance(e, AU)}
    assert len(positions) == 3
    assert len({pos for pos in positions.values()}) == 3
    assert positions["one"][0] == positions["two"][0]
    assert positions["one"][1] < positions["two"][1]
    assert positions["three"][0] > positions["one"][0]


def test_interp_abstract_error_reports_per_occurrence_position():
    from stemmata.errors import AbstractUnfilledError

    with pytest.raises(AbstractUnfilledError) as exc:
        _interp(
            "body: |\n  alpha ${abstract:one} beta ${abstract:two}\n",
            [{"one": "filled"}],
        )
    assert exc.value.details["placeholder"] == "two"
    loc = exc.value.location
    assert loc["line"] is not None and loc["column"] is not None


def test_scan_declared_abstracts_is_declaration_only():
    from stemmata.interp import scan_declared_abstracts

    data = _load(
        "flow: ${abstract:one}\n"                      # declaration (exact scalar)
        "mixed: hello ${abstract:two} there\n"         # usage, not a declaration
        "nested:\n"
        "  inner: ${abstract:three}\n"                 # declaration
        "list:\n"
        "  - ${abstract:four}\n"                       # declaration (element is exact)
        "  - plain text\n"
        "block: |\n"
        "  ${abstract:five}\n"                         # usage (block scalar is textual)
    )
    refs = scan_declared_abstracts(data, file_fallback="x.yaml")
    paths = sorted(r.path for r in refs)
    assert paths == ["four", "one", "three"]


def test_scan_abstract_references_includes_usages():
    from stemmata.interp import scan_abstract_references

    data = _load(
        "flow: ${abstract:one}\n"
        "mixed: hello ${abstract:two} there\n"
        "block: |\n"
        "  ${abstract:three}\n"
    )
    refs = scan_abstract_references(data, file_fallback="x.yaml")
    paths = sorted(r.path for r in refs)
    assert paths == ["one", "three", "two"]
