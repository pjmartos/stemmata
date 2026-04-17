import pytest

from stemmata.errors import SchemaError
from stemmata.json_loader import load_json_with_positions, safe_load_json
from stemmata.yaml_loader import _ScalarStr, scalar_meta


def test_basic_object():
    data, _ = load_json_with_positions('{"foo": "bar"}', file="x.json")
    assert data == {"foo": "bar"}


def test_nested_object():
    data, _ = load_json_with_positions('{"a": {"b": 1}}', file="x.json")
    assert data == {"a": {"b": 1}}


def test_array():
    data, _ = load_json_with_positions('[1, "two", true, null]', file="x.json")
    assert data == [1, "two", True, None]


def test_string_values_are_scalar_str():
    data, _ = load_json_with_positions('{"key": "value"}', file="x.json")
    assert isinstance(data["key"], _ScalarStr)
    for k in data:
        assert isinstance(k, _ScalarStr)


def test_string_position_tracking():
    text = '{\n  "name": "hello"\n}'
    data, _ = load_json_with_positions(text, file="x.json")
    _, line, col, is_flow = scalar_meta(data["name"])
    assert line == 2
    assert col == 11
    assert is_flow is True


def test_key_position_tracking():
    text = '{\n  "name": "hello"\n}'
    data, _ = load_json_with_positions(text, file="x.json")
    for k in data:
        if k == "name":
            _, line, col, _ = scalar_meta(k)
            assert line == 2
            assert col == 3


def test_multiline_positions():
    text = '{\n  "a": "x",\n  "b": "y",\n  "c": "z"\n}'
    data, _ = load_json_with_positions(text, file="x.json")
    _, line_a, _, _ = scalar_meta(data["a"])
    _, line_b, _, _ = scalar_meta(data["b"])
    _, line_c, _, _ = scalar_meta(data["c"])
    assert line_a == 2
    assert line_b == 3
    assert line_c == 4


def test_escape_sequences():
    text = r'{"msg": "hello\nworld\t\"quoted\""}'
    data, _ = load_json_with_positions(text, file="x.json")
    assert data["msg"] == 'hello\nworld\t"quoted"'


def test_unicode_escape():
    text = '{"ch": "\\u0041"}'
    data, _ = load_json_with_positions(text, file="x.json")
    assert data["ch"] == "A"


def test_surrogate_pair():
    text = '{"emoji": "\\uD83D\\uDE00"}'
    data, _ = load_json_with_positions(text, file="x.json")
    assert data["emoji"] == "\U0001f600"


def test_number_int():
    data, _ = load_json_with_positions('{"n": 42}', file="x.json")
    assert data["n"] == 42
    assert isinstance(data["n"], int)


def test_number_negative():
    data, _ = load_json_with_positions('{"n": -7}', file="x.json")
    assert data["n"] == -7


def test_number_float():
    data, _ = load_json_with_positions('{"n": 3.14}', file="x.json")
    assert abs(data["n"] - 3.14) < 1e-9


def test_number_exponent():
    data, _ = load_json_with_positions('{"n": 1e10}', file="x.json")
    assert data["n"] == 1e10


def test_boolean_and_null():
    data, _ = load_json_with_positions('{"t": true, "f": false, "n": null}', file="x.json")
    assert data["t"] is True
    assert data["f"] is False
    assert data["n"] is None


def test_empty_object():
    data, _ = load_json_with_positions("{}", file="x.json")
    assert data == {}


def test_empty_array():
    data, _ = load_json_with_positions("[]", file="x.json")
    assert data == []


def test_empty_string():
    data, _ = load_json_with_positions('{"x": ""}', file="x.json")
    assert data["x"] == ""
    assert isinstance(data["x"], _ScalarStr)


def test_empty_input():
    data, _ = load_json_with_positions("", file="x.json")
    assert data is None


def test_whitespace_only():
    data, _ = load_json_with_positions("  \n  ", file="x.json")
    assert data is None


def test_bom_stripped():
    data, _ = load_json_with_positions('\ufeff{"x": 1}', file="x.json")
    assert data == {"x": 1}


def test_malformed_json():
    with pytest.raises(SchemaError) as exc:
        load_json_with_positions("{bad", file="x.json")
    assert "json_parse_error" == exc.value.details["reason"]


def test_unterminated_string():
    with pytest.raises(SchemaError):
        load_json_with_positions('{"key": "unterminated', file="x.json")


def test_trailing_content():
    with pytest.raises(SchemaError):
        load_json_with_positions('{"a": 1} extra', file="x.json")


def test_safe_load_json_basic():
    data = safe_load_json('{"x": 1}', file="x.json")
    assert data == {"x": 1}


def test_safe_load_json_error():
    with pytest.raises(SchemaError):
        safe_load_json("{bad", file="x.json")


def test_safe_load_json_bom():
    data = safe_load_json('\ufeff{"x": 1}', file="x.json")
    assert data == {"x": 1}


def test_nested_array_of_objects():
    text = '{"items": [{"id": 1}, {"id": 2}]}'
    data, _ = load_json_with_positions(text, file="x.json")
    assert data["items"][0]["id"] == 1
    assert data["items"][1]["id"] == 2


def test_positions_dict_populated():
    text = '{"a": "hello", "b": "world"}'
    data, positions = load_json_with_positions(text, file="x.json")
    # positions should contain entries for all _ScalarStr instances
    assert len(positions) > 0
    for _id, (line, col, is_flow) in positions.items():
        assert line >= 1
        assert col >= 1
        assert is_flow is True
