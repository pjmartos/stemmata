import pytest

from stemmata.errors import SchemaError
from stemmata.yaml_loader import load_with_positions, safe_load_yaml, scalar_meta


def test_basic_load():
    data = safe_load_yaml("foo: bar\n", file="x.yaml")
    assert data == {"foo": "bar"}


def test_rejects_python_tag():
    text = "foo: !!python/object/apply:os.system ['rm -rf /']\n"
    with pytest.raises(SchemaError):
        safe_load_yaml(text, file="x.yaml")


def test_rejects_custom_bang_tag():
    with pytest.raises(SchemaError):
        safe_load_yaml("foo: !Custom bar\n", file="x.yaml")


def test_rejects_bom():
    with pytest.raises(SchemaError):
        safe_load_yaml("\ufefffoo: bar\n", file="x.yaml")


def test_accepts_crlf_per_prd_780():
    data = safe_load_yaml("foo: bar\r\n", file="x.yaml")
    assert data == {"foo": "bar"}


def test_yaml_error_raises_schema_error():
    with pytest.raises(SchemaError):
        safe_load_yaml("foo: [unterminated\n", file="x.yaml")


def test_positions_tracked_for_flow_scalars():
    text = "foo: ${bar}\n"
    data, _ = load_with_positions(text, file="x.yaml")
    file, line, column, is_flow = scalar_meta(data["foo"])
    assert line == 1
    assert is_flow is True


def test_block_scalar_is_not_flow():
    text = "body: |\n  hello ${foo}\n"
    data, _ = load_with_positions(text, file="x.yaml")
    _, _, _, is_flow = scalar_meta(data["body"])
    assert is_flow is False
