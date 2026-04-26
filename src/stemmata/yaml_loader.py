from __future__ import annotations

from typing import Any

import yaml

from stemmata.errors import SchemaError


_CORE_TAGS = {
    "tag:yaml.org,2002:str",
    "tag:yaml.org,2002:int",
    "tag:yaml.org,2002:float",
    "tag:yaml.org,2002:bool",
    "tag:yaml.org,2002:null",
    "tag:yaml.org,2002:seq",
    "tag:yaml.org,2002:map",
}


class _SafeLoader(yaml.SafeLoader):
    pass


def _reject_non_core(loader: yaml.Loader, tag_suffix: str, node: yaml.Node) -> None:
    mark = node.start_mark
    raise SchemaError(
        f"disallowed YAML tag '{node.tag}' at line {mark.line + 1}",
        file=getattr(mark, "name", None),
        line=mark.line + 1,
        column=mark.column + 1,
        field_name=node.tag,
        reason="disallowed_yaml_tag",
    )


_SafeLoader.add_multi_constructor("tag:yaml.org,2002:python/", _reject_non_core)
_SafeLoader.add_multi_constructor("!", _reject_non_core)


def _reject_non_core_tag(loader: yaml.Loader, tag_suffix: str, node: yaml.Node) -> None:
    full_tag = "tag:" + tag_suffix
    if full_tag in _CORE_TAGS:
        # Let the core constructor handle it; should not normally reach here
        # since PyYAML dispatches core tags to specific constructors first.
        return loader.construct_mapping(node) if isinstance(node, yaml.MappingNode) else loader.construct_scalar(node)  # type: ignore[arg-type]
    _reject_non_core(loader, tag_suffix, node)


_SafeLoader.add_multi_constructor("tag:", _reject_non_core_tag)


class Node:
    __slots__ = ("value", "line", "column", "file")

    def __init__(self, value: Any, line: int | None, column: int | None, file: str | None):
        self.value = value
        self.line = line
        self.column = column
        self.file = file


def _check_bom_and_crlf(text: str, file: str) -> None:
    if text.startswith("\ufeff"):
        raise SchemaError(
            f"YAML file {file} begins with a BOM",
            file=file,
            line=1,
            column=1,
            field_name="<bom>",
            reason="bom_present",
        )


def safe_load_yaml(text: str, *, file: str, strict: bool = True) -> Any:
    if strict:
        _check_bom_and_crlf(text, file)
    try:
        loader = _SafeLoader(text)
        loader.name = file
        try:
            return loader.get_single_data()
        finally:
            loader.dispose()
    except SchemaError:
        raise
    except yaml.YAMLError as e:
        mark = getattr(e, "problem_mark", None)
        line = mark.line + 1 if mark else None
        column = mark.column + 1 if mark else None
        raise SchemaError(
            f"YAML parse error in {file}: {e}",
            file=file,
            line=line,
            column=column,
            field_name="<yaml>",
            reason=str(e),
        )


def _is_flow_scalar(node: yaml.Node) -> bool:
    if not isinstance(node, yaml.ScalarNode):
        return False
    return node.style in (None, "", "'", '"')


def load_with_positions(text: str, *, file: str, strict: bool = True) -> tuple[Any, dict[int, tuple[int, int, bool]]]:
    if strict:
        _check_bom_and_crlf(text, file)
    loader = _SafeLoader(text)
    loader.name = file
    positions: dict[int, tuple[int, int, bool]] = {}

    original_construct_scalar = loader.construct_scalar

    def tracking_construct_scalar(node: yaml.ScalarNode) -> Any:
        value = original_construct_scalar(node)
        wrapped = _ScalarStr(value)
        wrapped._pcli_line = node.start_mark.line + 1
        wrapped._pcli_column = node.start_mark.column + 1
        wrapped._pcli_flow = _is_flow_scalar(node)
        positions[id(wrapped)] = (
            wrapped._pcli_line,
            wrapped._pcli_column,
            wrapped._pcli_flow,
        )
        return wrapped

    loader.construct_scalar = tracking_construct_scalar  # type: ignore[method-assign]

    try:
        node = loader.get_single_node()
        if node is None:
            return None, positions
        data = loader.construct_document(node)
    except SchemaError:
        raise
    except yaml.YAMLError as e:
        mark = getattr(e, "problem_mark", None)
        raise SchemaError(
            f"YAML parse error in {file}: {e}",
            file=file,
            line=mark.line + 1 if mark else None,
            column=mark.column + 1 if mark else None,
            field_name="<yaml>",
            reason=str(e),
        )
    finally:
        loader.dispose()
    return data, positions


class _ScalarStr(str):
    __slots__ = ("_pcli_line", "_pcli_column", "_pcli_flow", "_pcli_file")

    def __new__(cls, value: str) -> "_ScalarStr":
        inst = super().__new__(cls, value)
        inst._pcli_line = None
        inst._pcli_column = None
        inst._pcli_flow = True
        inst._pcli_file = None
        return inst


def scalar_meta(s: Any) -> tuple[str | None, int | None, int | None, bool]:
    if isinstance(s, _ScalarStr):
        return s._pcli_file, s._pcli_line, s._pcli_column, s._pcli_flow
    # Default to is_flow=True for plain str values.  Block scalars (|, >)
    # always arrive as _ScalarStr with _pcli_flow=False from the YAML loader.
    # A plain str that lost its _ScalarStr wrapper is more likely a flow
    # scalar than a block scalar, so defaulting to True avoids silently
    # suppressing structural interpolation.
    return None, None, None, True


def load_all_with_positions(
    text: str, *, file: str, strict: bool = True
) -> list[tuple[Any, int]]:
    """Load every document from a multi-document YAML stream.

    Returns ``[(data, start_line), ...]``.  Line numbers inside
    ``_ScalarStr`` instances are absolute (relative to the full stream).
    """
    if strict:
        _check_bom_and_crlf(text, file)
    loader = _SafeLoader(text)
    loader.name = file
    original_construct_scalar = loader.construct_scalar

    def tracking(node: yaml.ScalarNode) -> Any:
        value = original_construct_scalar(node)
        wrapped = _ScalarStr(value)
        wrapped._pcli_line = node.start_mark.line + 1
        wrapped._pcli_column = node.start_mark.column + 1
        wrapped._pcli_flow = _is_flow_scalar(node)
        return wrapped

    loader.construct_scalar = tracking  # type: ignore[method-assign]
    docs: list[tuple[Any, int]] = []
    try:
        while loader.check_data():
            start_line = loader.get_mark().line + 1
            docs.append((loader.get_data(), start_line))
    except SchemaError:
        raise
    except yaml.YAMLError as e:
        mark = getattr(e, "problem_mark", None)
        raise SchemaError(
            f"YAML parse error in {file}: {e}",
            file=file,
            line=mark.line + 1 if mark else None,
            column=mark.column + 1 if mark else None,
            field_name="<yaml>",
            reason=str(e),
        )
    finally:
        loader.dispose()
    return docs


def attach_file(data: Any, file: str) -> Any:
    if isinstance(data, _ScalarStr):
        data._pcli_file = file
    elif isinstance(data, dict):
        for v in data.values():
            attach_file(v, file)
    elif isinstance(data, list):
        for v in data:
            attach_file(v, file)
    return data
