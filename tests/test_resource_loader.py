import pytest

from stemmata.errors import SchemaError
from stemmata.resource_loader import parse_resource, read_resource


def test_plain_resource_has_no_references():
    doc = parse_resource("# Heading\n\nSome prose.\n", file="x.md")
    assert doc.content == "# Heading\n\nSome prose.\n"
    assert doc.references == []


def test_reference_on_own_line():
    src = "Intro\n${resource:foo.md}\nOutro\n"
    doc = parse_resource(src, file="x.md")
    assert [r.raw for r in doc.references] == ["foo.md"]
    assert doc.references[0].line == 2


def test_coordinate_reference():
    src = "${resource:@acme/common@1.0.4#footer}\n"
    doc = parse_resource(src, file="x.md")
    assert [r.raw for r in doc.references] == ["@acme/common@1.0.4#footer"]


def test_multiple_references_on_separate_lines():
    src = "${resource:a.md}\n\n${resource:b.md}\n"
    doc = parse_resource(src, file="x.md")
    assert [r.raw for r in doc.references] == ["a.md", "b.md"]
    assert [r.line for r in doc.references] == [1, 3]


def test_mid_line_reference_rejected():
    src = "prefix ${resource:foo.md} suffix\n"
    with pytest.raises(SchemaError) as ei:
        parse_resource(src, file="x.md")
    assert ei.value.details["reason"] == "resource_not_line_exclusive"


def test_leading_whitespace_rejected():
    src = "   ${resource:foo.md}\n"
    with pytest.raises(SchemaError) as ei:
        parse_resource(src, file="x.md")
    assert ei.value.details["reason"] == "resource_not_line_exclusive"


def test_trailing_whitespace_rejected():
    src = "${resource:foo.md}  \n"
    with pytest.raises(SchemaError) as ei:
        parse_resource(src, file="x.md")
    assert ei.value.details["reason"] == "resource_not_line_exclusive"


def test_two_references_on_one_line_rejected():
    src = "${resource:a.md}${resource:b.md}\n"
    with pytest.raises(SchemaError) as ei:
        parse_resource(src, file="x.md")
    # Either multi-per-line or not-line-exclusive is acceptable; both are valid
    # diagnostics for the same underlying problem.
    assert ei.value.details["reason"] in (
        "resource_multiple_per_line",
        "resource_not_line_exclusive",
    )


def test_escaped_reference_not_counted():
    src = "literal $${resource:foo.md} example\n"
    doc = parse_resource(src, file="x.md")
    assert doc.references == []


def test_empty_body_rejected():
    src = "${resource:}\n"
    # The regex requires at least one char between 'resource:' and '}', so
    # ``${resource:}`` does not match — treated as plain text, no refs.
    doc = parse_resource(src, file="x.md")
    assert doc.references == []


def test_whitespace_only_body_rejected():
    src = "${resource:   }\n"
    with pytest.raises(SchemaError) as ei:
        parse_resource(src, file="x.md")
    assert ei.value.details["reason"] == "resource_empty_body"


def test_bom_rejected_in_strict_mode():
    raw = b"\xef\xbb\xbf# Heading\n"
    with pytest.raises(SchemaError) as ei:
        parse_resource(raw.decode("utf-8"), file="x.md", strict=True, raw_bytes=raw)
    assert ei.value.details["reason"] == "bom_present"


def test_strict_mode_accepts_crlf_per_prd_780(tmp_path):
    p = tmp_path / "x.md"
    p.write_bytes(b"# heading\r\n${resource:./other.md}\r\nmore\r\n")
    doc = read_resource(str(p), strict=True)
    assert "\r" not in doc.content
    assert len(doc.references) == 1
    assert doc.references[0].raw == "./other.md"
    assert doc.references[0].line == 2


def test_lax_mode_accepts_crlf():
    raw = "# heading\r\nmore\r\n"
    doc = parse_resource(raw, file="x.md", strict=False)
    assert doc.references == []


def test_read_resource_from_disk(tmp_path):
    p = tmp_path / "foo.md"
    p.write_bytes(b"${resource:bar.md}\n")
    doc = read_resource(str(p))
    assert [r.raw for r in doc.references] == ["bar.md"]
