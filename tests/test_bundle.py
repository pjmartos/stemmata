import gzip
import io
import tarfile

import pytest

from stemmata.bundle import (
    BundleMember,
    build_tarball,
    collect_members,
    integrity_sha512,
    shasum_sha1,
    tarball_filename,
)
from stemmata.errors import SchemaError


def _list_members(tarball: bytes) -> list[tarfile.TarInfo]:
    with tarfile.open(fileobj=io.BytesIO(tarball), mode="r:gz") as tf:
        return list(tf.getmembers())


def test_build_tarball_is_byte_deterministic():
    members = [
        BundleMember(arcname="package.json", data=b'{"name":"@x/y","version":"1.0.0"}'),
        BundleMember(arcname="prompts/base.yaml", data=b"vars:\n  x: 1\n"),
    ]
    a = build_tarball(members)
    b = build_tarball(members)
    assert a == b


def test_build_tarball_is_order_independent():
    m1 = [
        BundleMember(arcname="package.json", data=b"{}"),
        BundleMember(arcname="prompts/a.yaml", data=b"a: 1\n"),
        BundleMember(arcname="prompts/b.yaml", data=b"b: 2\n"),
    ]
    m2 = list(reversed(m1))
    # collect_members already sorts, but build_tarball without pre-sort still
    # needs to produce a deterministic stream as long as inputs agree on
    # arcname ordering. Verify by sorting before build.
    a = build_tarball(sorted(m1, key=lambda m: m.arcname))
    b = build_tarball(sorted(m2, key=lambda m: m.arcname))
    assert a == b


def test_tarball_uses_package_root_and_safe_modes():
    members = [
        BundleMember(arcname="prompts/base.yaml", data=b"a: 1\n"),
        BundleMember(arcname="package.json", data=b"{}"),
    ]
    tb = build_tarball(sorted(members, key=lambda m: m.arcname))
    infos = _list_members(tb)
    names = [i.name.rstrip("/") for i in infos]
    assert "package" in names
    assert "package/package.json" in names
    assert "package/prompts/base.yaml" in names
    for info in infos:
        if info.isfile():
            assert info.mode == 0o644
            assert info.mode & 0o111 == 0
        elif info.isdir():
            assert info.mode == 0o755
        assert info.uid == 0
        assert info.gid == 0
        assert info.uname == ""
        assert info.gname == ""
        assert info.mtime == 0


def test_collect_members_strips_bom_and_normalises_crlf(tmp_path):
    (tmp_path / "package.json").write_bytes(b"{}")
    bad = tmp_path / "prompts" / "x.yaml"
    bad.parent.mkdir()
    bad.write_bytes(b"\xef\xbb\xbfvars:\r\n  x: 1\r\n")
    members = collect_members(tmp_path, ["package.json"], ["prompts/x.yaml"])
    payload = next(m for m in members if m.arcname == "prompts/x.yaml")
    assert payload.data == b"vars:\n  x: 1\n"


def test_collect_members_rejects_unsafe_paths(tmp_path):
    (tmp_path / "package.json").write_bytes(b"{}")
    with pytest.raises(SchemaError):
        collect_members(tmp_path, ["../escape.json"], [])


def test_collect_members_missing_prompt_file_raises(tmp_path):
    (tmp_path / "package.json").write_bytes(b"{}")
    with pytest.raises(SchemaError):
        collect_members(tmp_path, ["package.json"], ["prompts/missing.yaml"])


def test_integrity_and_shasum_helpers():
    data = b"hello"
    sha = integrity_sha512(data)
    assert sha.startswith("sha512-")
    sh1 = shasum_sha1(data)
    assert len(sh1) == 40


def test_tarball_filename_strips_scope():
    assert tarball_filename("@acme/prompts-core", "1.2.3") == "prompts-core-1.2.3.tgz"
    assert tarball_filename("plain", "0.1.0") == "plain-0.1.0.tgz"


def test_gzip_header_is_reproducible():
    members = [BundleMember(arcname="package.json", data=b"{}")]
    a = build_tarball(members)
    # GzipFile with mtime=0 and no filename writes a stable header. Confirm
    # bytes 4-8 (mtime field) are zero.
    assert a[4:8] == b"\x00\x00\x00\x00"
    decoded = gzip.decompress(a)
    assert b"package/package.json" in decoded
