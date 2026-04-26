import gzip
import io
import json
import tarfile

import pytest

from stemmata.cache import Cache, _extract_tarball
from stemmata.errors import CacheError, SchemaError


def _make_tarball(files: dict[str, bytes], *, mode: int = 0o644, extras: list[tarfile.TarInfo] | None = None) -> bytes:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        for name, data in files.items():
            ti = tarfile.TarInfo(name=name)
            ti.size = len(data)
            ti.mode = mode
            tf.addfile(ti, io.BytesIO(data))
        if extras:
            for e in extras:
                tf.addfile(e, io.BytesIO(b""))
    return buf.getvalue()


def test_cache_init_wraps_oserror_into_cache_error(tmp_path):
    blocker = tmp_path / "blocker"
    blocker.write_bytes(b"")
    with pytest.raises(CacheError) as ei:
        Cache(root=blocker)
    assert ei.value.code == 21
    assert ei.value.details["cache_path"] == str(blocker)
    assert "reason" in ei.value.details and ei.value.details["reason"]


def test_install_tarball_basic(tmp_path):
    cache = Cache(root=tmp_path / "c")
    manifest = {"name": "@a/b", "version": "1.0.0", "prompts": [{"path": "prompts/x.yaml"}]}
    tar = _make_tarball({
        "package/package.json": json.dumps(manifest).encode(),
        "package/prompts/x.yaml": b"foo: bar\n",
    })
    cache.install_tarball("@a/b", "1.0.0", tar)
    assert cache.has_package("@a/b", "1.0.0")
    pkg_root = cache.package_dir("@a/b", "1.0.0")
    assert (pkg_root / "package.json").exists()
    assert (pkg_root / "prompts/x.yaml").read_bytes() == b"foo: bar\n"


def test_install_tarball_rejects_traversal(tmp_path):
    cache = Cache(root=tmp_path / "c")
    ti = tarfile.TarInfo(name="../../etc/passwd")
    ti.size = 0
    tar = _make_tarball({}, extras=[ti])
    with pytest.raises(SchemaError):
        cache.install_tarball("@a/b", "1.0.0", tar)


def test_install_tarball_rejects_symlink(tmp_path):
    cache = Cache(root=tmp_path / "c")
    ti = tarfile.TarInfo(name="package/link")
    ti.type = tarfile.SYMTYPE
    ti.linkname = "/etc/passwd"
    tar = _make_tarball({"package/package.json": b"{}"}, extras=[ti])
    with pytest.raises(SchemaError):
        cache.install_tarball("@a/b", "1.0.0", tar)


def test_install_tarball_rejects_exec_bit(tmp_path):
    cache = Cache(root=tmp_path / "c")
    tar = _make_tarball({"package/script.sh": b"#!\n"}, mode=0o755)
    with pytest.raises(SchemaError):
        cache.install_tarball("@a/b", "1.0.0", tar)


def test_evict_package(tmp_path):
    cache = Cache(root=tmp_path / "c")
    tar = _make_tarball({"package/package.json": b"{}"})
    cache.install_tarball("@a/b", "1.0.0", tar)
    assert cache.has_package("@a/b", "1.0.0")
    ok, size = cache.evict("@a/b", "1.0.0")
    assert ok
    assert size > 0
    assert not cache.has_package("@a/b", "1.0.0")


def test_clear_all(tmp_path):
    cache = Cache(root=tmp_path / "c")
    for v in ["1.0.0", "1.0.1"]:
        tar = _make_tarball({"package/package.json": b"{}"})
        cache.install_tarball("@a/b", v, tar)
    removed, _ = cache.clear_all()
    assert removed == 2


def test_install_twice_is_idempotent(tmp_path):
    cache = Cache(root=tmp_path / "c")
    tar = _make_tarball({"package/package.json": b"{}"})
    cache.install_tarball("@a/b", "1.0.0", tar)
    cache.install_tarball("@a/b", "1.0.0", tar)
    assert cache.has_package("@a/b", "1.0.0")


def test_decompression_size_limit(tmp_path):
    cache = Cache(root=tmp_path / "c")
    big = b"x" * 2048
    tar = _make_tarball({"package/big.bin": big})
    with pytest.raises(CacheError):
        cache.install_tarball("@a/b", "1.0.0", tar, max_decompressed=1024)
