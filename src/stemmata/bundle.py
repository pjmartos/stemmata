from __future__ import annotations

import gzip
import hashlib
import io
import os
import tarfile
from dataclasses import dataclass
from pathlib import Path

from stemmata.errors import SchemaError


# Deterministic build constants. All members carry the same mtime so two
# builds of identical inputs produce byte-identical tarballs.
_DETERMINISTIC_MTIME = 0
_FILE_MODE = 0o644
_DIR_MODE = 0o755
_BOM_BYTES = b"\xef\xbb\xbf"


@dataclass
class BundleMember:
    arcname: str
    data: bytes
    is_dir: bool = False


def _normalise_yaml_bytes(raw: bytes, *, file: str) -> bytes:
    """Strip BOM and normalise CRLF -> LF for YAML payloads on publish."""
    if raw.startswith(_BOM_BYTES):
        raw = raw[len(_BOM_BYTES):]
    if b"\r\n" in raw or b"\r" in raw:
        raw = raw.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    return raw


def _is_safe_arcname(arcname: str) -> bool:
    if not arcname:
        return False
    if arcname.startswith("/") or "\\" in arcname:
        return False
    parts = arcname.split("/")
    for p in parts:
        if p in ("", ".", ".."):
            return False
    return True


def collect_members(
    package_root: Path,
    extra_files: list[str],
    yaml_paths: list[str],
) -> list[BundleMember]:
    """Collect tarball members from a publish source directory.

    ``yaml_paths`` are POSIX-relative paths to YAML payload files (subject to
    BOM/CRLF normalisation). ``extra_files`` are additional package-relative
    files to ship verbatim (e.g. ``package.json``, ``README.md``, ``LICENSE``).
    """
    members: list[BundleMember] = []
    seen: set[str] = set()

    def _add_file(rel: str, data: bytes) -> None:
        if rel in seen:
            return
        seen.add(rel)
        if not _is_safe_arcname(rel):
            raise SchemaError(
                f"unsafe path in bundle: {rel!r}",
                file=str(package_root / rel),
                field_name="bundle",
                reason="unsafe_path",
            )
        members.append(BundleMember(arcname=rel, data=data))

    for rel in extra_files:
        if not _is_safe_arcname(rel):
            raise SchemaError(
                f"unsafe path in bundle: {rel!r}",
                file=str(package_root / rel),
                field_name="bundle",
                reason="unsafe_path",
            )
        full = package_root / rel
        if not full.is_file():
            continue
        if full.is_symlink():
            raise SchemaError(
                f"refusing to bundle symlink: {rel!r}",
                file=str(full),
                field_name="bundle",
                reason="symlink_forbidden",
            )
        _add_file(rel, full.read_bytes())

    for rel in yaml_paths:
        full = package_root / rel
        if not full.is_file():
            raise SchemaError(
                f"prompt payload file declared by manifest does not exist: {rel}",
                file=str(full),
                field_name="path",
                reason="missing_prompt_file",
            )
        if full.is_symlink():
            raise SchemaError(
                f"refusing to bundle symlink: {rel!r}",
                file=str(full),
                field_name="bundle",
                reason="symlink_forbidden",
            )
        raw = full.read_bytes()
        normalised = _normalise_yaml_bytes(raw, file=str(full))
        _add_file(rel, normalised)

    members.sort(key=lambda m: m.arcname)
    return members


def build_tarball(members: list[BundleMember]) -> bytes:
    """Build a deterministic gzipped tarball with a single ``package/`` root.

    All members share mtime 0, uid/gid 0, owner ""/"" and mode 0644 (files) or
    0755 (dirs). Entries are emitted in sorted arcname order. Gzip is wrapped
    around the tar stream with a fixed header (no embedded filename / mtime).
    """
    tar_buf = io.BytesIO()
    with tarfile.open(fileobj=tar_buf, mode="w", format=tarfile.USTAR_FORMAT) as tf:
        emitted_dirs: set[str] = set()

        def _ensure_dir(dirpath: str) -> None:
            if not dirpath or dirpath in emitted_dirs:
                return
            parent = "/".join(dirpath.split("/")[:-1])
            if parent:
                _ensure_dir(parent)
            info = tarfile.TarInfo(name=f"package/{dirpath}/")
            info.type = tarfile.DIRTYPE
            info.mode = _DIR_MODE
            info.mtime = _DETERMINISTIC_MTIME
            info.uid = 0
            info.gid = 0
            info.uname = ""
            info.gname = ""
            tf.addfile(info)
            emitted_dirs.add(dirpath)

        # Always emit the root package directory itself as the first entry.
        root_info = tarfile.TarInfo(name="package/")
        root_info.type = tarfile.DIRTYPE
        root_info.mode = _DIR_MODE
        root_info.mtime = _DETERMINISTIC_MTIME
        root_info.uid = 0
        root_info.gid = 0
        root_info.uname = ""
        root_info.gname = ""
        tf.addfile(root_info)

        for m in members:
            if m.is_dir:
                _ensure_dir(m.arcname)
                continue
            parent = "/".join(m.arcname.split("/")[:-1])
            if parent:
                _ensure_dir(parent)
            info = tarfile.TarInfo(name=f"package/{m.arcname}")
            info.size = len(m.data)
            info.mode = _FILE_MODE
            info.mtime = _DETERMINISTIC_MTIME
            info.uid = 0
            info.gid = 0
            info.uname = ""
            info.gname = ""
            info.type = tarfile.REGTYPE
            tf.addfile(info, io.BytesIO(m.data))

    tar_bytes = tar_buf.getvalue()
    gz_buf = io.BytesIO()
    # mtime=0 + filename=None + no comment -> reproducible gzip header.
    with gzip.GzipFile(filename="", mode="wb", fileobj=gz_buf, mtime=0) as gz:
        gz.write(tar_bytes)
    return gz_buf.getvalue()


def integrity_sha512(data: bytes) -> str:
    import base64
    return "sha512-" + base64.b64encode(hashlib.sha512(data).digest()).decode("ascii")


def shasum_sha1(data: bytes) -> str:
    return hashlib.sha1(data).hexdigest()


def tarball_filename(name: str, version: str) -> str:
    if name.startswith("@") and "/" in name:
        _, simple = name.split("/", 1)
    else:
        simple = name
    return f"{simple}-{version}.tgz"
