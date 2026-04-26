from __future__ import annotations

import contextlib
import hashlib
import os
import shutil
import sys
import tarfile
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from stemmata.errors import CacheError, SchemaError


MAX_DECOMPRESSED_BYTES = 256 * 1024 * 1024


def default_cache_dir() -> Path:
    override = os.environ.get("PROMPT_CLI_CACHE_DIR")
    if override:
        return Path(override)
    return Path.home() / ".cache" / "stemmata"


@dataclass
class Cache:
    root: Path

    def __post_init__(self) -> None:
        try:
            self.root.mkdir(parents=True, exist_ok=True)
            (self.root / "packages").mkdir(exist_ok=True)
            (self.root / "locks").mkdir(exist_ok=True)
            (self.root / "staging").mkdir(exist_ok=True)
        except OSError as e:
            raise CacheError(
                str(self.root),
                f"cannot create cache directory ({type(e).__name__}: {e.strerror or e})",
            ) from e

    def package_dir(self, name: str, version: str) -> Path:
        safe = _safe_dirname(name)
        return self.root / "packages" / safe / version

    def has_package(self, name: str, version: str) -> bool:
        return self.package_dir(name, version).is_dir()

    @contextlib.contextmanager
    def lock(self, name: str, version: str) -> Iterator[None]:
        safe = _safe_dirname(f"{name}@{version}")
        lock_path = self.root / "locks" / f"{safe}.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR)
        deadline = time.monotonic() + 60.0
        try:
            if sys.platform == "win32":
                import msvcrt
                if os.fstat(fd).st_size < 1:
                    os.write(fd, b"\0")
                while True:
                    os.lseek(fd, 0, os.SEEK_SET)
                    try:
                        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                        break
                    except OSError:
                        if time.monotonic() > deadline:
                            raise CacheError(str(lock_path), "timed out waiting for lock")
                        time.sleep(0.05)
                try:
                    yield
                finally:
                    os.lseek(fd, 0, os.SEEK_SET)
                    try:
                        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
                    except OSError:
                        pass
            else:
                import fcntl
                while True:
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except (BlockingIOError, OSError):
                        if time.monotonic() > deadline:
                            raise CacheError(str(lock_path), "timed out waiting for lock")
                        time.sleep(0.05)
                try:
                    yield
                finally:
                    fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    def install_tarball(self, name: str, version: str, tarball_bytes: bytes, *, max_decompressed: int = MAX_DECOMPRESSED_BYTES, force: bool = False) -> Path:
        target = self.package_dir(name, version)
        if target.is_dir() and not force:
            return target
        staging = self.root / "staging" / f"{_safe_dirname(name)}-{version}-{uuid.uuid4().hex}"
        staging.mkdir(parents=True, exist_ok=False)
        tmp_tar = staging / "archive.tgz"
        tmp_tar.write_bytes(tarball_bytes)
        try:
            _extract_tarball(tmp_tar, staging / "pkg", max_decompressed=max_decompressed)
        except (SchemaError, CacheError):
            shutil.rmtree(staging, ignore_errors=True)
            raise
        target.parent.mkdir(parents=True, exist_ok=True)
        evict_staging: Path | None = None
        if target.exists():
            evict_staging = self.root / "staging" / f"evict-{uuid.uuid4().hex}"
            os.replace(target, evict_staging)
        os.replace(staging / "pkg", target)
        shutil.rmtree(staging, ignore_errors=True)
        if evict_staging is not None:
            shutil.rmtree(evict_staging, ignore_errors=True)
        return target

    def evict(self, name: str, version: str) -> tuple[bool, int]:
        target = self.package_dir(name, version)
        if not target.exists():
            return False, 0
        try:
            with self.lock(name, version):
                size = _dir_size(target)
                staging = self.root / "staging" / f"evict-{uuid.uuid4().hex}"
                os.replace(target, staging)
                shutil.rmtree(staging, ignore_errors=True)
                return True, size
        except CacheError:
            return False, 0

    def clear_all(self) -> tuple[int, int]:
        pkgs_root = self.root / "packages"
        removed = 0
        bytes_freed = 0
        if not pkgs_root.exists():
            return 0, 0
        for scope_dir in pkgs_root.iterdir():
            if not scope_dir.is_dir():
                continue
            for version_dir in scope_dir.iterdir():
                if not version_dir.is_dir():
                    continue
                name = _unsafe_dirname(scope_dir.name)
                version = version_dir.name
                ok, size = self.evict(name, version)
                if ok:
                    removed += 1
                    bytes_freed += size
            with contextlib.suppress(OSError):
                if not any(scope_dir.iterdir()):
                    scope_dir.rmdir()
        return removed, bytes_freed


def _safe_dirname(name: str) -> str:
    return name.replace("/", "__").replace("@", "AT_")


def _unsafe_dirname(name: str) -> str:
    return name.replace("__", "/").replace("AT_", "@")


def _dir_size(path: Path) -> int:
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            with contextlib.suppress(OSError):
                total += os.path.getsize(os.path.join(root, f))
    return total


def _extract_tarball(tar_path: Path, dest: Path, *, max_decompressed: int) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    total = 0
    try:
        with tarfile.open(tar_path, mode="r:gz") as tf:
            try:
                tf.extraction_filter = tarfile.data_filter  # type: ignore[attr-defined]
            except AttributeError:
                raise CacheError(str(dest), "python 3.12+ required for safe tar extraction")
            members: list[tarfile.TarInfo] = []
            for m in tf.getmembers():
                name = m.name.replace("\\", "/")
                if name.startswith("/") or ".." in name.split("/"):
                    raise SchemaError(
                        f"tarball contains unsafe path: {m.name!r}",
                        file=str(tar_path),
                        field_name="tarball",
                        reason="path_traversal",
                    )
                if m.issym() or m.islnk():
                    raise SchemaError(
                        f"tarball contains symlink/hardlink: {m.name!r}",
                        file=str(tar_path),
                        field_name="tarball",
                        reason="symlink_forbidden",
                    )
                if m.isdev() or m.isfifo():
                    raise SchemaError(
                        f"tarball contains device/FIFO: {m.name!r}",
                        file=str(tar_path),
                        field_name="tarball",
                        reason="device_forbidden",
                    )
                if m.isfile():
                    if m.mode & 0o111:
                        raise SchemaError(
                            f"tarball file has executable bit: {m.name!r}",
                            file=str(tar_path),
                            field_name="tarball",
                            reason="exec_bit",
                        )
                    total += m.size
                    if total > max_decompressed:
                        raise CacheError(str(dest), "decompression size limit exceeded")
                members.append(m)

            for m in members:
                name = m.name.replace("\\", "/")
                if name.startswith("package/"):
                    relname = name[len("package/"):]
                else:
                    relname = name
                if not relname:
                    continue
                target = dest / relname
                target.parent.mkdir(parents=True, exist_ok=True)
                if m.isdir():
                    target.mkdir(exist_ok=True)
                    continue
                extracted = tf.extractfile(m)
                if extracted is None:
                    continue
                data = extracted.read()
                if target.exists():
                    target.unlink()
                target.write_bytes(data)
    except tarfile.TarError as e:
        raise CacheError(str(tar_path), f"invalid tarball: {e}")
