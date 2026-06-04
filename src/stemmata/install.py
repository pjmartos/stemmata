from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from stemmata.bundle import build_tarball, collect_members
from stemmata.cache import Cache
from stemmata.errors import SchemaError, UsageError
from stemmata.manifest import parse_manifest


@dataclass
class InstallResult:
    name: str
    version: str
    cache_path: str
    installed: bool


def _missing_or_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, (str, list, dict, tuple)) and len(value) == 0:
        return True
    return False


def run_install(path: Path, *, cache: Cache, refresh: bool = False) -> InstallResult:
    base = path.resolve()
    if not base.is_dir():
        raise UsageError(
            f"install target {str(path)!r} is not a directory",
            argument="path",
            reason="not_a_directory",
        )

    manifest_file = base / "package.json"
    if not manifest_file.is_file():
        raise SchemaError(
            f"no package.json found at {manifest_file}",
            file=str(manifest_file),
            field_name="package.json",
            reason="missing_manifest",
        )

    raw = manifest_file.read_text(encoding="utf-8")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise SchemaError(
            f"package.json at {manifest_file} is not valid JSON: {e.msg}",
            file=str(manifest_file),
            line=e.lineno,
            column=e.colno,
            field_name="<json>",
            reason="invalid_json",
        )
    if not isinstance(data, dict):
        raise SchemaError(
            f"package.json at {manifest_file} must be a JSON object",
            file=str(manifest_file),
            field_name="<root>",
            reason="not_object",
        )

    missing = [key for key in ("name", "version", "prompts") if key not in data or _missing_or_empty(data[key])]
    if missing:
        if missing == ["prompts"] and "prompts" in data and _missing_or_empty(data["prompts"]):
            raise SchemaError(
                f"package.json at {manifest_file} 'prompts' array must not be empty",
                file=str(manifest_file),
                field_name="prompts",
                reason="empty_prompts",
            )
        raise SchemaError(
            f"package.json at {manifest_file} is missing required field(s): {', '.join(missing)}",
            file=str(manifest_file),
            field_name=missing[0],
            reason="missing_field",
        )

    name = data["name"]
    version = data["version"]
    if (
        not refresh
        and isinstance(name, str)
        and isinstance(version, str)
        and cache.has_package(name, version)
    ):
        return InstallResult(
            name=name,
            version=version,
            cache_path=str(cache.package_dir(name, version)),
            installed=False,
        )

    manifest = parse_manifest(raw, file=str(manifest_file))

    extra_files: list[str] = ["package.json"]
    for optional in ("README.md", "LICENSE", "LICENSE.md", "LICENSE.txt"):
        if (base / optional).is_file():
            extra_files.append(optional)
    yaml_paths = [e.path for e in manifest.prompts]
    resource_paths = [e.path for e in manifest.resources]
    members = collect_members(base, extra_files, yaml_paths, resource_paths)
    tarball_bytes = build_tarball(members)

    with cache.lock(manifest.name, manifest.version):
        if cache.has_package(manifest.name, manifest.version) and not refresh:
            installed = False
        else:
            cache.install_tarball(manifest.name, manifest.version, tarball_bytes, force=refresh)
            installed = True

    return InstallResult(
        name=manifest.name,
        version=manifest.version,
        cache_path=str(cache.package_dir(manifest.name, manifest.version)),
        installed=installed,
    )
