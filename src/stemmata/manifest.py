from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from stemmata.errors import SchemaError


_SCOPED_NAME_RE = re.compile(r"^@([a-z0-9][a-z0-9\-_]*)/([a-z0-9][a-z0-9\-_]*)$")
_SEMVER_RE = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-((?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*)"
    r"(?:\.(?:0|[1-9]\d*|\d*[A-Za-z-][0-9A-Za-z-]*))*))?"
    r"(?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$"
)
_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_\-]*$")
_ASCII_PRINTABLE_RE = re.compile(r"^[\x20-\x7e]+$")
_FORBIDDEN_SUFFIXES = ("-SNAPSHOT",)


def is_scoped_name(name: str) -> bool:
    return bool(_SCOPED_NAME_RE.match(name))


def is_semver(version: str) -> bool:
    if not _SEMVER_RE.match(version):
        return False
    for suffix in _FORBIDDEN_SUFFIXES:
        if version.endswith(suffix):
            return False
    return True


@dataclass
class PromptEntry:
    id: str
    path: str
    contentType: str


@dataclass
class Manifest:
    name: str
    version: str
    description: str | None
    license: str | None
    dependencies: dict[str, str]
    prompts: list[PromptEntry]

    def prompt_by_id(self, pid: str) -> PromptEntry | None:
        for p in self.prompts:
            if p.id == pid:
                return p
        return None

    def prompt_by_path(self, path: str) -> PromptEntry | None:
        for p in self.prompts:
            if p.path == path:
                return p
        return None


def _entry_handle(entry: Any, idx: int) -> str:
    if isinstance(entry, dict):
        path = entry.get("path")
        if isinstance(path, str) and path:
            return f"prompts[path={path!r}]"
        pid = entry.get("id")
        if isinstance(pid, str) and pid:
            return f"prompts[id={pid!r}]"
    return f"prompts[index={idx}]"


def _derive_default_id(path: str, *, file: str) -> str:
    basename = path.rstrip("/").split("/")[-1]
    if "." in basename:
        derived = basename.rsplit(".", 1)[0]
    else:
        derived = basename
    if not _ID_RE.match(derived):
        raise SchemaError(
            f"derived default id '{derived}' from path '{path}' does not match grammar",
            file=file,
            field_name="id",
            reason="invalid_default_id",
        )
    return derived


def parse_manifest(raw: str, *, file: str = "package.json") -> Manifest:
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise SchemaError(
            f"package.json is not valid JSON: {e.msg}",
            file=file,
            line=e.lineno,
            column=e.colno,
            field_name="<json>",
            reason="invalid_json",
        )
    if not isinstance(data, dict):
        raise SchemaError(
            "package.json must be a JSON object",
            file=file,
            field_name="<root>",
            reason="not_object",
        )
    return validate_manifest(data, file=file)


def validate_manifest(data: dict[str, Any], *, file: str = "package.json") -> Manifest:
    def req(key: str) -> Any:
        if key not in data:
            raise SchemaError(
                f"package.json is missing required field '{key}'",
                file=file,
                field_name=key,
                reason="missing_required",
            )
        return data[key]

    name = req("name")
    if not isinstance(name, str) or not is_scoped_name(name):
        raise SchemaError(
            f"package.json 'name' must match '@<scope>/<name>' grammar, got {name!r}",
            file=file,
            field_name="name",
            reason="invalid_name",
        )
    version = req("version")
    if not isinstance(version, str) or not is_semver(version):
        raise SchemaError(
            f"package.json 'version' must be strict SemVer 2.0.0, got {version!r}",
            file=file,
            field_name="version",
            reason="invalid_version",
        )
    description = data.get("description")
    if description is not None and not isinstance(description, str):
        raise SchemaError(
            "package.json 'description' must be a string",
            file=file,
            field_name="description",
            reason="invalid_description",
        )
    license_ = data.get("license")
    if license_ is not None and not isinstance(license_, str):
        raise SchemaError(
            "package.json 'license' must be a string",
            file=file,
            field_name="license",
            reason="invalid_license",
        )
    dependencies_raw = data.get("dependencies", {})
    if not isinstance(dependencies_raw, dict):
        raise SchemaError(
            "package.json 'dependencies' must be an object",
            file=file,
            field_name="dependencies",
            reason="invalid_dependencies",
        )
    dependencies: dict[str, str] = {}
    for k, v in dependencies_raw.items():
        if not isinstance(k, str) or not is_scoped_name(k):
            raise SchemaError(
                f"dependencies key '{k}' is not a valid scoped name",
                file=file,
                field_name=f"dependencies.{k}",
                reason="invalid_dep_name",
            )
        if not isinstance(v, str) or not is_semver(v):
            raise SchemaError(
                f"dependencies['{k}'] must be strict SemVer, got {v!r}",
                file=file,
                field_name=f"dependencies.{k}",
                reason="invalid_dep_version",
            )
        dependencies[k] = v

    prompts_raw = req("prompts")
    if not isinstance(prompts_raw, list):
        raise SchemaError(
            "package.json 'prompts' must be an array",
            file=file,
            field_name="prompts",
            reason="invalid_prompts",
        )
    if len(prompts_raw) == 0:
        raise SchemaError(
            "package.json 'prompts' array must not be empty",
            file=file,
            field_name="prompts",
            reason="empty_prompts",
        )

    seen_ids: set[str] = set()
    seen_paths: set[str] = set()
    entries: list[PromptEntry] = []
    for idx, entry in enumerate(prompts_raw):
        handle = _entry_handle(entry, idx)
        if not isinstance(entry, dict):
            raise SchemaError(
                f"{handle} must be an object",
                file=file,
                field_name=handle,
                reason="invalid_entry",
            )
        path = entry.get("path")
        if not isinstance(path, str) or not path:
            raise SchemaError(
                f"{handle}.path must be a non-empty string",
                file=file,
                field_name=f"{handle}.path",
                reason="invalid_path",
            )
        if not _ASCII_PRINTABLE_RE.match(path):
            raise SchemaError(
                f"prompt entry path={path!r} contains non-printable-ASCII characters",
                file=file,
                field_name=f"prompts[path={path!r}].path",
                reason="non_ascii_path",
            )
        case_key = path.casefold()
        for existing in seen_paths:
            if existing.casefold() == case_key:
                raise SchemaError(
                    f"prompt entry path={path!r} collides with path={existing!r} under case-folding",
                    file=file,
                    field_name=f"prompts[path={path!r}].path",
                    reason="path_case_collision",
                )
        seen_paths.add(path)

        pid = entry.get("id")
        if pid is None:
            pid = _derive_default_id(path, file=file)
        elif not isinstance(pid, str) or not _ID_RE.match(pid):
            raise SchemaError(
                f"prompt entry path={path!r} has id {pid!r} which does not match '[a-z0-9][a-z0-9_-]*'",
                file=file,
                field_name=f"prompts[path={path!r}].id",
                reason="invalid_id",
            )
        if pid in seen_ids:
            raise SchemaError(
                f"duplicate prompt id {pid!r} in manifest (for path={path!r})",
                file=file,
                field_name=f"prompts[path={path!r}].id",
                reason="duplicate_id",
            )
        seen_ids.add(pid)

        content_type = entry.get("contentType", "yaml")
        if content_type not in ("yaml", "json"):
            raise SchemaError(
                f"prompt entry path={path!r} has contentType {content_type!r}; must be 'yaml' or 'json'",
                file=file,
                field_name=f"prompts[path={path!r}].contentType",
                reason="invalid_content_type",
            )
        entries.append(PromptEntry(id=pid, path=path, contentType=content_type))

    return Manifest(
        name=name,
        version=version,
        description=description,
        license=license_,
        dependencies=dependencies,
        prompts=entries,
    )
