from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from stemmata.errors import SchemaError, UsageError
from stemmata.manifest import _derive_default_id


_YAML_EXTS = {".yaml", ".yml"}
_JSON_EXTS = {".json"}
_MARKDOWN_EXTS = {".md"}


@dataclass
class InitResult:
    manifest_path: str
    created: bool
    name: str
    version: str
    license: str | None
    prompts: list[dict[str, Any]] = field(default_factory=list)
    resources: list[dict[str, Any]] = field(default_factory=list)


def _content_type_for(ext: str, *, kind: str) -> str | None:
    if kind == "prompt":
        if ext in _YAML_EXTS:
            return "yaml"
        if ext in _JSON_EXTS:
            return "json"
        return None
    if ext in _MARKDOWN_EXTS:
        return "markdown"
    return None


def _scan_folder(folder: Path, kind: str, *, manifest_file: str) -> list[dict[str, str]]:
    if not folder.is_dir():
        return []
    prefix = folder.name
    out: list[dict[str, str]] = []
    for p in sorted(folder.rglob("*")):
        if not p.is_file():
            continue
        content_type = _content_type_for(p.suffix.lower(), kind=kind)
        if content_type is None:
            continue
        rel_parts = p.relative_to(folder).parts
        rel = "/".join((prefix, *rel_parts))
        entry_id = _derive_default_id(rel, file=manifest_file)
        out.append({"id": entry_id, "path": rel, "contentType": content_type})
    return out


def _render_entry_array(entries: list[dict[str, Any]]) -> str:
    if not entries:
        return "[]"
    first_keys = list(entries[0].keys())
    uniform = all(list(e.keys()) == first_keys for e in entries)
    if uniform and first_keys:
        segments_per_entry = [
            [f"{json.dumps(k)}: {json.dumps(e[k], ensure_ascii=False)}" for k in first_keys]
            for e in entries
        ]
        widths = [
            max(len(segs[i]) for segs in segments_per_entry)
            for i in range(len(first_keys))
        ]
        lines: list[str] = []
        for segs in segments_per_entry:
            padded = [(segs[i] + ",").ljust(widths[i] + 1) for i in range(len(segs) - 1)]
            padded.append(segs[-1])
            lines.append("    { " + " ".join(padded) + " }")
    else:
        lines = [
            "    { " + ", ".join(
                f"{json.dumps(k)}: {json.dumps(v, ensure_ascii=False)}"
                for k, v in e.items()
            ) + " }"
            for e in entries
        ]
    return "[\n" + ",\n".join(lines) + "\n  ]"


def _dump_manifest(manifest: dict[str, Any]) -> str:
    out: list[str] = ["{\n"]
    keys = list(manifest.keys())
    for i, key in enumerate(keys):
        value = manifest[key]
        trailer = "," if i < len(keys) - 1 else ""
        if key in ("prompts", "resources") and isinstance(value, list):
            body = _render_entry_array(value)
        else:
            serialized = json.dumps(value, indent=2, ensure_ascii=False)
            body = serialized.replace("\n", "\n  ")
        out.append(f"  {json.dumps(key)}: {body}{trailer}\n")
    out.append("}\n")
    return "".join(out)


def _merge_entries(existing: Any, scanned: list[dict[str, str]]) -> list[dict[str, Any]]:
    seen_paths: dict[str, dict[str, Any]] = {}
    if isinstance(existing, list):
        for entry in existing:
            if not isinstance(entry, dict):
                continue
            path = entry.get("path")
            if not isinstance(path, str) or not path:
                continue
            key = path.casefold()
            if key in seen_paths:
                continue
            seen_paths[key] = dict(entry)
    for entry in scanned:
        key = entry["path"].casefold()
        if key in seen_paths:
            continue
        seen_paths[key] = dict(entry)
    return sorted(seen_paths.values(), key=lambda e: e["path"])


def run_init(path: Path) -> InitResult:
    base = path.resolve()
    if not base.is_dir():
        raise UsageError(
            f"init target {str(path)!r} is not a directory",
            argument="path",
            reason="not_a_directory",
        )
    manifest_file = base / "package.json"
    pre_existed = manifest_file.is_file()

    existing: dict[str, Any] = {}
    if pre_existed:
        raw = manifest_file.read_text(encoding="utf-8")
        try:
            existing = json.loads(raw)
        except json.JSONDecodeError as e:
            raise SchemaError(
                f"existing package.json at {manifest_file} is not valid JSON: {e.msg}",
                file=str(manifest_file),
                line=e.lineno,
                column=e.colno,
                field_name="<json>",
                reason="invalid_json",
            )
        if not isinstance(existing, dict):
            raise SchemaError(
                f"existing package.json at {manifest_file} must be a JSON object",
                file=str(manifest_file),
                field_name="<root>",
                reason="not_object",
            )

    existing_name = existing.get("name")
    name = existing_name if isinstance(existing_name, str) and existing_name else base.name
    existing_version = existing.get("version")
    version = existing_version if isinstance(existing_version, str) and existing_version else "0.0.1.dev0"
    existing_license = existing.get("license")
    license_ = existing_license if isinstance(existing_license, str) and existing_license else "Apache-2.0"

    scanned_prompts = _scan_folder(base / "prompts", "prompt", manifest_file=str(manifest_file))
    scanned_resources = _scan_folder(base / "resources", "resource", manifest_file=str(manifest_file))

    prompts = _merge_entries(existing.get("prompts"), scanned_prompts)
    resources = _merge_entries(existing.get("resources"), scanned_resources)

    manifest: dict[str, Any] = {"name": name, "version": version, "license": license_}
    for key, value in existing.items():
        if key in ("name", "version", "license", "prompts", "resources"):
            continue
        manifest[key] = value
    manifest["prompts"] = prompts
    if resources:
        manifest["resources"] = resources

    manifest_file.write_text(_dump_manifest(manifest), encoding="utf-8")

    return InitResult(
        manifest_path=str(manifest_file),
        created=not pre_existed,
        name=name,
        version=version,
        license=license_,
        prompts=prompts,
        resources=resources,
    )
