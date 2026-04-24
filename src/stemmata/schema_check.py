from __future__ import annotations

import hashlib
import json
import os
import re
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import jsonschema
from jsonschema import Draft202012Validator

from stemmata.cache import default_cache_dir
from stemmata.errors import (
    NetworkError,
    OfflineError,
    PromptCliError,
    SchemaError,
)


def resolve_schema_uri(schema_uri: str, source_file: str) -> str:
    """Resolve *schema_uri* to a form ``_fetch_schema`` can consume.

    URIs with a scheme (``http://``, ``https://``, ``file://``) pass through
    unchanged.  Bare filesystem paths are resolved relative to the directory
    of *source_file* and returned as absolute paths.
    """
    if "://" in schema_uri:
        return schema_uri
    base = os.path.dirname(os.path.abspath(source_file))
    return os.path.normpath(os.path.join(base, schema_uri))


@dataclass
class SchemaCheckOptions:
    offline: bool = False
    refresh: bool = False
    http_timeout: float = 30.0
    cache_root: Path | None = None
    stderr: Any = None


def _schema_cache_dir(opts: SchemaCheckOptions) -> Path:
    root = opts.cache_root or default_cache_dir()
    return root / "schemas"


def _schema_cache_path(uri: str, opts: SchemaCheckOptions) -> Path:
    digest = hashlib.sha256(uri.encode("utf-8")).hexdigest()
    return _schema_cache_dir(opts) / f"{digest}.json"


def _fetch_schema(uri: str, opts: SchemaCheckOptions) -> dict[str, Any]:
    cache_path = _schema_cache_path(uri, opts)
    if cache_path.exists() and not opts.refresh:
        try:
            return json.loads(cache_path.read_text(encoding="utf-8"))
        except Exception:
            cache_path.unlink(missing_ok=True)

    if uri.startswith("file://"):
        try:
            raw = Path(urllib.request.url2pathname(uri[len("file://"):])).read_bytes()
        except OSError as e:
            raise SchemaError(f"cannot read local $schema {uri!r}: {e}",
                              file=uri, field_name="$schema", reason="schema_file_not_found")
    elif uri.startswith("http://") or uri.startswith("https://"):
        if opts.offline:
            raise OfflineError(uri)
        req = urllib.request.Request(uri, headers={"Accept": "application/schema+json, application/json"})
        try:
            with urllib.request.urlopen(req, timeout=opts.http_timeout) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as e:
            raise NetworkError(uri, e.code, f"HTTP {e.code}: {e.reason}")
        except urllib.error.URLError as e:
            raise NetworkError(uri, None, str(e.reason))
        except TimeoutError:
            raise NetworkError(uri, None, "request timed out")
    else:
        # Bare filesystem path (already resolved to absolute by resolve_schema_uri).
        p = Path(uri)
        if not p.is_file():
            raise SchemaError(f"$schema file not found: {uri!r}",
                              file=uri, field_name="$schema", reason="schema_file_not_found")
        try:
            raw = p.read_bytes()
        except OSError as e:
            raise SchemaError(f"cannot read $schema {uri!r}: {e}",
                              file=uri, field_name="$schema", reason="schema_file_not_found")

    try:
        schema = json.loads(raw)
    except json.JSONDecodeError as e:
        raise NetworkError(uri, None, f"invalid JSON schema: {e}")

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(schema, ensure_ascii=False), encoding="utf-8")
    return schema


def _lookup_line(instance: Any, path_parts: list[str | int]) -> int | None:
    """Walk *instance* along *path_parts* and return the source line number.

    String scalars carry ``_pcli_line`` from the YAML loader.  For non-string
    scalars (int, bool, etc.) we fall back to the YAML key's line, since dict
    keys are also ``_ScalarStr`` in YAML-loaded trees.
    """
    cur: Any = instance
    last_key_line: int | None = None
    for segment in path_parts:
        if isinstance(cur, dict):
            for k in cur:
                if k == segment:
                    kl = getattr(k, "_pcli_line", None)
                    if kl is not None:
                        last_key_line = kl
                    break
            if segment in cur:
                cur = cur[segment]
            else:
                return last_key_line
        elif isinstance(cur, list) and isinstance(segment, int) and 0 <= segment < len(cur):
            cur = cur[segment]
        else:
            return last_key_line
    return getattr(cur, "_pcli_line", None) or last_key_line


def _json_key_line(text: str, dotted_field: str) -> int | None:
    """Best-effort line number for a dotted field in raw JSON text."""
    pos = 0
    for segment in dotted_field.split("."):
        if segment.isdigit():
            continue
        m = re.search(r'"' + re.escape(segment) + r'"\s*:', text[pos:])
        if m is None:
            break
        pos += m.start()
    else:
        return text[:pos].count("\n") + 1 if pos > 0 else None
    return text[:pos].count("\n") + 1 if pos > 0 else None


def validate_against_schema(
    instance: Any,
    schema_uri: str,
    *,
    file: str,
    opts: SchemaCheckOptions,
    position_instance: Any | None = None,
) -> list[PromptCliError]:
    """Validate ``instance`` against the JSON Schema at ``schema_uri``.

    If *position_instance* is provided it is used for line-number lookup
    (useful when *instance* has been through interpolation and lost the
    ``_ScalarStr`` wrappers — pass the pre-interpolation tree instead).
    """
    try:
        schema = _fetch_schema(schema_uri, opts)
    except OfflineError:
        return [SchemaError(
            f"--offline: cannot fetch $schema {schema_uri!r} and no cached copy is available",
            file=file,
            field_name="$schema",
            reason="schema_unavailable_offline",
        )]
    except NetworkError as e:
        return [SchemaError(
            f"failed to fetch $schema {schema_uri!r}: {e.message}",
            file=file,
            field_name="$schema",
            reason="schema_fetch_failed",
        )]
    except SchemaError as e:
        return [SchemaError(
            e.message,
            file=file,
            field_name=e.details.get("field") or "$schema",
            reason=e.details.get("reason"),
        )]

    try:
        validator = Draft202012Validator(schema)
    except jsonschema.exceptions.SchemaError as e:
        return [SchemaError(
            f"$schema {schema_uri!r} is not a valid JSON Schema: {e.message}",
            file=file,
            field_name="$schema",
            reason="invalid_schema_document",
        )]

    pos_src = position_instance if position_instance is not None else instance
    errors: list[PromptCliError] = []
    for verr in sorted(validator.iter_errors(instance), key=lambda e: list(e.absolute_path)):
        path = ".".join(str(p) for p in verr.absolute_path) or "<root>"
        line = _lookup_line(pos_src, list(verr.absolute_path))
        errors.append(SchemaError(
            f"$schema validation failed at {path}: {verr.message}",
            file=file,
            line=line,
            field_name=path,
            reason="schema_validation_failed",
        ))
    return errors
