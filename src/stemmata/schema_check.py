from __future__ import annotations

import hashlib
import json
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from stemmata.cache import default_cache_dir
from stemmata.errors import (
    NetworkError,
    OfflineError,
    PromptCliError,
    SchemaError,
)


def _have_jsonschema() -> bool:
    try:
        import jsonschema  # noqa: F401
        return True
    except Exception:
        return False


@dataclass
class SchemaCheckOptions:
    offline: bool = False
    strict: bool = False
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

    if opts.offline:
        raise OfflineError(uri)

    if not (uri.startswith("http://") or uri.startswith("https://")):
        raise SchemaError(
            f"$schema URI {uri!r} is not http(s); only http/https schema URIs can be fetched",
            file=uri,
            field_name="$schema",
            reason="unsupported_schema_uri_scheme",
        )

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

    try:
        schema = json.loads(raw)
    except json.JSONDecodeError as e:
        raise NetworkError(uri, None, f"invalid JSON schema: {e}")

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(schema, ensure_ascii=False), encoding="utf-8")
    return schema


def _warn(stderr: Any, message: str) -> None:
    stream = stderr if stderr is not None else sys.stderr
    stream.write(f"warning: {message}\n")


def validate_against_schema(
    instance: Any,
    schema_uri: str,
    *,
    file: str,
    opts: SchemaCheckOptions,
) -> list[PromptCliError]:
    """Validate ``instance`` against the JSON Schema at ``schema_uri``.

    Returns a list of ``SchemaError``s describing each validation failure.
    Behaviour under unusual conditions:

    - If ``jsonschema`` is not installed and ``opts.strict`` is true, returns
      a single ``SchemaError`` instructing the user to install the publish
      extra. If not strict, returns ``[]`` after writing a warning to stderr.
    - If ``opts.offline`` is true and the schema is not cached, behaves
      analogously: error in strict mode, warning otherwise.
    """
    if not _have_jsonschema():
        msg = (
            "jsonschema is not installed; install with `pip install stemmata[publish]` "
            "to enable $schema enforcement"
        )
        if opts.strict:
            return [SchemaError(msg, file=file, field_name="$schema", reason="jsonschema_missing")]
        _warn(opts.stderr, msg)
        return []

    try:
        schema = _fetch_schema(schema_uri, opts)
    except OfflineError as e:
        if opts.strict:
            return [SchemaError(
                f"--offline: cannot fetch $schema {schema_uri!r} and no cached copy is available",
                file=file,
                field_name="$schema",
                reason="schema_unavailable_offline",
            )]
        _warn(opts.stderr, f"skipping $schema {schema_uri!r}: offline and not cached")
        return []
    except NetworkError as e:
        if opts.strict:
            return [SchemaError(
                f"failed to fetch $schema {schema_uri!r}: {e.message}",
                file=file,
                field_name="$schema",
                reason="schema_fetch_failed",
            )]
        _warn(opts.stderr, f"skipping $schema {schema_uri!r}: {e.message}")
        return []
    except SchemaError as e:
        return [e]

    import jsonschema  # type: ignore[import-not-found]
    from jsonschema import Draft202012Validator  # type: ignore[import-not-found]

    try:
        validator = Draft202012Validator(schema)
    except jsonschema.exceptions.SchemaError as e:  # type: ignore[attr-defined]
        return [SchemaError(
            f"$schema {schema_uri!r} is not a valid JSON Schema: {e.message}",
            file=file,
            field_name="$schema",
            reason="invalid_schema_document",
        )]

    errors: list[PromptCliError] = []
    for verr in sorted(validator.iter_errors(instance), key=lambda e: list(e.absolute_path)):
        path = ".".join(str(p) for p in verr.absolute_path) or "<root>"
        errors.append(SchemaError(
            f"$schema validation failed at {path}: {verr.message}",
            file=file,
            field_name=path,
            reason="schema_validation_failed",
        ))
    return errors
