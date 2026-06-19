from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import urlsplit

from stemmata.errors import ConfigError, SchemaError


_VAR_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_DOUBLE_DOLLAR = "\x00NPMRC_DOLLAR\x00"


def _substitute_vars(value: str, env: dict[str, str], *, file: str) -> str:
    raw = value.replace("$$", _DOUBLE_DOLLAR)

    def repl(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in env:
            raise SchemaError(
                f"undefined environment variable ${{{name}}} in {file}",
                file=file,
                field_name=name,
                reason="undefined_env_var",
            )
        return env[name]

    out = _VAR_RE.sub(repl, raw)
    return out.replace(_DOUBLE_DOLLAR, "$")


def _strip_quotes(v: str) -> str:
    if len(v) >= 2 and ((v[0] == v[-1] == '"') or (v[0] == v[-1] == "'")):
        return v[1:-1]
    return v


def parse_npmrc(text: str, env: dict[str, str] | None = None, *, file: str = "~/.npmrc") -> dict[str, str]:
    env = env if env is not None else dict(os.environ)
    result: dict[str, str] = {}
    text = text.lstrip("\ufeff")
    for raw_line in text.splitlines():
        line = raw_line.rstrip("\r")
        stripped = line.lstrip()
        if not stripped or stripped.startswith("#") or stripped.startswith(";"):
            continue
        if "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        key = key.strip()
        value = value.strip()
        for comment_ch in ("#", ";"):
            if comment_ch in value and not (value.startswith('"') or value.startswith("'")):
                value = value.split(comment_ch, 1)[0].rstrip()
        value = _strip_quotes(value)
        value = _substitute_vars(value, env, file=file)
        result[key] = value
    return result


@dataclass
class AuthMaterial:
    auth_token: str | None = None
    auth_basic: str | None = None
    username: str | None = None
    password_b64: str | None = None
    always_auth: bool = False


@dataclass
class NpmConfig:
    entries: dict[str, str]

    def default_registry(self) -> str | None:
        return self.entries.get("registry")

    def scope_registry(self, scope: str) -> str | None:
        if not scope.startswith("@"):
            scope = "@" + scope
        return self.entries.get(f"{scope}:registry")

    def registry_for_scope(self, scope: str) -> str | None:
        return self.scope_registry(scope) or self.default_registry()

    def auth_for_url(self, url: str) -> AuthMaterial:
        canon = _canonicalize_url(url)
        candidates: list[tuple[int, str, str]] = []
        for key, value in self.entries.items():
            if not key.startswith("//"):
                continue
            if ":" not in key:
                continue
            prefix, _, suffix = key.rpartition(":")
            if suffix not in {"_authToken", "_auth", "username", "_password", "always-auth"}:
                continue
            key_canon = _canonicalize_prefix(prefix)
            if canon.startswith(key_canon):
                candidates.append((len(key_canon), suffix, value))
        candidates.sort(key=lambda t: t[0], reverse=True)

        auth = AuthMaterial()
        seen_longest: int | None = None
        for length, suffix, value in candidates:
            if seen_longest is None:
                seen_longest = length
            if length != seen_longest:
                break
            if suffix == "_authToken" and auth.auth_token is None:
                auth.auth_token = value
            elif suffix == "_auth" and auth.auth_basic is None:
                auth.auth_basic = value
            elif suffix == "username" and auth.username is None:
                auth.username = value
            elif suffix == "_password" and auth.password_b64 is None:
                auth.password_b64 = value
            elif suffix == "always-auth":
                auth.always_auth = value.lower() in {"true", "1", "yes"}
        return auth


def _canonicalize_url(url: str) -> str:
    parts = urlsplit(url)
    host = parts.hostname or ""
    path = parts.path or ""
    if parts.port:
        host = f"{host}:{parts.port}"
    path = path.rstrip("/")
    return f"//{host.lower()}{path}"


def _canonicalize_prefix(prefix: str) -> str:
    if not prefix.startswith("//"):
        return prefix
    rest = prefix[2:]
    if "/" in rest:
        host, _, path = rest.partition("/")
        path = "/" + path
    else:
        host, path = rest, ""
    path = path.rstrip("/")
    return f"//{host.lower()}{path}"


def _resolve_npmrc_path(explicit: Path | None, env: dict[str, str]) -> Path | None:
    """Resolve which npmrc file to load via a three-tier precedence.

    1. An explicit ``--npmrc`` path is authoritative: if it does not point to an
       existing file the command hard-fails (``ConfigError``).
    2. ``NPM_CONFIG_USERCONFIG`` is honoured leniently for CI/CD: ``~`` is
       expanded and, following npm, an empty/whitespace value means "no
       userconfig" (treated as unset). If it names a real file it wins; if it is
       set but missing we skip to the final fallback rather than failing.
    3. ``~/.npmrc`` is the final fallback.

    Returns ``None`` when no file exists at the resolved fallback, so the caller
    yields an empty config (preserving the prior no-npmrc behaviour).
    """
    if explicit is not None:
        explicit = Path(os.path.expanduser(str(explicit)))
        if not explicit.exists():
            raise ConfigError(
                f"npmrc config file not found: {explicit}",
                path=str(explicit),
                reason="explicit_not_found",
            )
        return explicit

    raw = env.get("NPM_CONFIG_USERCONFIG")
    if raw is not None and raw.strip() != "":
        candidate = Path(os.path.expanduser(raw))
        if candidate.exists():
            return candidate
        # Set but missing: lenient skip to the ~/.npmrc fallback.

    fallback = Path.home() / ".npmrc"
    return fallback if fallback.exists() else None


def load_npmrc(path: Path | None = None, env: dict[str, str] | None = None) -> NpmConfig:
    resolved_env = env if env is not None else dict(os.environ)
    resolved = _resolve_npmrc_path(path, resolved_env)
    if resolved is None:
        return NpmConfig(entries={})
    text = resolved.read_text(encoding="utf-8")
    entries = parse_npmrc(text, env=resolved_env, file=str(resolved))
    return NpmConfig(entries=entries)
