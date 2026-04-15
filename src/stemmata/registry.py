from __future__ import annotations

import base64
import hashlib
import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

from stemmata.errors import NetworkError, OfflineError
from stemmata.npmrc import AuthMaterial, NpmConfig


@dataclass
class RegistryClient:
    config: NpmConfig
    offline: bool = False
    http_timeout: float = 30.0
    opener: urllib.request.OpenerDirector | None = None

    def _opener(self) -> urllib.request.OpenerDirector:
        if self.opener is None:
            self.opener = urllib.request.build_opener()
        return self.opener

    def _auth_headers(self, url: str) -> dict[str, str]:
        auth: AuthMaterial = self.config.auth_for_url(url)
        if auth.auth_token:
            return {"Authorization": f"Bearer {auth.auth_token}"}
        if auth.auth_basic:
            return {"Authorization": f"Basic {auth.auth_basic}"}
        if auth.username and auth.password_b64:
            try:
                password = base64.b64decode(auth.password_b64).decode("utf-8")
            except Exception:
                password = auth.password_b64
            blob = base64.b64encode(f"{auth.username}:{password}".encode()).decode()
            return {"Authorization": f"Basic {blob}"}
        return {}

    def _fetch(self, url: str, *, headers: dict[str, str] | None = None, accept: str = "application/json") -> bytes:
        if self.offline:
            raise OfflineError(url)
        req_headers = {"Accept": accept}
        req_headers.update(self._auth_headers(url))
        if headers:
            req_headers.update(headers)
        req = urllib.request.Request(url, headers=req_headers)
        try:
            with self._opener().open(req, timeout=self.http_timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            raise NetworkError(url, e.code, f"HTTP {e.code}: {e.reason}")
        except urllib.error.URLError as e:
            raise NetworkError(url, None, str(e.reason))
        except TimeoutError:
            raise NetworkError(url, None, "request timed out")

    def _put_json(self, url: str, body: bytes, *, headers: dict[str, str] | None = None) -> bytes:
        if self.offline:
            raise OfflineError(url)
        req_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Content-Length": str(len(body)),
        }
        req_headers.update(self._auth_headers(url))
        if headers:
            req_headers.update(headers)
        req = urllib.request.Request(url, data=body, method="PUT", headers=req_headers)
        try:
            with self._opener().open(req, timeout=self.http_timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            try:
                payload = e.read().decode("utf-8", errors="replace")
            except Exception:
                payload = ""
            reason = f"HTTP {e.code}: {e.reason}"
            if payload:
                reason = f"{reason} ({payload[:300]})"
            raise NetworkError(url, e.code, reason)
        except urllib.error.URLError as e:
            raise NetworkError(url, None, str(e.reason))
        except TimeoutError:
            raise NetworkError(url, None, "request timed out")

    def registry_for_package(self, name: str) -> str:
        scope = name.split("/", 1)[0] if name.startswith("@") else ""
        reg = self.config.registry_for_scope(scope)
        if not reg:
            raise NetworkError(
                "<registry>",
                None,
                f"no registry configured for scope {scope!r}; configure 'registry=' or '{scope}:registry=' in ~/.npmrc",
            )
        return reg.rstrip("/") + "/"

    def fetch_tarball(self, name: str, version: str) -> tuple[str, bytes]:
        registry = self.registry_for_package(name)
        scope, simple = _split_name(name)
        filename = f"{simple}-{version}.tgz"
        url = f"{registry}{name}/-/{filename}"
        data = self._fetch(url, accept="application/octet-stream")
        self._verify_integrity(name, version, data)
        return url, data

    def _verify_integrity(self, name: str, version: str, data: bytes) -> None:
        try:
            meta = self.fetch_metadata(name)
        except NetworkError:
            return
        versions = meta.get("versions", {})
        ver_meta = versions.get(version, {})
        dist = ver_meta.get("dist", {})
        integrity = dist.get("integrity")
        if integrity:
            if integrity.startswith("sha512-"):
                expected = integrity[len("sha512-"):]
                actual = base64.b64encode(hashlib.sha512(data).digest()).decode()
                if actual != expected:
                    raise NetworkError(
                        f"{name}@{version}",
                        None,
                        f"integrity check failed: expected sha512-{expected}, got sha512-{actual}",
                    )
                return
            if integrity.startswith("sha1-"):
                expected = integrity[len("sha1-"):]
                actual = base64.b64encode(hashlib.sha1(data).digest()).decode()
                if actual != expected:
                    raise NetworkError(
                        f"{name}@{version}",
                        None,
                        f"integrity check failed: expected sha1-{expected}, got sha1-{actual}",
                    )
                return
        shasum = dist.get("shasum")
        if shasum:
            actual_sha1 = hashlib.sha1(data).hexdigest()
            if actual_sha1 != shasum:
                raise NetworkError(
                    f"{name}@{version}",
                    None,
                    f"shasum check failed: expected {shasum}, got {actual_sha1}",
                )

    def publish_tarball(
        self,
        name: str,
        version: str,
        tarball: bytes,
        *,
        manifest: dict[str, Any],
    ) -> tuple[str, bytes]:
        """PUT an npm publish envelope to the registry.

        ``manifest`` is the contents of ``package.json`` (already validated).
        Returns the (url, response_body) tuple. Raises ``NetworkError`` on
        non-2xx responses or transport failures.
        """
        import base64

        registry = self.registry_for_package(name)
        scope, simple = _split_name(name)
        filename = f"{simple}-{version}.tgz"
        tarball_url = f"{registry}{name}/-/{filename}"
        integrity = "sha512-" + base64.b64encode(hashlib.sha512(tarball).digest()).decode("ascii")
        shasum = hashlib.sha1(tarball).hexdigest()

        version_meta: dict[str, Any] = dict(manifest)
        version_meta["name"] = name
        version_meta["version"] = version
        version_meta["_id"] = f"{name}@{version}"
        version_meta["dist"] = {
            "shasum": shasum,
            "integrity": integrity,
            "tarball": tarball_url,
        }

        envelope = {
            "_id": name,
            "name": name,
            "description": manifest.get("description", ""),
            "dist-tags": {"latest": version},
            "versions": {version: version_meta},
            "_attachments": {
                filename: {
                    "content_type": "application/octet-stream",
                    "data": base64.b64encode(tarball).decode("ascii"),
                    "length": len(tarball),
                }
            },
        }

        body = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
        url = f"{registry}{urllib.parse.quote(name, safe='@/')}"
        response = self._put_json(url, body)
        return url, response

    def fetch_metadata(self, name: str) -> dict[str, Any]:
        registry = self.registry_for_package(name)
        url = f"{registry}{urllib.parse.quote(name, safe='@/')}"
        raw = self._fetch(url)
        try:
            return json.loads(raw)
        except json.JSONDecodeError as e:
            raise NetworkError(url, None, f"invalid JSON metadata: {e}")


def _split_name(name: str) -> tuple[str, str]:
    if name.startswith("@") and "/" in name:
        scope, simple = name.split("/", 1)
        return scope, simple
    return "", name
