from __future__ import annotations

import json
from typing import Any

import yaml

from stemmata.errors import CATEGORIES, PromptCliError


def success(command: str, result: Any) -> dict[str, Any]:
    return {
        "status": "ok",
        "exit_code": 0,
        "command": command,
        "result": result,
        "error": None,
    }


def failure(command: str, err: PromptCliError) -> dict[str, Any]:
    return {
        "status": "error",
        "exit_code": err.code,
        "command": command,
        "result": None,
        "error": {
            "code": err.code,
            "category": CATEGORIES.get(err.code, "internal_error"),
            "message": err.message,
            "location": err.location,
            "details": err.details,
        },
    }


def to_yaml(envelope: dict[str, Any]) -> str:
    return yaml.dump(
        envelope,
        Dumper=yaml.SafeDumper,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
    )


def to_json(envelope: dict[str, Any]) -> str:
    return json.dumps(envelope, indent=2, sort_keys=False, ensure_ascii=False)


def to_text(envelope: dict[str, Any]) -> str:
    if envelope["status"] == "ok":
        result = envelope["result"]
        if result is None:
            return f"{envelope['command']}: ok"
        return f"{envelope['command']}: ok\n{json.dumps(result, indent=2)}"
    err = envelope["error"]
    loc = err.get("location")
    loc_str = ""
    if isinstance(loc, dict) and loc.get("file"):
        loc_str = f" at {loc['file']}:{loc.get('line') or '?'}"
    return f"error[{err['code']}] {err['category']}{loc_str}: {err['message']}"
