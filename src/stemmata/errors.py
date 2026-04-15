from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


EXIT_OK = 0
EXIT_GENERIC = 1
EXIT_USAGE = 2
EXIT_SCHEMA = 10
EXIT_REFERENCE = 11
EXIT_CYCLE = 12
EXIT_UNRESOLVABLE = 14
EXIT_MERGE = 15
EXIT_NETWORK = 20
EXIT_CACHE = 21
EXIT_OFFLINE = 22


CATEGORIES = {
    EXIT_GENERIC: "internal_error",
    EXIT_USAGE: "usage_error",
    EXIT_SCHEMA: "schema_validation",
    EXIT_REFERENCE: "reference_error",
    EXIT_CYCLE: "cycle_detected",
    EXIT_UNRESOLVABLE: "unresolvable_placeholder",
    EXIT_MERGE: "merge_failure",
    EXIT_NETWORK: "network_error",
    EXIT_CACHE: "cache_error",
    EXIT_OFFLINE: "offline_violation",
}


@dataclass
class PromptCliError(Exception):
    code: int
    message: str
    location: Any = None
    details: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        return self.message


class UsageError(PromptCliError):
    def __init__(self, message: str, argument: str | None = None, reason: str | None = None):
        super().__init__(
            EXIT_USAGE,
            message,
            None,
            {"argument": argument or "", "reason": reason or message},
        )


class SchemaError(PromptCliError):
    def __init__(self, message: str, *, file: str | None, line: int | None = None, column: int | None = None, field_name: str = "", reason: str | None = None):
        super().__init__(
            EXIT_SCHEMA,
            message,
            {"file": file, "line": line, "column": column} if file is not None else None,
            {"field": field_name, "reason": reason or message},
        )


class ReferenceError_(PromptCliError):
    def __init__(self, message: str, *, file: str | None, line: int | None, column: int | None, reference: str, searched_in: str):
        super().__init__(
            EXIT_REFERENCE,
            message,
            {"file": file, "line": line, "column": column},
            {"reference": reference, "searched_in": searched_in},
        )


class CycleError(PromptCliError):
    def __init__(self, nodes: list[dict[str, Any]], cycle_ids: list[str]):
        super().__init__(
            EXIT_CYCLE,
            f"Cycle detected: {' -> '.join(cycle_ids) if cycle_ids else ''}",
            nodes,
            {"cycle": cycle_ids},
        )


class UnresolvableError(PromptCliError):
    def __init__(self, placeholder: str, *, file: str | None, line: int | None, column: int | None, reason: str, ancestors_searched: list[str], providing_ancestor: str | None):
        super().__init__(
            EXIT_UNRESOLVABLE,
            f"Placeholder ${{{placeholder}}} in {file}:{line} could not be resolved ({reason})",
            {"file": file, "line": line, "column": column},
            {
                "reason": reason,
                "placeholder": placeholder,
                "ancestors_searched": ancestors_searched,
                "providing_ancestor": providing_ancestor,
            },
        )


class MergeError(PromptCliError):
    def __init__(self, path: str, conflict: str, types: list[str], nodes: list[dict[str, Any]]):
        super().__init__(
            EXIT_MERGE,
            f"Merge failure at '{path}': {conflict} ({', '.join(types)})",
            nodes,
            {"path": path, "conflict": conflict, "types": types},
        )


class NetworkError(PromptCliError):
    def __init__(self, url: str, http_status: int | None, reason: str):
        super().__init__(
            EXIT_NETWORK,
            f"Network error fetching {url}: {reason}",
            None,
            {"url": url, "http_status": http_status, "reason": reason},
        )


class CacheError(PromptCliError):
    def __init__(self, cache_path: str, reason: str):
        super().__init__(
            EXIT_CACHE,
            f"Cache error at {cache_path}: {reason}",
            None,
            {"cache_path": cache_path, "reason": reason},
        )


class OfflineError(PromptCliError):
    def __init__(self, url: str):
        super().__init__(
            EXIT_OFFLINE,
            f"Offline mode: refusing to fetch {url}",
            None,
            {"url": url},
        )


class GenericError(PromptCliError):
    def __init__(self, message: str, exception: str = "", traceback: str = ""):
        super().__init__(
            EXIT_GENERIC,
            message,
            None,
            {"exception": exception, "traceback": traceback},
        )


# Severity ordering for AggregatedError: lower index wins as the headline code.
# Cycles are most fatal (graph cannot be reasoned about), then schema/manifest
# problems, then missing references, then merge type conflicts, then placeholder
# resolution failures, then transport-layer errors, then generic.
_AGG_PRIORITY = [
    EXIT_CYCLE,
    EXIT_SCHEMA,
    EXIT_REFERENCE,
    EXIT_MERGE,
    EXIT_UNRESOLVABLE,
    EXIT_NETWORK,
    EXIT_CACHE,
    EXIT_OFFLINE,
    EXIT_USAGE,
    EXIT_GENERIC,
]


def _agg_rank(code: int) -> int:
    try:
        return _AGG_PRIORITY.index(code)
    except ValueError:
        return len(_AGG_PRIORITY)


class AggregatedError(PromptCliError):
    """Carries a list of underlying errors discovered in a single pass.

    The headline ``code`` is the highest-severity code among the children per
    ``_AGG_PRIORITY``. The full list is exposed via ``details.errors`` as a
    list of envelope-shaped dicts so JSON output preserves every diagnostic.
    """

    def __init__(self, errors: list[PromptCliError], *, command: str = ""):
        if not errors:
            raise ValueError("AggregatedError requires at least one error")
        self.errors = list(errors)
        ranked = sorted(self.errors, key=lambda e: _agg_rank(e.code))
        headline = ranked[0]
        children = [
            {
                "code": e.code,
                "category": CATEGORIES.get(e.code, "internal_error"),
                "message": e.message,
                "location": e.location,
                "details": e.details,
            }
            for e in self.errors
        ]
        message = f"{len(self.errors)} error(s); first: {headline.message}"
        super().__init__(
            headline.code,
            message,
            headline.location,
            {
                "aggregated": True,
                "count": len(self.errors),
                "errors": children,
            },
        )
