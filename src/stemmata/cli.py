from __future__ import annotations

import argparse
import io
import re
import signal
import sys
import traceback
from pathlib import Path
from typing import Any

import yaml

from stemmata import __version__
from stemmata.cache import Cache, default_cache_dir
from stemmata.envelope import failure, success, to_json, to_text
from stemmata.errors import (
    EXIT_GENERIC,
    EXIT_USAGE,
    GenericError,
    PromptCliError,
    UsageError,
)
from stemmata.interp import Layer, interpolate
from stemmata.merge import merge_namespaces
from stemmata.npmrc import load_npmrc
from stemmata.prompt_doc import RESERVED_KEYS
from stemmata.registry import RegistryClient
from stemmata.resolver import Session, layer_order, resolve_graph


_DURATION_RE = re.compile(r"^(\d+(?:\.\d+)?)(ms|s|m|h)?$")


def _parse_duration(v: str) -> float:
    m = _DURATION_RE.match(v.strip())
    if not m:
        raise UsageError(f"invalid duration: {v!r}", argument="duration", reason="invalid_duration")
    num = float(m.group(1))
    unit = m.group(2) or "s"
    return {"ms": num / 1000.0, "s": num, "m": num * 60.0, "h": num * 3600.0}[unit]


def _check_python_version() -> None:
    if sys.version_info < (3, 12):
        sys.stderr.write(
            f"stemmata requires Python 3.12+, found {sys.version_info.major}.{sys.version_info.minor}\n"
        )
        sys.exit(EXIT_GENERIC)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="stemmata")
    parser.add_argument("--version", action="version", version=__version__)
    parser.add_argument("--output", choices=["yaml", "json", "text"], default=None)
    parser.add_argument("--verbose", action="store_true", default=False)
    parser.add_argument("--offline", action="store_true", default=False)
    parser.add_argument("--refresh", action="store_true", default=False)
    parser.add_argument("--cache-dir", default=None)
    parser.add_argument("--npmrc", default=None)
    subs = parser.add_subparsers(dest="cmd")

    resolve = subs.add_parser("resolve")
    resolve.add_argument("target", nargs="?")
    resolve.add_argument("--max-prompts", type=int, default=1000)
    resolve.add_argument("--max-depth", type=int, default=50)
    resolve.add_argument("--max-download-size", type=int, default=64 * 1024 * 1024)
    resolve.add_argument("--max-total-size", type=int, default=512 * 1024 * 1024)
    resolve.add_argument("--http-timeout", default="30s")
    resolve.add_argument("--timeout", default="5m")

    cache_cmd = subs.add_parser("cache")
    cache_subs = cache_cmd.add_subparsers(dest="cache_cmd")
    cache_subs.add_parser("clear")

    publish_cmd = subs.add_parser("publish")
    publish_cmd.add_argument("path", nargs="?", default=".")
    publish_cmd.add_argument("--dry-run", action="store_true", default=False)
    publish_cmd.add_argument("--strict-schema", action="store_true", default=False)
    publish_cmd.add_argument("--tarball", default=None,
                             help="write the built tarball to this path (implies --dry-run unless combined with upload)")
    publish_cmd.add_argument("--max-prompts", type=int, default=1000)
    publish_cmd.add_argument("--max-depth", type=int, default=50)
    publish_cmd.add_argument("--max-download-size", type=int, default=64 * 1024 * 1024)
    publish_cmd.add_argument("--max-total-size", type=int, default=512 * 1024 * 1024)
    publish_cmd.add_argument("--http-timeout", default="30s")
    publish_cmd.add_argument("--timeout", default="5m")
    return parser


def _deterministic_yaml_dump(data: Any) -> str:
    class _Dumper(yaml.SafeDumper):
        pass

    def _str_representer(dumper: yaml.Dumper, value: str) -> yaml.nodes.ScalarNode:
        if "\n" in value:
            return dumper.represent_scalar("tag:yaml.org,2002:str", value, style="|")
        return dumper.represent_scalar("tag:yaml.org,2002:str", value)

    _Dumper.add_representer(str, _str_representer)
    from stemmata.yaml_loader import _ScalarStr

    _Dumper.add_representer(_ScalarStr, _str_representer)

    return yaml.dump(
        data,
        Dumper=_Dumper,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
        width=1024,
    )


def _run_resolve(args: argparse.Namespace, stdout, stderr) -> int:
    if not args.target:
        raise UsageError("resolve requires a target", argument="target", reason="missing")
    cache_root = Path(args.cache_dir) if args.cache_dir else default_cache_dir()
    cache = Cache(root=cache_root)
    npmrc_path = Path(args.npmrc) if args.npmrc else None
    config = load_npmrc(npmrc_path)
    http_timeout = _parse_duration(args.http_timeout)
    overall_timeout = _parse_duration(args.timeout)
    registry = RegistryClient(config=config, offline=args.offline, http_timeout=http_timeout)
    session = Session(
        cache=cache,
        registry=registry,
        refresh=args.refresh,
        max_prompts=args.max_prompts,
        max_depth=args.max_depth,
        max_download_bytes=args.max_download_size,
        max_total_bytes=args.max_total_size,
        verbose=bool(getattr(args, "verbose", False)),
        stderr=stderr,
    )

    deadline_handler_installed = False
    if overall_timeout > 0 and hasattr(signal, "SIGALRM"):
        def _timeout(_signum, _frame):
            raise TimeoutError("overall wall-clock timeout exceeded")
        signal.signal(signal.SIGALRM, _timeout)
        signal.setitimer(signal.ITIMER_REAL, overall_timeout)
        deadline_handler_installed = True
    try:
        graph = resolve_graph(args.target, session)
    finally:
        if deadline_handler_installed:
            signal.setitimer(signal.ITIMER_REAL, 0)

    order = layer_order(graph)
    layers_data = [graph.nodes[nid].doc.namespace for nid in order]
    provenance = [(nid.canonical, graph.nodes[nid].file) for nid in order]
    merged = merge_namespaces(layers_data, provenance=provenance)
    layers = [Layer(canonical_id=nid.canonical, data=graph.nodes[nid].doc.namespace) for nid in order]

    root_file = graph.nodes[graph.root_id].file
    resolved = interpolate(merged, layers, root_file=root_file)

    out_mode = args.output or "yaml"
    if out_mode == "yaml":
        stdout.write(_deterministic_yaml_dump(resolved))
        return 0
    ancestor_payload = [
        {"canonical_id": nid.canonical, "distance": graph.distances[nid]}
        for nid in order
        if nid != graph.root_id
    ]
    payload = {
        "root": graph.root_id.canonical,
        "content": resolved,
        "ancestors": ancestor_payload,
    }
    env = success("resolve", payload)
    if out_mode == "json":
        stdout.write(to_json(env))
    else:
        stdout.write(to_text(env))
    return 0


def _run_publish(args: argparse.Namespace, stdout, stderr) -> int:
    from stemmata.publish import PublishOptions, run_publish

    package_root = Path(args.path).resolve()
    if not package_root.is_dir():
        raise UsageError(
            f"publish target {args.path!r} is not a directory",
            argument="path",
            reason="not_a_directory",
        )
    cache_root = Path(args.cache_dir) if args.cache_dir else default_cache_dir()
    npmrc_path = Path(args.npmrc) if args.npmrc else None
    config = load_npmrc(npmrc_path)
    http_timeout = _parse_duration(args.http_timeout)
    overall_timeout = _parse_duration(args.timeout)

    tarball_out = Path(args.tarball) if args.tarball else None
    opts = PublishOptions(
        package_root=package_root,
        dry_run=bool(args.dry_run),
        strict_schema=bool(args.strict_schema),
        tarball_out=tarball_out,
        config=config,
        offline=bool(args.offline),
        refresh=bool(args.refresh),
        http_timeout=http_timeout,
        cache_root=cache_root,
        max_prompts=args.max_prompts,
        max_depth=args.max_depth,
        max_download_bytes=args.max_download_size,
        max_total_bytes=args.max_total_size,
        verbose=bool(getattr(args, "verbose", False)),
        stderr=stderr,
    )

    deadline_handler_installed = False
    if overall_timeout > 0 and hasattr(signal, "SIGALRM"):
        def _timeout(_signum, _frame):
            raise TimeoutError("overall wall-clock timeout exceeded")
        signal.signal(signal.SIGALRM, _timeout)
        signal.setitimer(signal.ITIMER_REAL, overall_timeout)
        deadline_handler_installed = True
    try:
        result = run_publish(opts)
    finally:
        if deadline_handler_installed:
            signal.setitimer(signal.ITIMER_REAL, 0)

    payload = {
        "name": result.name,
        "version": result.version,
        "tarball_path": result.tarball_path,
        "tarball_size": result.tarball_size,
        "integrity": result.integrity,
        "shasum": result.shasum,
        "uploaded": result.uploaded,
        "registry_url": result.registry_url,
        "prompts_checked": result.prompts_checked,
    }
    env = success("publish", payload)
    out_mode = args.output or "json"
    if out_mode == "yaml":
        raise UsageError(
            "'publish' does not produce YAML output; use --output json or --output text",
            argument="--output",
            reason="yaml_not_supported",
        )
    if out_mode == "text":
        stdout.write(to_text(env))
    else:
        stdout.write(to_json(env))
    return 0


def _run_cache_clear(args: argparse.Namespace, stdout, stderr) -> int:
    if args.output == "yaml":
        raise UsageError(
            "'cache clear' does not produce YAML output; use --output json or --output text",
            argument="--output",
            reason="yaml_not_supported",
        )
    cache_root = Path(args.cache_dir) if args.cache_dir else default_cache_dir()
    cache = Cache(root=cache_root)
    removed, freed = cache.clear_all()
    payload = {"entries_removed": removed, "bytes_freed": freed}
    env = success("cache.clear", payload)
    if args.output == "text":
        stdout.write(to_text(env))
    else:
        stdout.write(to_json(env))
    return 0


def run(argv: list[str] | None = None, *, stdout=None, stderr=None) -> int:
    _check_python_version()
    parser = _build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as e:
        return EXIT_USAGE if e.code not in (0, None) else (e.code or 0)

    stdout = stdout if stdout is not None else sys.stdout
    stderr = stderr if stderr is not None else sys.stderr

    command_name = args.cmd or "?"
    try:
        if args.cmd == "resolve":
            return _run_resolve(args, stdout, stderr)
        if args.cmd == "cache" and args.cache_cmd == "clear":
            return _run_cache_clear(args, stdout, stderr)
        if args.cmd == "publish":
            return _run_publish(args, stdout, stderr)
        raise UsageError(
            "no subcommand provided (try 'resolve', 'publish', or 'cache clear')",
            argument="<subcommand>",
            reason="missing_subcommand",
        )
    except PromptCliError as err:
        if args.cmd == "cache" and args.cache_cmd == "clear":
            command_name = "cache.clear"
        elif args.cmd == "resolve":
            command_name = "resolve"
        elif args.cmd == "publish":
            command_name = "publish"
        env = failure(command_name, err)
        stdout.write(to_json(env))
        if stderr is not None:
            stderr.write(to_text(env) + "\n")
        return err.code
    except TimeoutError as e:
        err = GenericError(str(e), exception="TimeoutError")
        env = failure(command_name, err)
        stdout.write(to_json(env))
        stderr.write(to_text(env) + "\n")
        return err.code
    except Exception as e:
        tb = traceback.format_exc() if getattr(args, "verbose", False) else ""
        err = GenericError(str(e) or repr(e), exception=type(e).__name__, traceback=tb)
        env = failure(command_name, err)
        stdout.write(to_json(env))
        stderr.write(to_text(env) + "\n")
        return err.code


def main(argv: list[str] | None = None) -> int:
    return run(argv)


__all__ = ["main", "run"]
