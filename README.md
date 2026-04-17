# stemmata

[![CI](https://github.com/pjmartos/stemmata/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/pjmartos/stemmata/actions/workflows/tests.yml)
[![PyPI](https://img.shields.io/pypi/v/stemmata?v=1)](https://pypi.org/project/stemmata/)
[![Python Version](https://img.shields.io/badge/python-3.12%2B-blue)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

Prompt reuse across repositories is a mess. You copy a YAML prompt into a new project, tweak it, and within a week the original and the copy have diverged. Multiply that by a dozen services and you're maintaining the same boilerplate in twenty places.

`stemmata` fixes this with hierarchical composition: prompts declare ancestors (by relative path or by registry coordinate), and the CLI resolves the full inheritance chain into a single, deterministic YAML document. Ancestor prompts are distributed as npm packages through any private registry you already run.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [CLI Reference](#cli-reference)
- [Prompt Format](#prompt-format)
- [Merge Semantics](#merge-semantics)
- [Exit Codes](#exit-codes)
- [Configuration](#configuration)
- [Testing](#testing)

## Features

- **Hierarchical composition**: prompts declare `ancestors` as paths or `(package, version, prompt)` coordinates; the full transitive closure is resolved eagerly via breadth-first search.
- **Deterministic merging**: nearest-wins for scalars and lists, deep-merge for maps, with breadth-first search distance plus reference occurring-ordering (for breaking ties) so the output is reproducible.
- **Placeholder interpolation**: `${path}` references resolve against the merged namespace, with structural, textual, and list-splat forms.
- **npm registry transport**: speaks the standard npm REST API; credentials read from `~/.npmrc`.

## Installation

```
pip install stemmata
```

Requires **Python 3.12+** (for `tarfile.data_filter`). Sole third-party dependency is `PyYAML`.

## Quick Start

```bash
# You have a local prompt that inherits from a base — resolve it:
stemmata resolve ./prompts/onboarding.yaml

# Or resolve a prompt published to your registry by coordinate:
stemmata resolve '@acme/prompts-core@1.2.3#onboarding'

# Need machine-readable output for a script or pipeline:
stemmata --output json resolve ./prompts/onboarding.yaml

# Validate a prompt (or an entire directory) against its $schema:
stemmata validate ./prompts/onboarding.yaml
stemmata validate ./prompts/

# Wipe the local cache (by default stored under ~/.cache/stemmata):
stemmata cache clear
```

## CLI Reference

```
stemmata [GLOBAL FLAGS] <subcommand> [ARGS]
```

### Global flags

| Flag                        | Default        | Description                                                       |
|-----------------------------|----------------|-------------------------------------------------------------------|
| `--output {yaml,json,text}` | `yaml`         | Output format (`yaml` for all subcommands by default).            |
| `--verbose`                 | off            | Timestamped diagnostics on stderr.                                |
| `--offline`                 | off            | Forbid network access; exit `22` if a fetch would be needed.      |
| `--refresh`                 | off            | Re-fetch artifacts even if cached.                                |
| `--version`                 | —              | Print version and exit.                                           |

### `resolve <target>`

Resolves a single prompt. Target is either a local path (`./prompts/onboarding.yaml`) or a registry coordinate (`@<scope>/<name>@<version>#<prompt-id>`).

Resource limits: `--max-prompts` (default 1000), `--max-depth` (default 50), `--http-timeout` (default 30s), `--timeout` (default 5m).

On success, stdout carries the resolved YAML (or a JSON envelope with `{root, content, ancestors[]}`). On failure, stdout carries a JSON error envelope regardless of `--output`, and stderr gets a one-line human-readable summary.

### `publish [path]`

Builds and uploads the package at `path` (default `.`) to the registry routed by `~/.npmrc`. Before any bytes leave the machine, every prompt listed in `package.json` is checked for: (1) ancestor cycles, (2) intra-document type conflicts, (3) placeholder resolvability against the fully resolved namespace, (4) `dependencies` consistency with the cross-package references found in the prompts, (5) manifest closure under relative-path references — every local `ancestors` entry must resolve to a path that is itself declared in `prompts`, since only manifest-listed files are bundled, and (6) `$schema` validation against the prompt's content contract. All errors discovered in the pass are aggregated into a single envelope; the headline exit code is the most severe one (cycle > schema > reference > merge > placeholder).

Flags: `--dry-run` (build the tarball but skip upload), `--strict-schema` (treat unfetchable / unvalidated `$schema` as an error rather than a warning), `--tarball <path>` (write the built tarball to `path`). The tarball is deterministic: identical inputs produce byte-identical output.

`$schema` enforcement requires `pip install stemmata[publish]` (adds `jsonschema`). Without it, `publish` warns and skips schema validation in default mode, or errors in `--strict-schema` mode.

### `validate <target>`

Validates prompt files against their `$schema`. Target is a file path or a directory (recursively discovers `.yaml`, `.yml`, `.json` files). For YAML prompts with ancestors, the full resolve → merge → interpolate pipeline runs before validation so inherited and interpolated values participate.

Multi-document YAML files (separated by `---`) are supported — each sub-document is validated independently against its own `$schema`. Files without `$schema` are silently skipped.

All violations are collected and reported together. Error payloads include the natural source line number of the offending value.

Flags: `--strict-schema` (treat unfetchable schemas as errors), plus the same resource-limit flags as `resolve`.

`$schema` enforcement requires `pip install stemmata[publish]` (adds `jsonschema`). Supports `file://`, `http://`, and `https://` URIs, as well as bare relative paths (resolved against the validated file's directory).

### `cache clear`

Evicts every cached entry.

## Prompt Format

A prompt is a structured mapping (YAML or JSON) with reserved envelope keys plus arbitrary content:

```yaml
ancestors:
  - ../base.yaml                         # relative path (within package)
  - package: "@acme/common"              # cross-package coordinate
    version: "1.0.4"
    prompt: "defaults"
$schema: "https://schemas.example/foo.v1.json"   # optional, enforced at publish time if present

database:
  host: "db.internal"
  port: 5432
body: |
  Region is ${vars.region}; DB is ${database.host}:${database.port}.
```

`ancestors` and `$schema` are stripped from the namespace; every other key is addressable via dotted path.

### Package manifest (`package.json`)

```json
{
  "name": "@acme/prompts-core",
  "version": "1.2.3",
  "license": "UNLICENSED",
  "dependencies": { "@acme/common": "1.0.4" },
  "prompts": [
    { "id": "base",       "path": "base.yaml",             "contentType": "yaml" },
    { "id": "onboarding", "path": "extra/onboarding.yaml", "contentType": "yaml" }
  ]
}
```

`name` must be `@<scope>/<n>`. `version` is strict SemVer, no ranges. `prompts` is non-empty; `id` defaults to basename without extension and must match `[a-z0-9][a-z0-9_-]*`.

## Merge Semantics

Reachable prompts are layered by breadth-first search distance from the root (distance 0 = root, wins everything). Ties at the same distance break by enqueue order.

**Maps** are deep-merged, with the nearer value winning at each leaf:

```yaml
# ancestor (distance 1)           # root (distance 0)
database:                         database:
  host: "base.internal"             host: "override.internal"
  port: 5432                        ssl: true
```

Resolved: `database.host` = `"override.internal"` (nearer wins), `database.port` = `5432` (survives from ancestor), `database.ssl` = `true` (only root provides it).

**Lists** replace wholesale — no element-level merge. **`null`** at a nearer layer shadows the entire subtree beneath it.

For the full interpolation reference (structural vs. textual placeholders, list splat, non-splat `${=...}` form, escaping, version conflict resolution), see [`docs/interpolation.md`](docs/interpolation.md).

## Exit Codes

| Code | Meaning                             |
|------|-------------------------------------|
| `0`  | Success                             |
| `1`  | Generic / unexpected failure        |
| `2`  | Usage error                         |
| `10` | Schema validation error             |
| `11` | Unknown ancestor or prompt id       |
| `12` | Cycle detected                      |
| `14` | Unresolvable placeholder            |
| `15` | Merge / interpolation type mismatch |
| `20` | Network / registry error            |
| `21` | Cache error                         |
| `22` | Offline-mode violation              |

On failure, stdout always carries a JSON error envelope with `{status, exit_code, command, error: {code, category, message, ...}}` regardless of `--output`. Stderr gets a single-line human summary.

## Configuration

Registry routing and credentials come from `~/.npmrc` for both fetch and publish.

## Testing

```
PYTHONPATH=src python -m pytest tests/ -q
```
