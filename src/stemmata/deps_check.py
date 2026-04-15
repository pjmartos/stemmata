from __future__ import annotations

from pathlib import Path

from stemmata.errors import PromptCliError, SchemaError
from stemmata.manifest import Manifest
from stemmata.prompt_doc import CoordRef, PathRef, parse_prompt, resolve_relative


def collect_cross_package_refs(
    manifest: Manifest,
    package_root: Path,
) -> set[tuple[str, str]]:
    """Walk every prompt declared in the manifest, collect (package, version)
    pairs from cross-package ancestor references.

    This is a static walk that only inspects the prompts shipped in *this*
    package; it does not recurse into the resolved transitive closure.
    only requires that the manifest's ``dependencies`` cover the closure of
    cross-package references in the prompt payloads — and that closure starts
    with what authors literally wrote in their own files.
    """
    refs: set[tuple[str, str]] = set()
    for entry in manifest.prompts:
        prompt_file = package_root / entry.path
        if not prompt_file.is_file():
            continue
        text = prompt_file.read_text(encoding="utf-8")
        try:
            doc = parse_prompt(text, file=str(prompt_file), validate_paths=False)
        except SchemaError:
            # Schema problems are surfaced by the orchestrator's per-prompt
            # check; here we silently skip so deps_check stays focused on
            # what it is responsible for.
            continue
        for ref in doc.ancestors:
            if isinstance(ref, CoordRef):
                refs.add((ref.package, ref.version))
    return refs


def check_consistency(
    manifest: Manifest,
    package_root: Path,
    *,
    manifest_file: str,
) -> list[PromptCliError]:
    """Compare the manifest's ``dependencies`` map against the cross-package
    references discovered in the prompt payloads.

    Returns a list of ``SchemaError`` entries — one per offence — covering:

    - **missing**: a (pkg, version) reference appears in a prompt but the
      package is not declared in ``dependencies`` at all.
    - **version_mismatch**: ``dependencies[pkg]`` is pinned to a different
      version than the one referenced by a prompt.
    - **multiple_versions**: two prompts in the same package reference the
      same dependency at different versions (the manifest can only pin one).
    - **extra**: ``dependencies`` declares a package that no prompt in the
      package actually references.
    """
    errors: list[PromptCliError] = []
    refs = collect_cross_package_refs(manifest, package_root)

    # Group referenced versions per package.
    versions_per_pkg: dict[str, set[str]] = {}
    for pkg, ver in refs:
        versions_per_pkg.setdefault(pkg, set()).add(ver)

    declared = dict(manifest.dependencies)

    for pkg, vers in versions_per_pkg.items():
        if len(vers) > 1:
            errors.append(SchemaError(
                f"package {pkg!r} is referenced at multiple versions {sorted(vers)} across prompts; "
                f"package.json 'dependencies' can only pin one version",
                file=manifest_file,
                field_name=f"dependencies.{pkg}",
                reason="multiple_versions_referenced",
            ))
            continue
        ver = next(iter(vers))
        if pkg not in declared:
            errors.append(SchemaError(
                f"prompts reference {pkg}@{ver} but it is not declared in package.json 'dependencies'",
                file=manifest_file,
                field_name=f"dependencies.{pkg}",
                reason="missing_dependency",
            ))
        elif declared[pkg] != ver:
            errors.append(SchemaError(
                f"package.json 'dependencies[{pkg!r}]' is pinned to {declared[pkg]!r} "
                f"but a prompt references {ver!r}",
                file=manifest_file,
                field_name=f"dependencies.{pkg}",
                reason="version_mismatch",
            ))

    referenced = set(versions_per_pkg.keys())
    for pkg in declared:
        if pkg not in referenced:
            errors.append(SchemaError(
                f"package.json 'dependencies' declares {pkg!r} but no prompt references it",
                file=manifest_file,
                field_name=f"dependencies.{pkg}",
                reason="unused_dependency",
            ))

    errors.extend(check_local_refs(manifest, package_root, manifest_file=manifest_file))

    return errors


def check_local_refs(
    manifest: Manifest,
    package_root: Path,
    *,
    manifest_file: str,
) -> list[PromptCliError]:
    """Ensure every relative-path ancestor in a manifest-declared prompt
    resolves to a path that is itself declared in ``manifest.prompts``.

    Only manifest-listed files are bundled into the publish tarball, so a
    prompt that refers to a sibling YAML which is not in the manifest would
    resolve fine in the author's source tree but fail after install: the
    referenced file is missing from the package.
    """
    errors: list[PromptCliError] = []
    declared_paths = {p.path.replace("\\", "/").casefold() for p in manifest.prompts}

    for entry in manifest.prompts:
        prompt_file = package_root / entry.path
        if not prompt_file.is_file():
            continue
        text = prompt_file.read_text(encoding="utf-8")
        try:
            doc = parse_prompt(text, file=str(prompt_file), validate_paths=False)
        except SchemaError:
            continue
        for ref in doc.ancestors:
            if not isinstance(ref, PathRef):
                continue
            resolved = resolve_relative(entry.path, ref.raw)
            if resolved.casefold() not in declared_paths:
                errors.append(SchemaError(
                    f"prompt {entry.id!r} references local file {ref.raw!r} "
                    f"(resolved to {resolved!r}) which is not declared in "
                    f"package.json 'prompts'",
                    file=str(prompt_file),
                    field_name="ancestors",
                    reason="undeclared_local_ref",
                ))
    return errors
