# Abstract placeholders — worked example

This example demonstrates `${abstract:…}` placeholders, stemmata's analogue of
the template-method pattern in OOP. A reusable mid-graph prompt declares
required "holes" that any descendant must fill before the graph is
resolvable.

## Files

- `prompts/persona-template.yaml` — the abstract library prompt. Marked
  `abstract: true` and declares three abstracts (`persona.name`,
  `persona.tone`, `persona.role`) through `${abstract:…}` markers. Cannot
  be `resolve`-d on its own.
- `prompts/friendly-persona.yaml` — a concrete descendant that fills every
  hole. Fully resolvable.
- `package.json` — ships both prompts as a single publishable package.

## Try it

Run these commands from this directory.

### 1. `resolve` fails on the abstract prompt (exit `16`)

```
stemmata resolve prompts/persona-template.yaml
```

Stdout carries a JSON error envelope; the exit code is `16` and
`error.details.reason == "is_abstract"` (because the prompt itself is
marked `abstract: true`, not because any specific hole was forgotten).
Without `abstract: true` the same prompt would still exit 16, but with a
per-marker `reason` of `not_provided` / `abstract_inherited`.

### 2. `resolve` succeeds on the concrete descendant

```
stemmata resolve prompts/friendly-persona.yaml
```

Produces a fully interpolated YAML document. `persona.name`, `persona.tone`,
and `persona.role` all come from the concrete child; the inherited
`system_message` block scalar has every `${abstract:…}` marker substituted
with the child-provided value.

### 3. `tree` annotates the abstract holes

```
stemmata tree prompts/friendly-persona.yaml
```

The ancestor node (`persona-template.yaml`) is labelled with
`[abstract] … [abstracts: persona.name, persona.role, persona.tone]` —
the `[abstract]` prefix comes from `abstract: true`; the `[abstracts: …]`
suffix lists the declared holes. The descendant node carries no annotation
because it introduces no abstracts of its own and is not marked abstract.

### 4. `describe` lists declared and inherited abstracts

```
stemmata install .
stemmata describe '@stemmata/abstract-placeholders@0.1.0'
```

For `persona-template` the output carries `abstract: true` and
`abstracts.declared` lists the three persona dotted paths;
`abstracts.inherited` is empty. For `friendly-persona` `abstract` is false
and both buckets are empty (the descendant filled every hole it inherited),
so `content` is the fully interpolated system message.

### 5. `validate` is permissive on abstracts

```
stemmata validate prompts/
```

Exits `0`. `validate` only inspects prompts that declare a `$schema`; the
template here does not, so it is silently skipped. Add a `$schema` to
either prompt and re-run to see the per-document `abstracts` list in the
success payload — `validate` never fails on unfilled holes; it simply
defers `$schema` enforcement for any document that still has one.

### 6. `publish` notes abstract prompts without failing

```
stemmata publish --dry-run .
```

Produces the tarball, exits `0`, and logs an `info:` line to stderr
identifying `persona-template` as `abstract: true` and listing its
declared holes. The `abstracts` field in the success payload records each
one (with `reason: "is_abstract"`) and its source location so CI can
surface them as library-contract documentation rather than failures. If
the prompt were *not* marked `abstract: true` you would see a `warning:`
line instead (same shape, `reason: "not_provided"`).
