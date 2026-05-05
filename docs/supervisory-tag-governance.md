# Supervisory Tag Governance

Status: current policy reference.

Models may make mess. Supervisors own the vocabulary.

## Rules

- Tags have stable IDs; names are handles.
- Renames, merges, and deletes leave stale-name guidance.
- Mandatory families constrain future MCP tag sets.
- Locks constrain MCP writes only.
- Supervisor UI and CLI remain authoritative.
- Policy is read from SQLite on each operation; no reload required.

## Locks

- `tags/definition`: MCP cannot create new tags.
- `tags/family`: MCP cannot mutate registry structure if such tools exist.

Existing tag assignment is not locked. Mandatory families are the shaping
mechanism.

## Mandatory Families

A mandatory family means every future MCP-created or MCP-replaced tag set must
include at least one active tag from that family.

The rule is prospective. Existing records are not invalidated.

Good porcelain:

```text
mandatory tag family `phase` is missing; include at least one of: baseline, ablation, optimization
```

## Stale Names

Stale context should receive a correction, not an opaque unknown-tag fault:

```text
tag `ls` was merged into `search/local`; use `search/local`
```

## Surface

MCP gets:

- `tag.add`
- `tag.list`
- tag policy faults

Supervisor surfaces get cleanup:

- create/rename/delete families
- assign tags to families
- mark families mandatory
- rename/merge/delete tags
- toggle locks

Every cleanup mutation records an event.
