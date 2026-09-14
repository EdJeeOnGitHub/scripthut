# Project artifact catalog

ScriptHut renders a read-only catalog owned by the artifact gateway. It does not
scan backend directories, infer publication identity from filenames, or calculate
host storage usage. Published artifacts are separate from task-output listings.

The existing `/api/v1/artifacts` list, immutable detail and authenticated file
streams remain supported. The UI falls back to the old list if the gateway returns
404 or 501 for the catalog. Storage failure is independent of catalog availability.

## Version 1 read contract

The private gateway serves these GET routes, proxied under `/api/v1`:

| Route | Query parameters |
| --- | --- |
| `/artifact-catalog/projects` | `q`, `page`, `limit` |
| `/artifact-catalog/publications` | `project` (required), `kind` (`report` or `data`), `q`, `page`, `limit`, `include_partial` |
| `/artifact-catalog/history` | `group` (required), `page`, `limit`, `include_partial` |
| `/artifact-catalog/storage` | None |

Search is case-insensitive. Page defaults to 1, limit to 50 (maximum 100).
`include_partial` accepts `true` or `false` and defaults to false. Unknown or
repeated parameters are rejected. Unknown projects/groups return 404. The old
artifact routes still reject query parameters.

Catalog responses contain `schema_version: 1`, `items`, `page`, `limit`, `total`.
Project items contain `project`, `report_count`, `data_count`, `latest_report`
(nullable UTC epoch seconds), and `logical_bytes` across retained versions.
Publication items contain `project`, `name`, `kind`, `version`, `created` (UTC epoch
seconds), `group`, `file_count`, `logical_bytes`, `version_count`, `origin`,
`status`, `eligible`, and nullable `request_id`/`run_id`.

Groups represent project, kind and stable publication name. Clients treat the
64-character group ID as opaque. Reports use `report`; Data includes `dataset`
and `results`. The newest eligible entry wins by publication time, then immutable
version ID. History collapses repeated identical content into one version.

Origins are `collection`, `import` or `legacy`. Statuses are `completed`,
`published`, `unknown`, `failed`, `cancelled` or `incomplete`. Imported artifacts
are published without claiming a successful run. Unknown legacy records retain
their original names; their execution status is not inferred. Failed/cancelled or
incomplete collections cannot replace the default latest eligible publication.
Including partial outputs reveals those entries, including partial-only groups.

The registry adds publication metadata without changing existing entry identities
or manifest hashes. Historical collection identity is recovered only from exact
receipt, entry name, kind and version matches. Additive `display_name` fields on
artifact detail entries let the UI show stable names while preserving raw names.

## Storage semantics

Storage responses include `schema_version`, `label`, `available`, and `stale`.
Available measurements also include `observed_at`, filesystem total/used/free
bytes, `reserve_floor_bytes`, `retained_allocated_bytes`,
`incoming_allocated_bytes`, `upload_reserved_bytes` and `transfer_reserved_bytes`.
Measurements refresh in a background worker every 60 seconds. Failure retains the
last measurement as stale; before the first successful sample, available is false.

Filesystem usage includes unrelated files. Store figures estimate allocated file
blocks, deduplicating hard links and ignoring symlinks. Incoming bytes are separate
from retained bytes. Reservations are commitments, not extra disk usage. Project
logical bytes deduplicate content hashes within a project, but shared content means
project totals are not additive. Responses never include storage paths.

PDF previews use the existing streaming, range-capable file endpoint. There is no
client-side whole-PDF fetch, thumbnail service, or new size cap. Browser-native
preview availability varies; open/download controls remain available.
