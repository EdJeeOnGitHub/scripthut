# Durable submission contract

External launchers can discover controller support through `GET /api/v1/capabilities`:

```json
{
  "schema_version": 1,
  "journal_id": "11111111-1111-4111-8111-111111111111",
  "capabilities": {
    "backend_storage": 1,
    "keyed_submission": 1,
    "archive_acknowledgement": 1,
    "artifact_references": 1
  }
}
```

Capabilities are integer protocol versions, not the application version. Durable
submission requires persistent run storage and POSIX locking; unavailable capabilities
are 0 and an unavailable journal has a null identity. Ordinary task submission remains
available. Use one controller process per run-storage directory.

Submit to `POST /api/v1/submission-requests` with `X-ScriptHut-Journal-ID` matching the
negotiated journal and a JSON body containing `task`, `backend`, and `request_key`.
Optional fields are `run_name`, `retain_until_archived`, and `artifact_refs`. Missing
journal headers return 428; mismatches return 409 before creating work. Successful
responses include `id`, `request_key`, and `journal_id`. Unknown top-level fields
return 422. Existing `/tasks/run` callers remain supported, with keyed requests using
the same journal; retention/artifact options require a key.

Keys are immutable submission identities. The digest is SHA-256 over sorted, compact
JSON excluding `request_key`, without inserting defaults. Same key and payload return
the original run; changed payloads return 409. Contention returns 503 and `Retry-After`.
Persisted reservations assign a stable run ID; run state and acceptance are durable
before scheduling. Restart recovers reservations. Accepted keys survive run-history
expiry; replay then returns the original ID and `status: history_expired` without
creating another run. In-place rerun of keyed runs is rejected; intentional new work
requires a new key.

`GET /api/v1/submission-requests/{key}` returns `key`, `digest`, `run_id`, `phase`
(`reserved` or `accepted`), `archive_receipt`, and `journal_id`. Only a structured 404
with `detail.code: request_key_not_found` and the expected `detail.journal_id` means
absence. A generic route 404 must not authorize replay.

`POST /api/v1/submission-requests/{key}/archive` accepts
`{"archive_receipt_sha256": "<64 lowercase hex characters>"}` only for terminal work
(or a repeated acknowledgement after history expiry). It returns `acknowledged`,
`run_id`, and `journal_id`. Until acknowledgement, a run submitted with retention is
protected from deletion and expiry. Conflicting receipts return 409.

Optional backend `storage` configuration provides `scratch_dir` and `persistent_dir`
on `/backends`. The launcher owns the lifecycle of its namespaced files.

Run artifact references use schema version 1 with immutable `request_id`,
`source_commit`, and `inputs`, plus terminal `outputs`. Each reference contains a name
and `sha256:<digest>` version. Failed/cancelled outputs carry a matching
`execution_status`. `POST /runs/{run_id}/artifacts` attaches outputs idempotently;
conflicting provenance or output identities return 409. Run detail exposes the
persisted references. This API does not own artifact bytes or the artifact registry.

Lookup, acknowledgement, and artifact attachment enforce `X-ScriptHut-Journal-ID`
when provided. Keep sending it after negotiation. Journal UUIDs are stored in additive
metadata alongside existing SQLite request rows and remain stable across restarts.
Back up the entire run storage, including `_submission_requests`, with writers stopped.
Never replace or regenerate the journal as a routine upgrade step.
