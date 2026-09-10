# OpenSSH socket integration: phased implementation

Date: 2026-09-10. Status: Phases 1–2 complete; Phases 3–5 not started.

## Purpose and how to use this document

Build one persistent ScriptHut controller that can use authenticated OpenSSH
control sockets for independent HPC clusters, recover safely from connection
loss, and optionally help the user authenticate through the browser.

The [original handoff](openssh-socket-handoff.md) records deployment context,
constraints, and the original design. Preserve it as historical context. This
document organizes implementation into bounded phases and records the decisions
made during planning. Check live deployment facts again before rollout.

Implement and review one phase at a time. Phase 1 below is complete;
before each later phase, refine its interfaces and implementation steps using
the evidence from previous phases. Record those decisions here, along with
validation results and any remaining blockers. Later-phase details are not an
instruction to build the entire feature in one change.

This roadmap includes deployment as the eventual endpoint. Writing it does not
itself authorize implementation, publication, deployment, or external messages;
follow the current session's instructions for those actions.

## Shared decisions and constraints

- Keep AsyncSSH as the default. OpenSSH socket transport is opt-in, and external
  socket mode must work without browser-assisted login.
- Put reusable transports, lifecycle management, UI/API, helper behavior, tests,
  and documentation in this repository. Put machine profiles, accounts, mounts,
  helper installation, and image pins in agent-infra.
- Keep the current local service working during development. Do not change Slurm
  scheduler policy or redesign the 60-second submission/accounting grace period.
- Use a dedicated host-owned master and a private 0700 socket directory mounted
  into the container with matching UID/GID. Mount the directory so replacement
  sockets become visible. Do not mount the whole `.ssh` directory or an engine
  socket. Never take ownership of the user's existing midway3 master.
- Automated operations must fail closed when socket reuse fails, including the
  race between checking a master and executing a command. They must never fall
  back to a fresh network connection or initiate password/Duo authentication.
- Defaults: eight-hour idle persistence, 30-second keepalives, three missed
  keepalives, two concurrent automated operations per backend, verified host
  keys, and a five-minute browser authentication timeout. These are not promises
  of session lifetime or an expiry countdown.
- Disconnection preserves cached job state, makes monitoring stale, and pauses
  new submissions. A transport failure is not evidence that a job failed.
- Persist a unique attempt before Slurm submission. The selected reconciliation
  marker is a unique suffix on the readable Slurm job name; preserve the task's
  application label and existing comments. An unresolved outcome must never
  trigger automatic resubmission.
- Browser login uses the existing verified localhost SSH connection to invoke a
  fixed host helper through a PTY. Resolve destinations from configured backend
  profiles, never browser-supplied commands. Do not add a host daemon.
- The selected access model is a single-user controller behind verified private
  access (Tailscale or localhost), with origin checks and CSRF/session binding.
  A separate application sign-in is outside this design. A reachable proxy route
  does not establish that its access policy is correct.
- Passwords and authentication transcripts must not enter command logs, terminal
  replay/history, diagnostics, files, or persistent state. No password storage,
  prompt scraping, hardcoded Duo choices, or automatic MFA retries.
- A successfully detached master survives browser and container exit. After a
  host reboot or connection loss, the user authenticates again.
- Ordinary tests use disposable SSH servers or fake processes, never RCC
  credentials or Duo pushes. Keep existing backends and Windows imports working;
  selecting an unsupported POSIX feature must fail clearly.

## Phase overview

| Phase | Deliverable | Depends on | Completion gate |
| --- | --- | --- | --- |
| 1. Compatibility fixes | Normal source fixes for known-hosts handling and Slurm TIMEOUT | None | Recorded baseline and passing regressions |
| 2. External socket transport | Commands, files, logs, terminals, and connection checks/status | 1 | Disposable SSH tests prove coverage and no direct fallback |
| 3. Disconnection correctness | Paused scheduling and durable submission reconciliation | 2 | Loss/restart tests prove no duplicate submissions |
| 4. Browser login | Host helper, login UI/API, and browser protections | 2 and 3 | Authentication lifecycle and browser tests pass |
| 5. Deployment | Pinned fork image, host integration, and midway3 configuration | 1–4 | Bounded local and midway3 acceptance passes |

**Phase 2 must not be deployed for remote submissions until Phase 3 passes.**
Transport development and disposable-server testing can proceed independently
of production scheduling. The two Phase 1 fixes remain independently reviewable.

## Phase 1: compatibility fixes

### Scope and implementation

1. Create a feature branch from the current checkout, preserving existing local
   changes. The inspected starting commit is `82b3bd6` (v0.12.20). Do not recreate
   the fork or modify the live deployment.
2. Create an isolated development environment and install `.[dev]`. Run full
   pytest and `scripthut --help`; record Python/dependency versions, commands,
   counts, and baseline failures. At planning time there was no local virtualenv
   and the system Python lacked pytest, so no baseline has been established.
3. In `src/scripthut/ssh/client.py`, convert a configured `known_hosts` Path to
   `str` before passing it to AsyncSSH. Narrow the intermediate annotation
   accordingly. Preserve existing behavior when no file is configured.
4. In `src/scripthut/backends/slurm.py`, collect parent TIMEOUT evidence
   independently of batch rows and apply it after parsing. Parent TIMEOUT takes
   precedence regardless of row order. Preserve existing batch OOM/failure and
   nonzero-exit detection when the parent reports COMPLETED, along with resource
   statistics and numeric exit-code behavior.
5. Make the two fixes separate commits with their own regression tests. Reference
   agent-infra's Containerfile, `fix_timeout.py`, and `verify_image.py` for the
   known failures, but do not copy their source-rewriting deployment mechanism.

### Tests and completion gate

- Use a disposable local AsyncSSH server and temporary keys/known-hosts files to
  test the actual connection path: matching key succeeds; incorrect key is
  rejected. Do not depend solely on a mocked argument assertion.
- Extend accounting parser tests with parent TIMEOUT plus batch CANCELLED in
  both row orders. Cover parent COMPLETED with batch OOM and nonzero exit, and
  ordinary cancellation. Verify relevant state and exit-code outputs.
- Run targeted regressions, full pytest, and CLI help. Run relevant lint/type
  checks and distinguish existing issues from changes introduced here; avoid
  unrelated formatting or cleanup.
- Record results in this document. The phase is complete when the fixes and
  regressions pass, the full-suite result is accounted for, and no deployment
  changes are needed to review the source fixes.

## Phase 2: external socket transport

### Interface decisions (2026-09-09)

- `ExecutionClient` is a structural protocol for the username, existing lifecycle,
  command result `(stdout, stderr, exit_code)`, logging callback, async context
  manager, and interactive-process operations. `InteractiveProcess` exposes
  byte stdin/stdout, resize, close, wait_closed, and returncode. Local execution
  explicitly rejects interactive sessions. EC2 SSM keeps constructing AsyncSSH.
- SSH configuration adds `transport: asyncssh | openssh`, `control_path`,
  `max_operations: 2`, `control_persist: 8h`, and optional `terminal_command`.
  The latter is operator-provided copyable text only, never executed by the
  application, allowing host/container path differences. OpenSSH requires an
  absolute expanded socket path; explicit known_hosts is required to generate
  a fallback master command when terminal_command is omitted.
- Automated OpenSSH argv uses an absolute ssh executable, `-F none`,
  `ControlMaster=no`, `BatchMode=yes`, `ProxyCommand=exec /absolute/false`,
  `StrictHostKeyChecking=yes`, disabled forwarding, and the explicit socket,
  host, user and port. Configuration is never read implicitly. Socket paths
  containing OpenSSH expansion tokens are rejected. Ordinary command stdin is
  closed; interactive sessions use a local raw PTY and `-tt -e none`.
- OpenSSH command concurrency is semaphore-bounded; interactive sessions do not
  hold automated slots. Timed-out/cancelled operations reap only their child
  process. Exit 255 is conservatively treated as transport failure (it can also
  be a remote exit code); no command is automatically replayed.
- OpenSSH owns its health snapshot: pathname presence, master response, last
  successful remote check, first-observed time, last error, and connection state.
  Probe with `-O check` then a remote `true`. Coalesce concurrent probes and use
  30-second successful probe caching or 30–300-second exponential failure
  backoff. Explicit Check bypasses the delay but joins any in-flight probe.
- Expose GET `/api/v1/backends/{name}/connection` (cached status) and POST on its
  `/check` subpath. The new check endpoint rejects cross-origin browser requests.
  It probes without replacing the master. Existing AsyncSSH behavior remains;
  detailed socket status/UI is specific to OpenSSH. No browser login is added.
- Polling preserves cached backend details on errors. Phase 3 retains ownership
  of scheduler state/reconciliation changes; this phase is not production-ready
  for remote submissions until that gate passes.

### Deliverable

- Introduce the smallest shared execution interface needed by command execution,
  lifecycle/status, logging, and interactive callers. Preserve the existing
  command-result contract and AsyncSSH default. Account for the local executor
  and keep EC2 SSM's existing AsyncSSH behavior.
- Add explicit transport selection and control-socket configuration; centralize
  runtime/CLI construction. Audit script staging, log retrieval, binary files,
  source/cache operations, and interactive sessions rather than replacing only
  `connect()`.
- Launch OpenSSH with argument arrays and controlled options. Evaluate a failing
  `ProxyCommand` with configuration inheritance disabled as the no-fallback
  mechanism; acceptance depends on observed race behavior, not just argument
  construction or a preliminary `ssh -O check`.
- Adapt POSIX PTY streams, resize, close, and exit status to a shared browser
  relay. Remove the need for duplicated AsyncSSH-specific relay logic.
- Add backend health/status and Check connection controls, with copyable terminal
  fallback instructions. Distinguish socket existence, control response, and
  remote-command success. Show last successful check and first-observed versus
  known connection time; retain useful cached details on failure.
- Bound automated operations per backend and back off/coalesce health probes.
  Disconnecting the application closes its own processes, never an external
  master. This phase does not initiate browser authentication.

### Completion gate and next design checkpoint

Prove commands, script staging, logs, binary files, and terminals work through a
disposable SSH server. Cover missing/stale/replaced sockets, permissions,
multiplexing limits, timeout cleanup, safe arguments, and master loss between
check and use. Assert that no fresh network connection occurs. Verify AsyncSSH,
local execution, EC2 construction, and unsupported-platform behavior.

The interface decisions above and Phase 2 evidence below complete this checkpoint.
The Phase 3 deployment gate still applies.

## Phase 3: disconnection and submission correctness

### Deliverable

- Carry explicit backend availability and poll freshness into scheduling and
  restoration. Cached jobs remain visible but failed polls cannot drive
  missing-job transitions, dependency releases, or new submissions.
- Separate deferred transport errors from definitive submission failures. Today
  `process_run()` can fail remaining pending tasks after submission failure;
  transport unavailability must bypass that behavior.
- Persist attempt ID, timestamp, expected scheduler job name, and any returned
  job ID. Introduce explicit nonterminal submission-in-progress/unknown states
  that block dependencies and account for potentially occupied concurrency.
- Add a strict durable write before `sbatch`, propagating persistence failures.
  The current `_persist_run()` only marks data dirty and `save_run()` logs write
  failures; neither is sufficient to establish this submission boundary.
- Persist a returned job ID before acceptance verification. On an ambiguous
  result or restart, reconcile queue/accounting records scoped to cluster, user,
  and attempt time, matching an untruncated unique job-name suffix.
- Adopt exactly one matching job. No matches, multiple matches, delayed
  accounting, or insufficient permissions retain an explicit unknown outcome.
  Do not infer that absence proves non-submission.
- Provide deliberate user reconciliation through UI/API/CLI: check again, bind a
  verified job, or explicitly declare an attempt unsubmitted before retrying
  with a new marker. Retain attempt history. Ensure cancel, delete, rerun, and
  controller restart cannot silently bypass unresolved attempts.
- Load existing state without manual conversion and document rollback limits
  for older controllers that do not understand the new states.

### Completion gate and next design checkpoint

Inject loss before submission, after scheduler acceptance, after the response,
and during persistence, including controller restart at each boundary. Assert
no duplicate jobs, no false failure from disconnection, and correct recovery of
monitoring and scheduling. Cover delayed/ambiguous accounting, disk-write errors,
manual resolution, dependencies, concurrency, and existing stored runs.

Before implementation, specify the state transitions, durable-write contract,
reconciliation query/marker format, run-pausing policy, public resolution
actions, and cancellation semantics here. Audit every submission entrypoint,
including CLI, restored runs, and interactive task submissions.

## Phase 4: browser-assisted login

### Deliverable

- Prototype the generic host helper's PTY/authenticate/detach lifecycle before
  building the UI. Use a per-profile lock and owned socket directory; report an
  already-healthy master without replacing it. Verify remote execution after
  authentication without parsing password/Duo prompts.
- Enforce one attempt per backend. Duplicate clicks report an active attempt.
  Explicit cancel, timeout, browser disconnect, and controller shutdown abort
  unfinished authentication, with host-side cleanup independent of the browser.
  Successful detach wins the lifecycle boundary: closing the UI afterward must
  leave the master alive. Test the success/cancel race explicitly.
- Add opt-in browser login using configured backend IDs, session-bound start/
  cancel/status operations, and a PTY WebSocket. Reuse the transport adapter and
  relay; keep authentication traffic outside ordinary terminal logs/history.
- Enforce allowed origins before accepting WebSockets, CSRF checks for browser
  state changes, and session ownership of attempts. Preserve non-browser CLI/API
  use. Require the configured private-access boundary before enabling login.
- Render a small dedicated login terminal using locally served pinned assets,
  restrictive page policy, transient output, and PTY no-echo behavior. Expose
  connected/disconnected/authenticating/failed states and terminal fallback.
  Persist only non-secret lifecycle results, never authentication transcripts.

### Completion gate and next design checkpoint

Test success, failed authentication, cancel, timeout, duplicate clicks, closed
browser, controller exit, rejected origins/sessions, and secret-log exclusion.
Prove post-success master survival and pre-success cleanup using local fixtures.
No ordinary test sends Duo pushes.

Before implementation, specify helper installation/profile format, ownership
and cleanup rules, detach evidence, API/session protocol, origin/CSRF handling,
and browser test tooling here. Do not proceed to live browser authentication
until these local lifecycle and endpoint tests pass.

## Phase 5: pinned deployment and live acceptance

### Deliverable

- Update agent-infra to install the helper/profile and mount the dedicated socket
  directory. Back up state/configuration, retain the current image, and build a
  candidate pinned to the fork commit and registry digest. Keep publication and
  service switching explicit; do not introduce automatic upgrades.
- Validate the candidate separately before switching. Remove build-time source
  patches only when the deployed fork contains their tested equivalents.
- Verify the effective private access policy before enabling browser login.
  A read-only check during planning confirmed Tailscale Serve proxies the tailnet
  URL to `127.0.0.1:8000`; the older agent-infra operations note saying Serve is
  disabled is stale. Effective identity restrictions remain unverified.
- Configure midway3's external hostname, user `edjee`, account `pi-akaring`, and
  initial partition `caslake`. Use dedicated timestamped smoke-test directories
  under personal scratch and tiny allocations. Coordinate password/Duo with the
  user; never retry MFA silently or use login nodes for computation.
- Re-run local acceptance and bounded midway3 success/failure, dependencies,
  logs/files, cancellation, and short timeout checks. Exercise socket loss and
  replacement while jobs continue, manual reconnect, and controller restart.
- Record image identity, test commands, scheduler IDs, and non-secret evidence.
  Document rollback with compatible state; never restore stale pre-submission
  state over newer scheduler activity.

### Completion gate and next design checkpoint

Both backends work from the persistent controller; connection status is accurate;
browser authentication has the tested lifecycle; local behavior is preserved;
and reconnect/restart preserves job IDs without duplicate submissions.

Before rollout, record exact image/helper versions, mounts, access-policy
evidence, test resource limits/directories, backup and rollback procedure, and
the live acceptance commands. A physical host reboot remains a planned
maintenance-window check; scientific project onboarding, further clusters,
Julia/modules setup, and retained-results layout are separate work.

## Progress and evidence

| Phase | Status | Evidence |
| --- | --- | --- |
| 1 | Complete | `8d8c2e1` (known-hosts), `ffb4597` (TIMEOUT); validation below |
| 2 | Complete | `9305a64` and backend-card follow-up on the same branch; validation below |
| 3 | Not started | Dirty-save and scheduling failure paths identified |
| 4 | Not started | Browser access/lifecycle requirements recorded |
| 5 | Not started | Existing deployment facts in handoff; Serve route checked during planning |

Update each row with commit IDs, test results, and links to non-secret evidence
as work completes. A phase is not complete merely because its code exists.

### Phase 1 validation (2026-09-09)

Branch: `fix/ssh-known-hosts-slurm-timeout`, based on `82b3bd6`.
Independent implementation commits:

- `8d8c2e1`: convert known-hosts Path to string; real loopback SSH tests prove
  trusted-key acceptance and mismatched-key rejection.
- `ffb4597`: preserve parent TIMEOUT after parsing batch rows; eight parameterized
  cases cover timeout/cancel, OOM, nonzero exit, and ordinary cancellation in
  both row orders, including CPU/RSS/timestamps and numeric exit-code preservation.

Environment: isolated `.venv`, Python 3.14.7, AsyncSSH 2.24.0,
cryptography 50.0.1, pytest 9.1.1, pytest-asyncio 1.4.0, FastAPI 0.141.1,
Starlette 0.52.1, Pydantic 2.13.5, Ruff 0.16.6, and mypy 2.3.1.
Installed with `.venv/bin/python -m pip install -e '.[dev]'`. The full dependency
snapshot and raw command output are local artifacts in `/tmp/scripthut-phase1/`;
the durable results are summarized here. Python 3.11–3.13 and other operating
systems were not exercised locally; the existing CI matrix remains unchanged.

| Check | Result |
| --- | --- |
| Baseline full pytest before fixes | 1,231 passed, 1 skipped, 16 warnings |
| SSH regressions before fix | Both failed with the expected Path TypeError |
| Slurm regressions before fix | Both TIMEOUT row orders failed; 39 other cases passed |
| Targeted regressions after fixes | 43 passed |
| Final full pytest | 1,241 passed, 1 skipped, 16 warnings |
| CLI help, before and after | Passed |
| Ruff on changed source/test files | Same 23 pre-existing findings; new SSH test file passes |
| mypy on the two changed source files | Same 5 pre-existing errors in SSH client |
| `git diff --check` | Passed |

Validation commands (run from the checkout):

```sh
.venv/bin/python -m pytest --tb=short -q
.venv/bin/python -m pytest tests/test_ssh_client.py tests/test_sacct_failure.py -q --tb=short
.venv/bin/scripthut --help
.venv/bin/ruff check src/scripthut/ssh/client.py src/scripthut/backends/slurm.py tests/test_sacct_failure.py tests/test_ssh_client.py --output-format concise
.venv/bin/mypy src/scripthut/ssh/client.py src/scripthut/backends/slurm.py
git diff --check
```

The baseline command excluded the newly added `tests/test_ssh_client.py` while
both source files were still unmodified and before the Slurm cases were added.
Tests stalled inside the restricted sandbox; the recorded baseline, regressions,
and final suite ran outside it with approval. The SSH server binds only loopback
on an ephemeral port and uses temporary generated keys; no RCC login occurred.

Existing lint findings concern imports, line lengths, naming, and modernization.
Existing type errors concern the client-key list, generic process annotation,
and bytes/string command results. These remain outside Phase 1. No live service,
Slurm policy, deployment patch, or grace-period behavior was changed.

### Phase 2 validation (2026-09-10)

Branch: `feat/openssh-external-sockets`, based on Phase 1 (`ffb4597`).
Core transport/API commit: `9305a64`; the backend-card template and this evidence
record are tracked in follow-up commits on the same branch.
The environment remains the isolated Python 3.14.7 virtualenv recorded above.
The real client is OpenSSH 10.5p1 with OpenSSL 3.6.4.
All live SSH tests use a disposable loopback AsyncSSH server and test-owned
OpenSSH masters with temporary public-key authentication. No RCC authentication,
host helper installation, production configuration change, or deployment occurred.

Implemented interfaces and integration:

- Added structural execution/process protocols and a shared SSH factory used by
  controller and CLI. Local execution satisfies the interface and explicitly
  rejects interactive terminals. EC2 SSM continues constructing AsyncSSH.
- Added external OpenSSH configuration, semaphore-bounded commands, cached/
  coalesced/backed-off probes, and a POSIX PTY adapter with explicit resize-signal
  delivery. Shutdown synchronizes with process creation and reaps owned children.
- Consolidated both browser terminal paths around one byte-stream relay, with
  incremental UTF-8 decoding and cleanup on either endpoint's exit.
- Added connection-status/check API routes and backend-card socket details,
  timestamps, errors, check action, and copyable terminal fallback. Poll/check
  failures preserve cached backend details; scheduler reconciliation is Phase 3.
- Audited script staging, log/disk/file retrieval, sources, stacks, cache, and
  task-output callers: remote access uses `run_command`, including base64 for
  binary outputs; no direct AsyncSSH connection or SFTP access needed replacement.
  Updated their annotations to the shared interface. Existing command construction
  outside the transport remains unchanged.

| Check | Result |
| --- | --- |
| Existing suite after integration, excluding new tests | 1,241 passed, 1 skipped, 16 warnings |
| Targeted OpenSSH, shared transport/relay, and known-hosts tests | 35 passed |
| Final full pytest | 1,274 passed, 1 skipped, 16 warnings |
| CLI help | Passed |
| Ruff on all new modules and tests | Passed |
| Full-source mypy compared with Phase 1 snapshot | 117 existing errors versus 119 before; no new diagnostics after normalizing renamed interface types |
| `git diff --check` | Passed |

The real OpenSSH tests count accepted TCP connections. Killing the master between
control check and remote command, killing it before interactive process startup,
and refusing new server sessions each leave the connection count unchanged:
there is no direct fallback. Loss during an active command produces a transport
error with exactly one remote invocation. Explicit fixture master replacement
creates the only additional connection and resets first-observed time.

Additional coverage includes stale files and stale Unix sockets, permission
errors, two-operation concurrency, timeout/cancellation reaping, shutdown during
spawn, probe coalescing/backoff/force, script heredoc staging through the actual
Slurm submission/verification path with test scheduler executables, log tailing,
binary retrieval, safe quoted command arguments, terminal input/resize/exit,
master survival, rejected check origins, status/card rendering, and unsupported
platform selection. Permission failures use a deterministic fixture; session
admission failures and no-fallback races use the actual OpenSSH client/server.

Reproduce from the checkout:

```sh
.venv/bin/python -m pytest tests/test_openssh.py tests/test_transport.py tests/test_ssh_client.py -q --tb=short
.venv/bin/python -m pytest --tb=short -q
.venv/bin/scripthut --help
.venv/bin/ruff check src/scripthut/ssh/transport.py src/scripthut/ssh/openssh.py src/scripthut/ssh/factory.py src/scripthut/ssh/pty_process.py tests/test_transport.py tests/test_openssh.py
.venv/bin/mypy src
git diff --check
```

Tests requiring loopback/process access ran outside the restricted sandbox with
approval. Raw results and dependency snapshot are local `/tmp/phase2-*` artifacts;
the results above are the durable summary. CI now verifies that the OpenSSH client
is present on Linux/macOS, so integration tests cannot silently skip for a missing
client there. Windows skips POSIX integration while testing default behavior and
explicit unsupported-platform errors. Other OS/Python matrix results remain for
CI to establish; they have not been run locally.
