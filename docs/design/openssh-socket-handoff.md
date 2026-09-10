# OpenSSH sockets and browser-assisted login: design and agent handoff

Date: 2026-09-09. Status: agreed direction; implementation not started.

## Start here

The user wants one persistent ScriptHut controller on limiting-factor, initially
submitting to its local Slurm and UChicago midway3, then other independent HPC
clusters. midway3 requires interactive authentication. The user accepts having
to authenticate again after connection loss or a host reboot. They want the UI
to show connection health and ideally provide a Connect button which lets them
complete password/Duo authentication without opening a separate terminal.

Keep the design and maintenance simple. Implement reusable application features
in this fork, not accumulating source-rewriting deployment patches. Preserve
the working local deployment while developing and testing these additions.

Fork: https://github.com/EdJeeOnGitHub/scripthut

Upstream: https://github.com/tlamadon/scripthut

Local checkout: `/home/ed/projects/scripthut`. `origin` is the fork; `upstream`
is the original repository. Fork point is `82b3bd687c14474b901d837c67ad7d367e49c146`
(v0.12.20). No implementation commits or upstream issues/PRs have been made.
This handoff is initially an uncommitted local document.

The next agent should read this document, inspect the referenced code, establish
the test baseline, and begin with the compatibility fixes on a feature branch.
Do not recreate the fork or reinstall Slurm. User authorization so far covers
the fork and this handoff; follow the user's next instruction for implementation.
Opening upstream issues, PRs, or sending messages is a separate external action;
do not infer permission to contact maintainers from this document.

## What is already working

The host is Arch Linux, user ed (UID/GID 1000). Local Slurm is installed with
account research, partition cpu, MUNGE, slurmdbd, and MariaDB. These services are
active and enabled at boot. Local batch reserves one physical core and 4 GiB
for other host work. Do not change scheduler policy for this feature.

ScriptHut runs as a rootless Podman Quadlet with host networking and keep-id.
Service: `systemctl --user status scripthut`. It starts at boot without login
because Linger=yes and WantedBy=default.target. It listens on 127.0.0.1:8000.
Tailscale Serve is now enabled at https://limiting-factor.tail08a2df.ts.net.
Serve's persistent route and tailscaled startup have been verified; the actual
tailnet access policy was not inspected by this agent. Do not silently assume
the browser login endpoint has application-level user authentication.

Deployment repository: `/home/ed/projects/agent-infra`.
Read `docs/scripthut-operations.md`, `docs/slurm-operations.md`, and
`machines/limiting-factor/scripthut/` there for exact operation and tests.
That repository contains unrelated dirty files; preserve them.

Runtime paths (credentials must never be copied into Git or tool output):

| Purpose | Host path |
| --- | --- |
| Config and dedicated local SSH key | `/home/ed/.config/scripthut/` |
| Controller state | `/home/ed/.local/share/scripthut/` |
| Quadlet | `/home/ed/.config/containers/systemd/scripthut.container` |
| Default backend logs | `/home/ed/.cache/scripthut/logs/` |
| Demo outputs and live evidence | `/scratch/ed/scripthut/` |
| Private backups | `/home/ed/.local/share/scripthut-backups/` |

The CLI is pipx-installed at 0.12.20. Its global configuration uses the running
localhost server and cli_autostart=never. Current sources include demo, a local
path source with a bounded Julia computation and a dependent result check.
It has hardcoded local paths and is not ready for a remote backend.

Installed base image:
`ghcr.io/tlamadon/scripthut@sha256:c998c910f50eac4e6b4acc5460028b863a2bb67b2c9ad0a2af30c3ef761d52ab`.
Running derived image: `localhost/scripthut:0.12.20-fixes-2`, image ID
`b89b27b98af6fbfd9e09cbbfb7dfb592d8989eaa015a8ee1c5ba453107304cfe`.

Acceptance evidence: `/scratch/ed/scripthut/acceptance-20260909-084726/result.json`.
All live checks passed: success, exit 7, failed dependencies not submitted,
timeout, cancellation, logs, Julia result, and controller restart with the
same Slurm job ID and recovered history. Backup creation and archive inspection
passed; a full restore and full host reboot test remain unperformed.

## First commits: replace the existing compatibility patches

Port these two fixes into normal source changes and regression tests:

1. `src/scripthut/ssh/client.py`: convert known_hosts Path to str before passing
   it to AsyncSSH. Unmodified 0.12.20 fails with
   `'PosixPath' object is not subscriptable` when verification is enabled.
   Test a correct host key and rejection of an incorrect one.
2. `src/scripthut/backends/slurm.py`: preserve the parent job's TIMEOUT when its
   batch step is CANCELLED. The current parser lets the batch state override
   the parent. Preserve detection of batch OOM/nonzero exits when the parent
   misleadingly reports COMPLETED. Test both parent/step row orders.

Reference patches and regression checks are in agent-infra's Containerfile,
fix_timeout.py, and verify_image.py. Do not copy their build-time rewriting
mechanism into the application. Keep these fixes independently reviewable.

Separate observed behavior: short jobs show submitted for about 60 seconds
because SUBMIT_TO_FAIL_GRACE_SECONDS delays accounting lookup when they were
never observed in squeue. This is not a lost submission. The user's demo run
905ae2fc completed as Slurm jobs 29 and 30. Do not expand this feature to redesign
that grace period unless the user requests it.

## Verified midway3 facts

Read-only checks succeeded through the user's existing OpenSSH control socket.
No remote files were created and no midway3 jobs have been submitted.

| Setting | Value |
| --- | --- |
| External endpoint | midway3.rcc.uchicago.edu |
| Observed internal login node | midway3-login3.rcc.local (do not pin this endpoint) |
| User | edjee |
| Selected Slurm account | pi-akaring (user calls it akaring) |
| Other association | ssd (not selected) |
| Initial CPU partition | caslake (default); amd is also available |
| Writable personal scratch | /scratch/midway3/edjee |
| Writable retained-results root | /project/akaring/ |
| Scheduler commands | sbatch, squeue, sacct all available |

Use a dedicated project/run subdirectory under /project/akaring, never clean
the shared root. Select the scientific project's exact subdirectory when it
is onboarded. Use bounded shell smoke jobs first; Julia/modules, QOS, walltime
allowances, and project-specific storage layout still need checking remotely.

Existing socket: `/home/ed/.ssh/midway3.sock`. Existing alias has
ControlMaster=auto, ControlPersist=8h, ServerAliveInterval=0, TCPKeepAlive=yes.
It is owned by the user's interactive workflow; do not kill or take ownership
of it. It can be used for authorized read-only discovery.

RCC documents CNetID password followed by Duo. Key-only exceptions require RCC
review; the current guide describes the exception process as PI-only. A
phone-only login must not be promised: expect password entry plus Duo approval.
Do not store the password or automate repeated authentication attempts.

## Target architecture and ownership

Application code belongs here: generic SSH transport selection, connection
lifecycle/status, login UI/API, tests, and documentation. Cluster names,
accounts, filesystem locations, socket mounts, host helper installation, and
image pins belong in agent-infra. Credentials and sockets stay outside Git.

Add OpenSSH transport alongside AsyncSSH; preserve AsyncSSH as the default so
existing configurations and non-socket backends keep working. Create the
smallest shared transport interface needed by the existing callers. Audit
command execution, script staging, log/file retrieval, and interactive process
handling; do not assume replacing one connect() call covers them all.

Useful entrypoints:

- `src/scripthut/ssh/client.py`, `config_schema.py`, `runtime.py`.
- `src/scripthut/terminal.py` and terminal/websocket handlers in `main.py`.
- `src/scripthut/api.py`, backend templates, `runs/manager.py`.
- Other SSHClient constructors in `cli.py` and `backends/ec2_ssm.py`.

The existing browser terminal is for an ALREADY authenticated connection and
expects AsyncSSH-like process methods. Introduce a minimal adapter where
necessary instead of duplicating the whole terminal implementation.

Use a dedicated OpenSSH master on the HOST, with a private directory such as
`/home/ed/.local/run/scripthut-ssh/` (0700), bind-mounted into the container.
Mount the directory rather than an individual socket, so replacement after
reauthentication is visible. Match UID/GID. Expose neither the whole .ssh
directory nor a container-engine socket. The master must survive a ScriptHut
container restart. It cannot survive a host reboot.

Keep the application's externally managed socket mode usable independently
of browser login. For browser-managed login, use the existing verified
localhost SSH transport to invoke a fixed host-side helper via a PTY. The
helper starts the dedicated OpenSSH master, interactively authenticates, and
detaches after success. Do not introduce a general-purpose host daemon or a
second web service. Prototype the detach/PTY lifecycle before building the UI.
Ship generic helper behavior with the feature; install/configure it through
agent-infra. Resolve hosts/users from configured profiles, not browser-supplied
shell command strings.

Defaults: 8-hour idle persistence, ServerAliveInterval=30,
ServerAliveCountMax=3, two concurrent automated operations per backend, verified
host keys. OpenSSH defaults to ten simultaneous sessions per connection;
RCC's actual limit is unknown. Queued/running Slurm jobs do not each consume
an SSH channel. Do not use keepalives to imply a guaranteed session lifetime.

Important: OpenSSH ordinarily falls back to a fresh connection if a socket is
unavailable. Automated transport must fail closed instead. An initial
`ssh -O check` alone is insufficient because the master can die before the
next command. Ensure the command invocation cannot open a new direct network
connection (including the check/use race), and test this explicitly.

## UI and authentication behavior

Backend card states: connected, disconnected, authenticating, connection
failed. Show last successful remote check and connected-since when known.
Expandable details show socket path, configured idle timeout, and last error.
An externally established connection's original creation time may be unknown;
show first-observed time as such. Never invent an expiry countdown.

Disconnected card: Connect button and a copyable terminal fallback command.
Connect opens a small login terminal/dialog which relays real OpenSSH/RCC
prompts. User enters their password and selects/completes Duo as prompted.
Do not hardcode Duo menu numbers, scrape fragile prompt strings, or promise
that every login is push-only. Connected cards need a Check connection action;
do not destroy a healthy shared connection just to refresh its status.

Only one login attempt per backend; repeated clicks attach to it or report it
already active. Explicit Cancel and a bounded authentication timeout (start
with five minutes). Abort an unfinished login when its owning browser session
closes. Once successfully detached, closing the dialog must leave the master
running. Never terminate the user's unrelated socket/master.

Treat the UI/API as an authentication surface. Require the intended private
access boundary; reject cross-origin WebSocket and state-changing requests,
and add appropriate CSRF/session binding. Use server-configured backend IDs.
Login input and authentication transcripts must not enter command logs,
terminal replay/history, files, diagnostics, or persistent state. Password
input follows terminal no-echo behavior and is only transiently relayed.
Do not add a password vault or automatic MFA retries. Failed login is a
user-visible result, not a trigger for more attempts or pushes.

## Disconnection and submission correctness

Distinguish a socket pathname existing, its master answering a control check,
and an actual remote command succeeding. Show errors without discarding cached
job state. Back off health checks; don't flood a disconnected login node.

While disconnected, existing Slurm jobs continue, monitoring is stale, and
new submissions pause. Never mark a job failed just because SSH disappeared.
Manual authentication restores monitoring and scheduling.

Submission is special: SSH can disconnect AFTER sbatch accepts a job but
BEFORE the response arrives. Do not blindly resubmit on reconnect. Persist a
unique attempt marker before submission, pass it through an appropriate Slurm
metadata field, and reconcile with squeue/sacct scoped to cluster/user before
retrying. Assess interaction with existing user comments and accounting
permissions. If the outcome cannot be established, retain an explicit unknown
submission state for user reconciliation rather than silently duplicating work.
Test this race, including across controller restart. Read-only operations can
be retried separately; do not treat all SSH failures with one retry policy.

## Delivery sequence and verification

1. Feature branches and clean test baseline; port the two existing fixes.
2. OpenSSH transport plus external socket configuration, health/status UI,
   and terminal fallback. Prove commands, scripts, logs, files, and terminal
   sessions work. This is an independently useful upstream contribution.
3. Host helper and browser-assisted authentication. Add lifecycle and endpoint
   tests before asking the user to exercise password/Duo through the browser.
4. Deploy a pinned fork image via agent-infra; configure midway3 and run bounded
   live acceptance. Add further clusters incrementally after midway3 is proven.

Local tests should use a disposable SSH server/fake process fixtures; ordinary
tests must not require RCC credentials or send Duo pushes. Cover socket absence,
stale files, replacement, permission errors, multiplexing-limit errors,
connection loss, safe argument handling, no direct fallback, task concurrency,
uncertain submission, and existing AsyncSSH backend regression cases.

Browser tests: successful login, cancel, failure, timeout, duplicate clicks,
closed browser during login, rejected origins/unauthorized initiation, no
credential logging, and post-success master survival after UI/container exit.

Live midway3 checks: bounded success/failure, dependencies, log retrieval,
cancellation, short timeout, loss/reconnect while jobs continue, and controller
restart without duplicate submissions. Use tiny allocations and dedicated test
directories. Do not run computations on login nodes. Authentication requiring
the user's phone must be coordinated, not silently retried.

CI currently runs `pip install -e ".[dev]"`, `pytest --tb=short -v`, and
`scripthut --help` across Linux/macOS/Windows and Python 3.11–3.13. New POSIX
transport support should fail clearly when selected on unsupported platforms;
do not break imports or existing backends on Windows. Record baseline failures
separately. Add targeted tests, then run the relevant suite and full pytest
before deployment. Ruff/mypy configuration exists; avoid unrelated formatting.

For production: back up state/config, retain the current working image, and
pin the fork image to a specific commit and registry digest. Publishing the
image and switching the live service should follow the current session's
authorization. No automatic upgrades. Remove the two build-time patches only
when the deployed fork includes and tests their equivalent source fixes.

An upstream design proposal is sensible before the browser login work, but
prepare it for the user's review rather than contacting maintainers unasked.

## Primary references

- RCC SSH/authentication: https://docs.rcc.uchicago.edu/connection/ssh/main/
- OpenSSH multiplexing and keepalives: https://man.openbsd.org/ssh_config
- Server session limits: https://man.openbsd.org/sshd_config#MaxSessions
- ScriptHut installation/authentication: https://github.com/tlamadon/scripthut/blob/main/docs/installation.md

RCC documents password/Duo and limited key exceptions. The socket integration
and browser-assisted login described here are our design, not an RCC-approved
ScriptHut recipe or an existing upstream capability.
