# OpenSSH sockets and browser-assisted login

ScriptHut can reuse authenticated OpenSSH control sockets for remote backends.
AsyncSSH remains the default; OpenSSH transport is opt-in. External socket reuse
works independently of browser-assisted authentication.

## Ownership

ScriptHut owns the transport implementation, submission recovery, login lifecycle,
UI and tests. Deployment configuration owns host profiles, account names, socket
mounts and service installation. Keep operational inventories and acceptance
records in a private deployment repository, using generic examples in public docs.

A host-owned OpenSSH master exposes a control socket in a private directory.
Containers mount the directory so replacement sockets remain visible. Automated
operations reuse the configured socket and must not initiate interactive login
when it is unavailable. Connection loss makes monitoring stale; it does not prove
that a scheduler job failed or authorize automatic resubmission.

## Browser authentication

Browser-assisted login invokes a configured host helper through a PTY. The browser
selects a configured profile, never an arbitrary destination or command. Origin
checks, CSRF protection and session ownership bind the terminal to its login attempt.
The single-user controller requires verified private network access; the login UI
is not a replacement for that access boundary.

Passwords and authentication transcripts must not be persisted or logged. A
successfully detached master can survive browser and controller exit. New host
sessions may require the user to authenticate again.

## Validation

Tests use disposable SSH servers and fake processes. They cover socket reuse,
disconnection, uncertain submission recovery, terminal ownership, cancellation,
timeouts and secret exclusion. Real deployment checks and their identifying
host/account/job details belong in private operational records.

See [backend configuration](../configuration/backends.md) and the
[durable submission contract](../submission-contract.md) for supported interfaces.
