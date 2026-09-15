# Maintained production releases

The `production` branch in `EdJeeOnGitHub/scripthut` owns the application code used
by agent-infra. Changes go through this branch and its test workflow before release.
Upstream integration is separate; deployment does not apply external patches.

Production tags use `production-YYYY-MM-DD.N` and are annotated and immutable.
Publish a tag after the application test suite passes. The deployment repository
then validates the pinned tag and release pair before promotion. Publishing a tag
or merging application changes does not deploy them. Never move an existing
release tag; corrections require a new release.

`Containerfile.production` is the supported local production build. Build from a
clean archive of a full Git commit and pass that commit as `SOURCE_REVISION`.
It intentionally retains the verified digest-pinned upstream runtime base, including
its dependency installation. Changing dependencies requires updating and validating
that base in a separate release. The development Dockerfile is not the production
recipe. No controller images are published by this workflow.

Agent-infra owns the release manifest, host configuration, build receipts and
production rollout. Its manifest pins this repository's full commit and release tag. Every
candidate and promotion uses the exact locally built image from its validated
receipt; independently rebuilt images need their own validation.

To update a managed instance, follow `docs/controller-release.md` in the private
agent-infra repository. After updating its release manifest and passing its CI,
run these commands from the agent-infra checkout on the deployment host:

```sh
bin/scripthut-release update
bin/scripthut-release status
```

`update` builds, validates and promotes the exact release, coordinates affected
services, backs up state, verifies preserved runs and cleans up its candidates.
Use `bin/scripthut-release update --check-only` to validate without promoting.
The deployment runbook owns failure recovery; old phase-specific scripts are
historical records. Hostnames, accounts and operational paths belong only in
that private repository.

The maintained production release targets Linux containers. Production-branch CI
runs the full suite on Linux with Python 3.11, 3.12 and 3.13. Unix sockets, POSIX
file locks and host OpenSSH integration are requirements; native Windows hosting
is not supported by this release. Main/develop retain their existing CI matrix.
The first published tag's Linux jobs passed; its inherited Windows jobs exposed
this platform mismatch. The branch's subsequent CI-only correction does not change
the tagged application or image recipe.
