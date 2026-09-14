# Maintained production releases

The `production` branch in `EdJeeOnGitHub/scripthut` owns the application code used
by agent-infra. Changes go through this branch and its test workflow before release.
Upstream integration is separate; deployment does not apply external patches.

Production tags use `production-YYYY-MM-DD.N` and are annotated and immutable.
Create a tag only after the test suite and candidate acceptance pass. Never move
an existing release tag; corrections require a new release.

`Containerfile.production` is the supported local production build. Build from a
clean archive of a full Git commit and pass that commit as `SOURCE_REVISION`.
It intentionally retains the verified digest-pinned upstream runtime base, including
its dependency installation. Changing dependencies requires updating and validating
that base in a separate release. The development Dockerfile is not the production
recipe. No controller images are published by this workflow.

Agent-infra owns the release manifest, host configuration, build receipts and staged
rollout. Its manifest pins this repository's full commit and release tag. Every
candidate and promotion uses the exact locally built image from its validated
receipt; independently rebuilt images need their own validation.
