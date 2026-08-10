# Demand Execution — moved

This document now lives in **`aibs-informatics-cdk-lib`**:

> `docs/design/demand-execution.md`
> <https://github.com/AllenInstitute/aibs-informatics-cdk-lib/blob/main/docs/design/demand-execution.md>

## Why it moved

The demand execution reference spans all four repos, and no single one holds a majority of
its citations (`aws-lambda` 35%, `cdk-lib` 27%, `aws-utils` 20%, `core` 18%). It sits in
`cdk-lib` because that repo is at the top of the dependency stack — its app builds this
repo's Docker image from `main` at synth time — so it is the only one that can reference all
four without inverting the dependency. It also owns `DemandExecutionStack` and the reference
app, which is what the document actually describes as "deployed today."

## What is still documented here

The handlers themselves. This repo owns:

- `handlers/demand/scaffolding.py` — resolves volumes, derives paths, builds setup/cleanup configs
- `handlers/demand/context_manager.py` — all path derivation, sync-request generation, and the
  container's command and environment
- `handlers/data_sync/` — the transfer, scan, and remove handlers
- `common/handler.py`, `main.py`, `docker/docker-entrypoint.sh` — the Lambda-vs-Batch dispatch that
  makes one image serve both roles

The design reference explains how those pieces are wired together and why. Read it there.
