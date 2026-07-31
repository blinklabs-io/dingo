# Branch protection and the DevNet gate

This file documents the status check that gates consensus-sensitive changes, and
the conditions for making it required. Branch protection itself is configured in
repository settings; nothing in this repository can change it.

## The required check name

```text
devnet-gate
```

That is the `devnet-gate` job in `.github/workflows/devnet.yml`. It is the only
name in that workflow that is contract. The suite jobs report as
`devnet / dingo / suite` and `devnet / conformance / suite` through the reusable
workflow `.github/workflows/devnet-suite.yml`, and their names may change; do not
name them in branch protection.

`devnet-gate` always reports on a pull request. When a pull request touches no
consensus-sensitive path it completes through the `classify paths` job rather
than being omitted, so the check never sits pending and the name stays stable
whether or not the suites ran.

## Current status: not required yet

`devnet-gate` is deliberately not in the required set today.

The DevNet suites are intermittently red on unmodified `main`. Two baseline runs
on a 128-core host went 12 pass / 0 fail and 10 pass / 2 fail; the recurring
failures are `TestChainGrowthRate` and `TestSustainedConsensus`, with
`TestEpochBoundaryConsensus` failing occasionally. The cause is a node-side
liveness wedge on a depth-1 fork at the tip, tracked in
[#3029](https://github.com/blinklabs-io/dingo/issues/3029): a pending header for
the non-adopted branch stays latched, forged blocks are refused with `does not
match first pending header hash`, and blockfetch hot-loops the same single-block
range. Making the gate required before that lands would block every pull request
in the repository, including unrelated ones.

The gate is honest about this rather than papering over it: no
`continue-on-error`, no `|| true` around the suite, no retry-until-pass. A red
suite reports red.

## Making it required (follow-up, gated on #3029)

Once #3029 has landed and the DevNet suites pass on `main` (verify with the
scheduled `devnet` run, or a manual `workflow_dispatch` of it against `main`),
this is a single settings change:

1. Repository settings, Branches, the `main` protection rule.
2. Require status checks to pass before merging.
3. Add `devnet-gate`.

Nothing in the workflow needs to change at that point. Do the reverse (remove
`devnet-gate` from the required set) if the suites regress into sustained
flakiness again; leaving a known-red check required is worse than leaving it
advisory.

## What runs when

| Trigger | Suites | Path filtered |
|---|---|---|
| Pull request touching a consensus-sensitive path | all-Dingo and `--conformance` | yes, see `.github/devnet-paths.txt` |
| Pull request touching nothing consensus-sensitive | none; `devnet-gate` passes through the classify job | yes |
| Push to `main` | both | no |
| Tag `v*` (release candidates and releases) | both | no |
| Nightly schedule (04:30 UTC) | both | no |
| `workflow_dispatch` | selectable: all, dingo, or conformance | no |

The scheduled run exists to catch path-filter omissions and environmental drift
(base image changes, a republished `cardano-node` image, runner changes).

`devnet-gate` fails when a selected suite did not succeed, and also when a
selected suite reported no test count or zero tests started. A suite that never
reached `go test` cannot report green.

## Runner capacity

The suites drive real slot time: the all-Dingo network runs three producers plus
a relay against a 1-second-slot, 500-slot-epoch network while `txpump` submits
transactions, and the scenarios assert wall-clock chain growth and sustained
agreement. An under-provisioned runner fails on liveness, not on timeout, so a
small runner produces false red rather than a slow green.

A single all-Dingo run takes 840 to 1060 seconds of wall clock on a 128-core
host, including image build.

Measured on GitHub-hosted `ubuntu-latest` (4 CPUs, 15 GiB RAM, 87 GB free disk,
run 30660232485): both suites completed well inside the 90-minute job timeout,
17 minutes for conformance and 19 for dingo, image builds included. The
conformance suite passed there in full, including `TestChainGrowthRate`,
`TestSustainedConsensus`, and `TestEpochBoundaryConsensus`. The dingo suite
failed `TestDingoChainAdvances`, `TestChainGrowthRate`, and
`TestSustainedConsensus` while `TestEpochBoundaryConsensus` and
`TestBasicBlockForging` passed, and `txpump` reported 3462 accepted submissions,
so the network was live. That failure set is the same one seen on a 128-core
host, so it is not primarily a capacity result.

Two things follow. `ubuntu-latest` is a usable runner for these suites today,
which is why it is the default. But a larger runner is still the right target
before the check is made required, both for headroom and because a 4-CPU host
leaves nothing spare when the DevNet is competing with a race-instrumented
`go-test` job on the same queue.

Point the workflow at more capacity by setting repository variables:

| Variable | Meaning |
|---|---|
| `DEVNET_RUNNER` | `runs-on` label for both suite jobs. Unset means `ubuntu-latest`. Prefer a runner with 16 or more CPUs, 16 GB or more of RAM, and 40 GB or more of free disk. |
| `DEVNET_SYSTEM_START_DELAY` | Seconds the configurator puts between finishing key generation and slot 0. Unset means 30, the configurator default. Raise it only for a host that starts nodes behind genesis: `run-tests.sh` starts the Go scenarios as soon as the nodes are healthy and the scenario deadlines are wall clock from that point, so a larger delay spends scenario budget waiting for the chain to start. Measured on the 4-CPU runner above, dingo mode had 28 of the 30 seconds to spare, while conformance mode's `cardano-node` socket appeared about 4 seconds after slot 0. |

Setting `DEVNET_RUNNER` also switches the suite jobs into a shared serialized
concurrency group. That is required on any persistent (self-hosted or reused)
runner: the DevNet uses fixed container names, a fixed `172.20.0.0/24` subnet,
fixed host ports, and a single `devnet` Compose project, so two concurrent runs
on one machine collide. Serialization costs wall-clock time on busy days and is
not optional for correctness there.

Each run records the runner's CPU count, memory, disk, Docker version, the Dingo
commit under test, and the resolved `cardano-node` image digest in the job
summary and in the uploaded artifact, so a capacity-related failure is
distinguishable from a real one after the fact.

## Reproducing locally

```bash
# all-Dingo suite (default)
./internal/test/devnet/run-tests.sh

# Dingo beside cardano-node
./internal/test/devnet/run-tests.sh --conformance

# what the gate would decide for the current branch
git diff --name-only origin/main...HEAD > /tmp/changed.txt
./.github/scripts/devnet-path-filter.sh /tmp/changed.txt
```

`DEVNET_ARTIFACT_DIR=/some/dir` collects the same artifacts CI uploads (per-node
logs, generated genesis and configuration, container status, `txpump` log) before
teardown removes the volumes.

## Other checks in this repository

For reference, the other workflows that report on a pull request are `go-test`
(`.github/workflows/go-test.yml`, Linux with race detection plus macOS and
Windows), `golangci-lint`, `Docker CI`, and `Conventional Commits`. Which of them
are required is configured in repository settings and is out of scope for this
file.
