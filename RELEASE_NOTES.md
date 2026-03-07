---
title: Release notes
---

# Release notes

## Dingo v0.22.0: faster bootstrap, optional APIs, and block production

**Date:** March 7, 2026  \
**Version:** v0.22.0

Hi folks! Let’s dive in—this release makes it easier to spin up Dingo, run optional APIs, and produce blocks.

### ✨ What's New

- **Snapshot bootstrap (Mithril)**: Bootstrap the node from Mithril snapshots so you can start from an existing chain snapshot instead of syncing from genesis.
- **Optional HTTP APIs**: Run optional HTTP APIs alongside the node with Blockfrost-compatible and Mesh (Rosetta-compatible) endpoints for common integrations.
- **Block production**: Produce and broadcast blocks as a stake pool operator using your configured keys and leader schedule.
- **Governance tracking**: Track Conway-era governance so governance state stays queryable and rollbacks behave consistently.
- **Leios mode**: Try the new experimental Leios run mode to explore early Leios protocol support.
- **Block archive server**: Serve blocks through the built-in block archive server with signed URLs for safer object storage access.

### 💪 Improvements

- **Smarter validation near the tip**: Sync faster and safer by reducing historical validation until the node is close to the tip while still validating within a stability window.
- **More resilient chain synchronization**: Improve chain synchronization under forks, stalls, and multi-peer operation to reduce stuck states and speed up recovery.
- **More complete rollback support**: Restore ledger and database state cleanly to an earlier slot with deeper, more consistent rollback handling.
- **More efficient encoded-data caching**: Configure and streamline encoded chain data storage and caching to reduce overhead while keeping cache metrics visible.
- **More accurate stake snapshots**: Get more accurate stake snapshots and stake distribution queries for better scheduling and API responses.
- **More reliable leader scheduling**: Make leader election and epoch/slot tracking more reliable across epoch boundaries and snapshot/import scenarios.
- **Tighter peer controls**: Harden network and peer management with stricter validation, caps, connection limits, and improved node-to-node links.
- **More predictable transaction pool behavior**: Keep the transaction pool more predictable under pressure with eviction watermarks, TTL cleanup, and deadlock-resistant validation.
- **Better observability**: Beef up observability with additional Prometheus metrics, forging metrics, and cleaner structured logging.
- **Expanded docs**: Expand docs and operational guidance to make setup and architecture easier to understand.

### 🔧 Fixes

- **Safer event delivery**: Prevent event delivery deadlocks and goroutine leaks with non-blocking, time-bounded subscriptions and safer shutdown coordination.
- **Stricter header verification**: Tighten header verification and cryptographic checks across epochs and key formats to avoid accepting invalid chain data.
- **Safer slot/epoch conversion**: Harden slot/epoch conversion and time handling around genesis and edge conditions to prevent underflow and divide-by-zero issues.
- **More robust block fetching**: Make chain synchronization and block fetching more robust when peer or chain tips change unexpectedly to reduce stalls and invalid requests.
- **Safer fee and execution budgeting**: Handle fee calculation and execution budgeting edge cases safely with overflow checks and budget-aware forging.
- **Safer object storage keys**: Make object storage keys safer and more interoperable by hex-encoding S3 and Google Cloud Storage keys and decoding them on listing.
- **Config and file hardening**: Improve security and operational hardening around configuration and file handling with size limits, safer permissions, and request body limits.

### 📌 What You Need to Know

- **No action required by default**: All new API servers and run modes are optional, so you can enable them only when you need them.

### 🙏 Thank You

Thank you for the feedback and issue reports that helped us streamline syncing, rollback, and observability.
