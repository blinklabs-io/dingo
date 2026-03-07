---
title: Release notes
---

# Release notes

## v0.22.0 (March 7, 2026)

Hi folks! We’re excited to share what’s included in v0.22.0.

### ✨ What's New

- **Built-in HTTP APIs**: We rolled out built-in HTTP APIs so external services can query chain and ledger data without custom integrations.
- **More complete Mithril bootstrapping**: You can now bootstrap a node from Mithril snapshots more completely, cutting down the time and work needed to get a usable node from existing network state.
- **Signed-URL block archive path**: You can now use a new archive path to fetch blocks via signed URLs, which helps when blocks are stored remotely and need controlled access.
- **Leios run mode (experimental)**: You can now enable a new Leios execution mode to start experimenting with early Leios protocol plumbing.
- **Docker-based Cardano DevNet**: You can now spin up a reproducible Cardano DevNet setup, making end-to-end testing easier and more consistent.

### 💪 Improvements

- **Faster, more reliable chainsync**: Syncing is now blazing fast and more reliable, especially during catch-up and fork scenarios.
- **More efficient near-tip validation**: Ledger validation during initial sync is now more efficient so you can reach a usable tip sooner.
- **More production-ready forging**: Block production is now easier to operate, improving correctness and observability when forging is enabled.
- **Storage and query performance**: Storage and query performance improved, streamlining CBOR-heavy data paths and larger metadata workloads.
- **More complete governance and stake tracking**: Governance and stake tracking are now more complete, improving ledger-driven governance and pool/stake insights.
- **More predictable networking and peer management**: Networking and peer management behavior is now more predictable under load, improving stability when many peers connect.
- **Safer, more actionable events and metrics**: Events and observability are now safer and more actionable so you can integrate listeners without risking deadlocks.
- **Clearer docs and operator guidance**: Developer documentation and operator guidance are clearer, making it easier to run and extend the node.
- **More consistent transaction validation across eras**: Transaction validation is now more consistent across eras, improving correctness for older or transition-era transactions.

### 🔧 Fixes

- **Safer rollbacks**: Rollback handling is now safer, reducing the chance of inconsistent on-chain state after forks or operator-triggered rollbacks.
- **Fewer deadlocks and races**: Several deadlocks and race conditions were eliminated, improving stability under concurrent load and event-heavy scenarios.
- **Stricter block/header verification**: Block and header verification is now stricter and more correct, preventing invalid blocks from being accepted during sync.
- **More robust fee and ExUnits arithmetic**: Fee and execution-unit arithmetic is now more robust, preventing overflows and edge-case crashes.
- **Input validation and security hardening**: Input validation and security hardening were improved to reduce misuse and unsafe file/path behaviors.

### 📋 What You Need to Know

- **Event name changes**: If you rely on event type strings, you may need to update your integrations because several event type constants were renamed.
- **Tiered storage affects metadata**: If you enable API-focused storage modes, you may see different metadata persistence behavior depending on configuration.
- **Forging config and key permissions**: If you run with forging enabled in production, review your key and forging configuration before upgrading to avoid startup failures.

### 🙏 Thank You

Thank you for trying Dingo—let us know what you think!
