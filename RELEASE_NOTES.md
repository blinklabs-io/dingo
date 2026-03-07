# Release notes

## Dingo v0.22.0

**Date:** March 7, 2026  
**Version:** v0.22.0

Hi folks! Here’s what’s new in this release.

<!--
NOTE: The block below is auto-injected structured data used to draft the
human-friendly release notes.
-->

{
  "✨ What's New": [
    "Bootstrap the node end-to-end from Mithril snapshots so you can start from an existing chain snapshot instead of syncing from genesis.",
    "Run optional HTTP APIs alongside the node with a minimal Blockfrost-compatible server and a Mesh (Rosetta-compatible) server for common ecosystem integrations.",
    "Forge and broadcast blocks in production stake pool mode using configured keys and leader schedules.",
    "Query and roll back Conway-era governance state consistently with built-in governance tracking.",
    "Experiment with Leios protocol integration using the new `leios` run mode.",
    "Serve blocks via the new Bark archive server and a proxy blob store with signed URLs."
  ],
  "💪 Improvements": [
    "Sync faster and safer by reducing historical validation until the node is close to the tip while still validating within a stability window.",
    "Improve chainsync resilience under forks, stalls, and multi-peer operation to reduce stuck states and speed up recovery.",
    "Restore ledger and database state cleanly to an earlier slot with deeper, more consistent rollback handling.",
    "Configure and streamline CBOR storage and caching for chain data to reduce overhead while keeping metrics visible.",
    "Get more accurate stake snapshots and stake distribution queries for better scheduling and API responses.",
    "Make leader election and epoch/slot tracking more reliable across epoch boundaries and snapshot/import scenarios.",
    "Harden network and peer management with stricter validation, caps, connection limits, and improved node-to-node links.",
    "Keep mempool behavior predictable under pressure with eviction watermarks, TTL cleanup, and deadlock-resistant validation.",
    "Beef up observability with additional Prometheus gauges, forging metrics, and cleaner structured logging.",
    "Expand docs and operational guidance to make setup and architecture easier to understand."
  ],
  "🔧 Fixes": [
    "Prevent event delivery deadlocks and goroutine leaks with non-blocking, time-bounded subscriptions and safer shutdown coordination.",
    "Tighten header verification and cryptographic checks across epochs and key formats to avoid accepting invalid chain data.",
    "Harden slot/epoch conversion and time handling around genesis and edge conditions to prevent underflow and divide-by-zero issues.",
    "Make chainsync and blockfetch more robust when peer or chain tips change unexpectedly to reduce stalls and invalid requests.",
    "Handle fee and execution-unit accounting edge cases safely with overflow checks and budget-aware forging.",
    "Make object storage keys safer and more interoperable by hex-encoding S3 and GCS keys and decoding them on listing.",
    "Improve security hardening around configuration and file handling with size limits, safer permissions, and request body limits."
  ]
}

