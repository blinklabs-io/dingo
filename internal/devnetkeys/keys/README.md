# DevNet test keys

Signing keys and certificates for the bundled local DevNet in
`config/cardano/devnet`. Unit tests use them to forge and verify blocks, and
`dingo devnet` copies the producer credentials into its private state directory.
They secure nothing and must never be used on a real network.

These files are separate from the embedded network configuration. Only the
VRF key, KES key, and operational certificate are embedded for the explicit
local-devnet command; normal node configuration still gets production key
paths from the operator.
