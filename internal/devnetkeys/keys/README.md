# DevNet test keys

Signing keys and certificates for the bundled local DevNet in
`config/cardano/devnet`. Unit tests use them to forge and verify blocks, and
`dingo devnet` copies the producer and faucet credentials into its private
state directory.
They secure nothing and must never be used on a real network.

These files are separate from the embedded network configuration. The VRF
key, KES key, operational certificate, and faucet signing key,
verification key, and address are embedded for the explicit
local-devnet command; normal node configuration still gets production key
paths from the operator.

`faucet.addr` is the enterprise address derived from `faucet.vkey`. It has
900,000 test ADA in the bundled Shelley genesis and can be spent with
`faucet.skey`. These are public test credentials.
