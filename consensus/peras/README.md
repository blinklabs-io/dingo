# consensus/peras

Package path for Ouroboros Peras support in Dingo. It contains no logic yet.

Peras is specified in [CIP-0140](https://cips.cardano.org/cip/CIP-0140).
Praos block production is unchanged; voting committees elected per round cast
votes that aggregate into certificates, and a certificate boosts a block in
chain selection.

Wire and ledger types live in `gouroboros`, not here. Conformance vectors for
this package are loaded by `internal/test/conformance` (see its README).
