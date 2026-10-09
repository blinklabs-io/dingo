# DevNet test keys

Signing keys and certificates for the bundled DevNet genesis in
`config/cardano/devnet`. They exist only so unit tests can forge and verify
blocks against that genesis. They secure nothing and must never be used on a
real network.

They sit outside `config/cardano` because everything under an embedded network
directory is compiled into the binary. Production key paths are always
supplied by the operator (`shelleyVrfKey`, `shelleyKesKey`,
`shelleyOperationalCertificate`) and none is embedded by default.
