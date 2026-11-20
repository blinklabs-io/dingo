# v0.94.2 API-mode database

`database.tar.zst` is the data directory a binary built from the `v0.94.2`
release tag (commit `ea9a4b24c03d6c1c74cb670965d3de2a60d63625`) leaves after

```sh
dingo --network preview --storage-mode api --data-dir <dir> load <immutable>
```

where `<immutable>` holds chunks `00000` through `00020` of
`database/immutable/testdata`: all of epoch 0 and the first chunk of epoch 1,
ending at slot 90700, block 4535. It is the old shared layout as that release
wrote it: one SQLite metadata store in API storage mode holding the ledger
state, the UTxO set and the API rows, beside the Badger blob store.

`TestMigratedNodeServesTheSameAPIsAsAFreshOne` unpacks it, runs the startup
migration to the storage-role layout over it, and compares every API answer
with a node loaded from the same chunks by the current code.

`generate.sh` rebuilds the archive from the `v0.94.2` tag. The archive is not
byte-reproducible: the blob store records a new identifier on every run.
