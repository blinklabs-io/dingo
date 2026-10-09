# Single-node local DevNet

Run Dingo's bundled private network with one command:

```sh
# From a Dingo checkout, after `make build`:
./dingo devnet

# Or use the published npm package (Node.js 22+ and tar required):
npx @blinklabs/dingo devnet
```

The command starts one Dingo node with block production enabled and no outbound
peers. It uses the bundled DevNet genesis and test keys, so it does not need
Docker, a Cardano node, or a separate genesis-generation step.

Serving with `runMode: dev` always selects API storage and Dingo's regular
keyed block producer, regardless of the configured `storageMode` or
`blockProducer` values. The configuration still needs valid VRF, KES, and
operational-certificate credentials; `dingo devnet` provisions local test keys
for that purpose.

By default, configuration, test keys, and the database live in a private
temporary directory that is removed when the node exits, including after
Ctrl+C. Each invocation starts a fresh chain and leaves the current directory
untouched.

To retain and reuse a chain, give Dingo a state directory:

```sh
./dingo devnet --data-dir ./.dingo-devnet
```

Run the same command again to resume that chain. Reset it with:

```sh
./dingo devnet --data-dir ./.dingo-devnet --reset
```

The first run requires an empty directory. Dingo places a marker there before
creating its database, generated config, and test-key copies. `--reset` only
recreates those Dingo-managed paths; other files in the directory are kept.
Dingo rejects another invocation using the same directory, including attempts
to reset it while its node is running. Copied state directories are rebound to
their new database, config, and key paths when reused.
These keys and this network are for local testing and must not be used with
real funds.

For multi-node consensus and reference-conformance scenarios, use the separate
[DevNet integration harness](../internal/test/devnet/README.md).
