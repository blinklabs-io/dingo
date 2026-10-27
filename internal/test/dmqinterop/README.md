# DMQ interoperability tests

These tests run Dingo's DMQ stack against a real Intersect
[dmq-node](https://github.com/IntersectMBO/dmq-node). They exist because an
in-process pair of Dingo stacks cannot show that the bytes on the wire are the
ones dmq-node expects. `dmq/sigsub_messages_test.go` pins the encodings byte
for byte; these tests prove the whole exchange.

## Running them

```sh
internal/test/dmqinterop/run.sh
```

It needs Docker and network access, and takes about twenty minutes, most of it
waiting for the devnet's second epoch change. `run.sh`:

1. downloads a dmq-node release and checks its SHA-256 against the one the
   release publishes (`DMQ_NODE_VERSION`, default 0.7.2.1; the Linux binary
   runs in a container, never on the host);
2. builds Linux builds of Dingo and of `dmq-submit`, a small client that
   submits a message to a dmq-node local socket;
3. starts Dingo's single-node devnet (network magic 42) and dmq-node in one
   container, with dmq-node using the devnet as its Cardano node, so the
   devnet pool's stake is the real thing dmq-node checks;
4. runs the tests tagged `dingo_dmq_interop`.

Without the tag, or without the `DMQ_INTEROP_*` variables `run.sh` sets, the
tests are not built or are skipped, so `make test` is unaffected.

## What they cover

| Test | What it shows |
| --- | --- |
| `TestInteropDingoDialsDmqNode` | The handshake settles on version 2 and the connection stays up with keep-alive on mini-protocol 12. |
| `TestInteropDmqNodeServesAMessageToDingo` | A message signed by the devnet pool's real KES key, accepted by dmq-node, is pulled by Dingo, which checks the signatures and certificate itself. |
| `TestInteropDmqNodePullsAMessageFromDingo` | dmq-node dials Dingo's peer listener, pulls a message from it and accepts it; a second Dingo stack then fetches it from dmq-node, which proves dmq-node kept it. |
| `TestInteropPeerManagerDialsDmqNode` | Dingo's peer manager dials dmq-node from a topology, as the node does, and the connection holds. |
| `TestInteropNodeExchangesMessagesWithDmqNode` | Node mode only. A real Dingo binary with `dmq.enabled` dials dmq-node from its topology and accepts dmq-node's connection. A message dmq-node accepted reaches a consumer on the node's local socket after the node authenticates it against its own ledger, and a message the node accepted on that socket is accepted by dmq-node. |

dmq-node accepts a pool's messages only once the pool's set stake snapshot,
which it reads from the Cardano node once per epoch, is nonzero. On the devnet
that is after the second epoch change, so the tests retry until
`DMQ_INTEROP_TIMEOUT` (default 30 minutes).

To iterate without waiting for an epoch change each time, start the environment
once with `DMQ_INTEROP_SETUP_ONLY=1`, which prints the variables the tests need
and leaves the container running, then rerun `go test -tags dingo_dmq_interop`
against it. `DMQ_NODE_SEVERITY=Debug` makes dmq-node log every mini-protocol
message. Remove the container (`docker rm -f`) when done.

## Variables

| Variable | Meaning |
| --- | --- |
| `DMQ_NODE_VERSION` | dmq-node release to test. |
| `DMQ_INTEROP_NODE_PORT` | Host port for dmq-node's peer port (default 3141). |
| `DMQ_INTEROP_LISTEN_PORT` | Host port Dingo listens on for dmq-node to dial in (default 3999). |
| `DMQ_INTEROP_TIMEOUT` | How long a test waits for the pool's stake (default 30m). |
| `DMQ_INTEROP_SETUP_ONLY` | Start the environment, print the test variables, and leave it running. |
| `DMQ_NODE_SEVERITY` | dmq-node's log level (default `Info`). |
| `DMQ_INTEROP_WORKDIR` | Where downloads and builds are kept. |
| `DMQ_INTEROP_KEEP` | Keep the container and working directory afterwards. |
| `DMQ_INTEROP_NODE_MODE` | Also run a native Dingo with `dmq.enabled` and run only the node-mode test. It binds Dingo's default ports (3001, 3002, 12798) and `DMQ_INTEROP_LISTEN_PORT` on this machine. |
| `DMQ_INTEROP_METRICS_PORT` | The node's metrics port in node mode (default 12798), for a machine where that port is in use. |
| `DMQ_INTEROP_CARDANO_BIN` | A Linux Dingo binary for the Cardano node dmq-node talks to; see below. |

## The Cardano node's idle limit

Dingo closes a node-to-client connection after two minutes without traffic.
dmq-node keeps one open to its Cardano node and sends nothing between epochs,
so against a Dingo with that limit it exits within minutes with `BearerClosed`.
Until dmq-node is exempted, point `DMQ_INTEROP_CARDANO_BIN` at a Linux build of
Dingo from before the limit for the Cardano node. The Dingo under test is
always built from this tree.

## gouroboros version

`TestInteropDmqNodeServesAMessageToDingo`,
`TestInteropDmqNodePullsAMessageFromDingo` and
`TestInteropNodeExchangesMessagesWithDmqNode` need a gouroboros whose
`MessageAuthenticator`
verifies a message's KES signature over the payload's raw CBOR, which is what
dmq-node signs and verifies. gouroboros v0.214.0 does; earlier releases verify a
copy wrapped in a CBOR byte string, so Dingo would reject every message
dmq-node signs and dmq-node would reject every message Dingo's authenticator
accepts. CIP-0137's `messageId`, `Blake2b-256(cbor(messagePayload))`, hashes
the same unwrapped bytes.

## What dmq-node requires

Things the tests learned the hard way, for anyone writing a DMQ client:

- A message body is 90 to 2000 bytes, a KES signature 448, and the
  operational certificate's key and signature 32 and 64.
- The KES signature covers the payload array's raw CBOR, as it appears in the
  message, not a byte-string wrapped copy.
- dmq-node will not start without a Cardano node socket and a Shelley genesis
  file, and it exits if the socket refuses a connection.
- The item list in messages 2, 4 and 5 is an indefinite-length array; a
  definite-length one is refused.
- A blocking request must be answered within 20 seconds, with
  `MsgReplyNoSigIds` if there is nothing to announce; a non-blocking request
  and a request for bodies within 10. dmq-node ends the connection otherwise.
- The pulling side is the connection's initiator and starts eagerly.
- dmq-node only pulls from a peer it chose, so a node that dials it is served
  but not pulled from unless it is in dmq-node's topology.
- A pool may send one message a minute (`SigTooFrequent` otherwise).
- dmq-node 0.7.2.1 never announces the first message its mempool holds, to
  peers connected before or after it. The tests therefore send a throwaway
  message first and wait out the one-minute limit.
