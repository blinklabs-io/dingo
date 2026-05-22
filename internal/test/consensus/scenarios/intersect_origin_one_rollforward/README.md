# intersect_origin_one_rollforward

Smallest possible consensus-capture scenario. Smoke-tests the whole
pipeline end to end with the absolute minimum protocol surface:

1. Capture-sidecar handshake against a single-pool cardano-node.
2. `find_intersect [origin]` →  cardano-node replies `intersect_found`.
3. `request_next` → cardano-node replies `roll_forward` carrying the
   first forged block.
4. Sidecar writes a `category=consensus` JSON vector and exits 0.

The vector has exactly one `peers` entry (`peer_id: 0`) whose `served`
array contains the captured RollForward;
`expected_output.downstream_chainsync` mirrors that served trace
(single peer = no chain-selection ambiguity);
`expected_output.final_tip` matches the RollForward's tip.

## How to run

From this directory (or via the dispatcher):

```bash
# Via the dispatcher — discovers this scenario by name.
../../capture-scenario.sh intersect_origin_one_rollforward -out /tmp/vector.json

# Or directly.
./run.sh -out /tmp/vector.json
```

`--keep-up` leaves the docker-compose stack running on success — useful
when poking the produced vector or the running cardano-node by hand.

## Stack contents

| Service           | Role                                                     |
|-------------------|----------------------------------------------------------|
| `configurator`    | Genesis + config generation. Exits 0 after seeding.      |
| `cardano-node`    | Single forging cardano-node. The capture oracle.         |
| `capture-sidecar` | Runs `cmd/capture-sidecar` against cardano-node.         |

Subnet: `172.23.0.0/24`. Host port (overridable):
`CONSENSUS_CAPTURE_CARDANO_PORT` → cardano-node (default 3030).
