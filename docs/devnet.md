# Local DevNet

`devmode.sh` runs one Dingo process against the checked-in DevNet genesis
configuration. It is useful for exercising startup, block production, and
transaction submission locally without a reference node.

Run it from the Dingo repository on Linux; the helper uses GNU `date` and
`sed` options:

```sh
./devmode.sh
```

For debug logging:

```sh
DEBUG=true ./devmode.sh
```

Each run rewrites the start times in
[`config/cardano/devnet/`](../config/cardano/devnet/) and removes the contents
of `.devnet/`, which is the local database directory. Keep any edits to those
genesis files and any `.devnet` data you need outside these paths before
running the script.

The Shelley genesis configuration uses one-second slots, 600-slot epochs, a
security parameter of 100, and an active slot coefficient of 1.0. These
settings make local epoch transitions and block production quick to observe.
