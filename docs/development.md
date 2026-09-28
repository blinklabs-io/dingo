# Dingo development

Use the Go version declared in [`go.mod`](../go.mod) and GNU Make.

## Build and test

From the repository root:

```sh
make          # format and build
make build    # build the command binaries
make test     # run the test suite with race detection
```

For a focused test, name the package and test directly:

```sh
go test -race -run TestName ./ledger
```

The Makefile also provides `make lint`, `make docs-parity`,
`make config-parity`, and `make sql-check`. After changing SQL queries, run
`make sql` to regenerate the checked-in code before `make sql-check`.

## Local development workflows

- [Run Dingo on the local DevNet](devnet.md).
- [Run benchmarks and collect profiles](benchmarks.md).
- Read the [architecture](../ARCHITECTURE.md) and [database design](../DATABASE.md)
  when a change affects component ownership or persisted state.
- Use the [documentation index](README.md) to find package, integration, and
  operator documentation.
