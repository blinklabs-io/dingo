# Compiled-in Plugin Development

Dingo plugins are compiled-in Go providers registered explicitly on an
application-owned `plugin.Host`. Registration never happens in `init`, provider
configuration is decoded before construction, and the host owns `Start`/`Stop`.

Storage contracts remain in `database/plugin/blob` and
`database/plugin/metadata`; the generic `plugin` package does not import either
domain. The same platform also hosts mempool and API providers.

## Provider shape

Define a typed provider config, use the domain dependency bundle for shared
settings, and register a typed factory:

```go
type Config struct {
    Bucket string `yaml:"bucket"`
}

func RegisterProvider(host *plugin.Host) error {
    return plugin.Register(
        host,
        plugin.Descriptor{
            Capability:  plugin.CapabilityStorageBlob,
            Name:        "example",
            Description: "Example blob store",
        },
        func() Config { return Config{} },
        func(
            ctx context.Context,
            cfg Config,
            deps blob.ProviderDependencies,
        ) (*Store, plugin.Instance, error) {
            store, err := New(cfg.Bucket, deps.Logger)
            if err != nil {
                return nil, nil, err
            }
            lifecycle := plugin.Lifecycle{
                StartFunc: func(ctx context.Context) error {
                    if err := ctx.Err(); err != nil {
                        return err
                    }
                    return store.Start()
                },
                StopFunc: func(context.Context) error { return store.Stop() },
            }
            return store, lifecycle, nil
        },
    )
}
```

Keep shared node settings such as data directory, storage mode, logger, and
metrics registry in the dependency bundle. A local storage provider may expose
a typed `dataDir` override while using the injected directory as its fallback;
this preserves the application-wide database-path shortcut. Other provider
config contains only provider-specific settings. Configuration decoding rejects
unknown fields.

Register always-built providers in `internal/plugins/register.go`. Register
optional providers in `internal/plugins/register_extra.go`, guarded by
`dingo_extra_plugins`. Add the provider name to the known optional-provider
map so an untagged binary returns the actionable build-tag error.

## Configuration

```yaml
plugins:
  storage:
    blob:
      provider: example
      config:
        bucket: blocks
```

The selector flag is `--blob example`. Generic environment paths flatten the
capability and config field names, for example:

```text
DINGO_PLUGINS_STORAGE_BLOB_PROVIDER=example
DINGO_PLUGINS_STORAGE_BLOB_CONFIG_BUCKET=blocks
```

Precedence is selector CLI flag, generic plugin environment, YAML, then
provider defaults. There are no provider-specific flags, mutable global option
destinations, or name-based storage constructors.

## Lifecycle and tests

Factories should construct resources without starting background work.
`Start` begins the provider and `Stop` must be idempotent. If startup fails,
the host stops that provider before returning the error. Composition code owns
unwinding previously started providers around their non-plugin dependents.

The built-in storage stores expose `Start`/`Stop` with no context parameter, so
the lifecycle adapter honors cancellation only at the boundary: `StartFunc`
returns early on `ctx.Err()` before beginning work, but it cannot interrupt a
`Start` already in progress. `StopFunc` runs cleanup regardless of `ctx` state.
If your store's own start/stop accept a context, thread it through instead.

Test strict configuration decoding, construction/start failures, normal stop,
and the subsystem contract. Storage providers must preserve transaction,
iterator-lifetime, commit-timestamp, optimization, and persisted-format
semantics. Run both:

```text
go test ./...
go test -tags dingo_extra_plugins ./...
```

## Conformance tests

`internal/test/storagetest` is a shared conformance suite that checks every
storage plugin against the same behavioral contract instead of each plugin
inventing its own CRUD test shape -- CRUD and large-payload round-trips,
transaction/rollback/iteration semantics, concurrent access, an
operation-timeout bound, and (as standalone per-plugin tests alongside it)
unreachable-endpoint, bad-credential, and resource-cleanup checks. See
[`internal/test/storagetest/README.md`](../../internal/test/storagetest/README.md)
for what each check covers, the environment variables each cloud/database
backend reads, and CI availability.

Add a new plugin's conformance test as a thin `conformance_test.go` in its
own package, in-package so it can use the plugin's real constructor:

```go
func TestBlobStoreConformance(t *testing.T) {
    storagetest.RunBlobStoreConformance(t, func(t *testing.T) blob.BlobStore {
        store, err := New(WithBucket(bucket))
        require.NoError(t, err)
        require.NoError(t, store.Start())
        t.Cleanup(func() { require.NoError(t, store.Stop()) })
        return store
    })
}
```

`newStore` is called once; the harness reuses that store across every
subtest, so construction against a real bucket or database stays cheap.
Cloud- or database-backed plugins skip cleanly (never fail) when their
backend is not configured, following the same convention this repository
already uses for cloud credentials and CI database services.

`internal/integration/storage_migration_test.go` covers a distinct
concern -- migrating data between two different plugins, not just each
plugin in isolation -- by writing a small dataset through one backend's
typed API and replaying the exact retrieved values into a second backend.
