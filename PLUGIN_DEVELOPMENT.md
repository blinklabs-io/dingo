# Plugin Development Guide

This guide explains how to develop plugins for Dingo's storage system.

## Overview

Dingo supports pluggable storage backends through a registration-based plugin system. Plugins can extend the system with new blob storage (blocks, transactions) and metadata storage (indexes, state) implementations.

## Plugin Types

### Blob Storage Plugins
Store blockchain data (blocks, transactions, etc.). Examples:
- `badger` - Local BadgerDB key-value store
- `gcs` - Google Cloud Storage
- `s3` - AWS S3

### Metadata Storage Plugins
Store metadata and indexes. Examples:
- `sqlite` - SQLite relational database

## Plugin Interface

All plugins must implement the `plugin.Plugin` interface:

```go
type Plugin interface {
    Start() error
    Stop() error
}
```

## Plugin Registration

Plugins register themselves during package initialization using the `plugin.Register()` function:

```go
func init() {
    plugin.Register(plugin.PluginEntry{
        Type:               plugin.PluginTypeBlob, // or PluginTypeMetadata
        Name:               "myplugin",
        Description:        "My custom storage plugin",
        NewFromOptionsFunc: NewFromCmdlineOptions,
        Options: []plugin.PluginOption{
            // Plugin-specific options
        },
    })
}
```

## Plugin Options

Plugins define configuration options using the `PluginOption` struct:

```go
plugin.PluginOption{
    Name:         "data-dir",           // Option name
    Type:         plugin.PluginOptionTypeString, // Data type
    Description:  "Data directory path", // Help text
    DefaultValue: "/tmp/data",         // Default value
    Dest:         &cmdlineOptions.dataDir, // Destination variable
}
```

Supported option types:
- `PluginOptionTypeString`
- `PluginOptionTypeBool`
- `PluginOptionTypeInt`
- `PluginOptionTypeUint`

## Environment Variables

Plugins automatically support environment variables with the pattern:
`DINGO_DATABASE_{TYPE}_{PLUGIN}_{OPTION}`

Examples:
- `DINGO_DATABASE_BLOB_BADGER_DATA_DIR=/data`
- `DINGO_DATABASE_METADATA_SQLITE_DATA_DIR=/metadata.db`

## YAML Configuration

Plugins can be configured in `dingo.yaml`:

```yaml
database:
  blob:
    plugin: "myplugin"
    myplugin:
      option1: "value1"
      option2: 42
  metadata:
    plugin: "sqlite"
    sqlite:
      data-dir: "/data/metadata.db"
```

## Configuration Precedence

1. Command-line flags (highest priority)
2. Environment variables
3. YAML configuration
4. Default values (lowest priority)

## Command Line Options

Plugins support command-line flags with the pattern:
`--{type}-{plugin}-{option}`

Examples:
- `--blob-badger-data-dir /data`
- `--metadata-sqlite-data-dir /metadata.db`

## Plugin Development Steps

### 1. Create Plugin Structure

```text
database/plugin/{type}/{name}/
├── plugin.go      # Registration and options
├── options.go     # Option functions
├── database.go    # Core implementation
└── options_test.go # Unit tests
```

### 2. Implement Core Plugin

Create the main plugin struct that implements `plugin.Plugin`:

```go
type MyPlugin struct {
    // Fields
}

func (p *MyPlugin) Start() error {
    // Initialize resources
    return nil
}

func (p *MyPlugin) Stop() error {
    // Clean up resources
    return nil
}
```

### 3. Define Options

Create option functions following the pattern:

```go
func WithOptionName(value Type) OptionFunc {
    return func(p *MyPlugin) {
        p.field = value
    }
}
```

### 4. Implement Constructors

Provide both options-based and legacy constructors:

```go
func NewWithOptions(opts ...OptionFunc) (*MyPlugin, error) {
    p := &MyPlugin{}
    for _, opt := range opts {
        opt(p)
    }
    return p, nil
}

func New(legacyParam1, legacyParam2) (*MyPlugin, error) {
    // For backward compatibility
    return NewWithOptions(
        WithOption1(legacyParam1),
        WithOption2(legacyParam2),
    )
}
```

### 5. Register Plugin

In `plugin.go`, register during initialization:

```go
var cmdlineOptions struct {
    option1 string
    option2 int
}

func init() {
    plugin.Register(plugin.PluginEntry{
        Type: plugin.PluginTypeBlob,
        Name: "myplugin",
        Description: "My custom plugin",
        NewFromOptionsFunc: NewFromCmdlineOptions,
        Options: []plugin.PluginOption{
            {
                Name: "option1",
                Type: plugin.PluginOptionTypeString,
                Description: "First option",
                DefaultValue: "default",
                Dest: &cmdlineOptions.option1,
            },
            // More options...
        },
    })
}

func NewFromCmdlineOptions() plugin.Plugin {
    p, err := NewWithOptions(
        WithOption1(cmdlineOptions.option1),
        WithOption2(cmdlineOptions.option2),
    )
    if err != nil {
        panic(err)
    }
    return p
}
```

### 6. Add Tests

Create comprehensive tests:

```go
func TestOptions(t *testing.T) {
    // Test option functions
}

func TestLifecycle(t *testing.T) {
    p, err := NewWithOptions(WithOption1("test"))
    // Test Start/Stop
}
```

### 7. Update Imports

Add your plugin to the import list in the appropriate store file:
- `database/plugin/blob/blob.go` for blob plugins
- `database/plugin/metadata/metadata.go` for metadata plugins

## Example: Complete Plugin

See the existing plugins for complete examples:
- `database/plugin/blob/badger/` - BadgerDB implementation
- `database/plugin/metadata/sqlite/` - SQLite implementation
- `database/plugin/blob/gcs/` - Google Cloud Storage implementation
- `database/plugin/blob/aws/` - AWS S3 implementation

## Best Practices

1. **Error Handling**: Always return descriptive errors
2. **Resource Management**: Properly implement Start/Stop for resource lifecycle
3. **Thread Safety**: Ensure plugins are safe for concurrent use
4. **Configuration Validation**: Validate configuration during construction
5. **Backward Compatibility**: Maintain compatibility with existing deployments
6. **Documentation**: Document all options and their effects
7. **Testing**: Provide comprehensive unit and integration tests

## Testing Your Plugin

### Unit Tests
Test individual components and option functions.

### Integration Tests
Test the complete plugin lifecycle and interaction with the plugin system.

### CLI Testing
Use the CLI to test plugin listing and selection:

```bash
./dingo --blob list
./dingo --metadata list
```

### Configuration Testing
Test environment variables and YAML configuration:

```bash
DINGO_DATABASE_BLOB_MYPLUGIN_OPTION1=value ./dingo --blob myplugin
```