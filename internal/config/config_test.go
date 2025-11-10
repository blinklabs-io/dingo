package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func resetGlobalConfig() {
	globalConfig = &Config{
		BadgerCacheSize: 1073741824,
		MempoolCapacity: 1048576,
		BindAddr:        "0.0.0.0",
		CardanoConfig:   "", // Will be set dynamically based on network
		DatabasePath:    ".dingo",
		SocketPath:      "dingo.socket",
		IntersectTip:    false,
		Network:         "preview",
		MetricsPort:     12798,
		PrivateBindAddr: "127.0.0.1",
		PrivatePort:     3002,
		RelayPort:       3001,
		UtxorpcPort:     9090,
		Topology:        "",
		TlsCertFilePath: "",
		TlsKeyFilePath:  "",
		DevMode:         false,
	}
}

func TestLoad_CompareFullStruct(t *testing.T) {
	resetGlobalConfig()
	yamlContent := `
badgerCacheSize: 8388608
mempoolCapacity: 2097152
bindAddr: "127.0.0.1"
cardanoConfig: "./cardano/preview/config.json"
databasePath: ".dingo"
socketPath: "env.socket"
intersectTip: true
network: "preview"
metricsPort: 8088
privateBindAddr: "127.0.0.1"
privatePort: 8000
relayPort: 4000
utxorpcPort: 9940
topology: ""
tlsCertFilePath: "cert1.pem"
tlsKeyFilePath: "key1.pem"
`

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test-dingo.yaml")

	err := os.WriteFile(tmpFile, []byte(yamlContent), 0644)
	if err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	defer os.Remove(tmpFile)

	expected := &Config{
		BadgerCacheSize: 8388608,
		MempoolCapacity: 2097152,
		BindAddr:        "127.0.0.1",
		CardanoConfig:   "./cardano/preview/config.json",
		DatabasePath:    ".dingo",
		SocketPath:      "env.socket",
		IntersectTip:    true,
		Network:         "preview",
		MetricsPort:     8088,
		PrivateBindAddr: "127.0.0.1",
		PrivatePort:     8000,
		RelayPort:       4000,
		UtxorpcPort:     9940,
		Topology:        "",
		TlsCertFilePath: "cert1.pem",
		TlsKeyFilePath:  "key1.pem",
		DevMode:         false,
	}

	actual, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf(
			"Loaded config does not match expected.\nActual: %+v\nExpected: %+v",
			actual,
			expected,
		)
	}
}
func TestLoad_WithoutConfigFile_UsesDefaults(t *testing.T) {
	resetGlobalConfig()

	// Without Config file
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Expected is the updated default values from globalConfig
	expected := &Config{
		BadgerCacheSize: 1073741824,
		MempoolCapacity: 1048576,
		BindAddr:        "0.0.0.0",
		CardanoConfig:   "preview/config.json", // Set dynamically based on network
		DatabasePath:    ".dingo",
		SocketPath:      "dingo.socket",
		IntersectTip:    false,
		Network:         "preview",
		MetricsPort:     12798,
		PrivateBindAddr: "127.0.0.1",
		PrivatePort:     3002,
		RelayPort:       3001,
		UtxorpcPort:     9090,
		Topology:        "",
		TlsCertFilePath: "",
		TlsKeyFilePath:  "",
		DevMode:         false,
	}

	if !reflect.DeepEqual(cfg, expected) {
		t.Errorf(
			"config mismatch without file:\nExpected: %+v\nGot:      %+v",
			expected,
			cfg,
		)
	}
}

func TestLoad_WithDevModeConfig(t *testing.T) {
	resetGlobalConfig()

	// Test with dev mode in config file
	yamlContent := `
devMode: true
network: "preview"
`

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test-dev-mode.yaml")

	err := os.WriteFile(tmpFile, []byte(yamlContent), 0644)
	if err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if !cfg.DevMode {
		t.Errorf("expected DevMode to be true, got: %v", cfg.DevMode)
	}
}

func TestLoadConfig_EmbeddedDefaults(t *testing.T) {
	resetGlobalConfig()

	// Test loading config without any file (should use defaults including embedded path)
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("expected no error loading default config, got: %v", err)
	}

	// Should use embedded path for CardanoConfig
	expected := "preview/config.json"
	if cfg.CardanoConfig != expected {
		t.Errorf(
			"expected CardanoConfig to be %q, got %q",
			expected,
			cfg.CardanoConfig,
		)
	}

	// Should have other default values
	if cfg.Network != "preview" {
		t.Errorf("expected Network to be 'preview', got %q", cfg.Network)
	}

	if cfg.RelayPort != 3001 {
		t.Errorf("expected RelayPort to be 3001, got %d", cfg.RelayPort)
	}
}

func TestLoadConfig_MainnetNetwork(t *testing.T) {
	resetGlobalConfig()
	globalConfig.Network = "mainnet"

	// Test loading config with non-preview network uses /opt/cardano path
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("expected no error for non-preview network, got: %v", err)
	}

	// Should use /opt/cardano path for non-preview networks
	expected := "/opt/cardano/mainnet/config.json"
	if cfg.CardanoConfig != expected {
		t.Errorf(
			"expected CardanoConfig to be %q, got %q",
			expected,
			cfg.CardanoConfig,
		)
	}

	if cfg.Network != "mainnet" {
		t.Errorf("expected Network to be 'mainnet', got %q", cfg.Network)
	}
}

func TestLoadConfig_DevnetNetwork(t *testing.T) {
	resetGlobalConfig()
	globalConfig.Network = "devnet"
	globalConfig.DevMode = true // Set devmode to avoid topology issues

	// Test loading config with devnet network uses /opt/cardano path
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("expected no error for devnet network, got: %v", err)
	}

	// Should use /opt/cardano path for devnet network
	expected := "/opt/cardano/devnet/config.json"
	if cfg.CardanoConfig != expected {
		t.Errorf(
			"expected CardanoConfig to be %q, got %q",
			expected,
			cfg.CardanoConfig,
		)
	}

	if cfg.Network != "devnet" {
		t.Errorf("expected Network to be 'devnet', got %q", cfg.Network)
	}
}

func TestLoadConfig_UnsupportedNetworkWithUserConfig(t *testing.T) {
	resetGlobalConfig()
	globalConfig.Network = "unsupported"
	globalConfig.CardanoConfig = "/custom/path/config.json"
	globalConfig.DevMode = true // Set devmode to avoid topology issues

	// Test that unsupported network works if user provides CardanoConfig
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf(
			"expected no error when user provides CardanoConfig, got: %v",
			err,
		)
	}

	if cfg.CardanoConfig != "/custom/path/config.json" {
		t.Errorf(
			"expected CardanoConfig to be user-provided path, got %q",
			cfg.CardanoConfig,
		)
	}
}
