package config

import (
	"os"
	"reflect"
	"testing"
)

func resetGlobalConfig() {
	globalConfig = &Config{
		BindAddr:        "0.0.0.0",
		CardanoConfig:   "./config/cardano/preview/config.json",
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
	}
}

func TestLoad_CompareFullStruct(t *testing.T) {
	resetGlobalConfig()
	yamlContent := `
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

	tmpFile := "test-dingo.yaml"
	err := os.WriteFile(tmpFile, []byte(yamlContent), 0644)
	if err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	defer os.Remove(tmpFile)

	expected := &Config{
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
	}

	actual, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Loaded config does not match expected.\nActual: %+v\nExpected: %+v", actual, expected)
	}
}
func TestLoad_WithoutConfigFile_UsesDefaults(t *testing.T) {
	resetGlobalConfig()

	// Without Config file
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Expected is the original default values from globalConfig
	expected := &Config{
		BindAddr:        "0.0.0.0",
		CardanoConfig:   "./config/cardano/preview/config.json",
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
	}

	if !reflect.DeepEqual(cfg, expected) {
		t.Errorf("config mismatch without file:\nExpected: %+v\nGot:      %+v", expected, cfg)
	}
}
