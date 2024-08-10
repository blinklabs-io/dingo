// Copyright 2024 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package node

import (
	"fmt"
	"io"
	"log/slog"

	ouroboros "github.com/blinklabs-io/gouroboros"
)

type Config struct {
	dataDir            string
	logger             *slog.Logger
	listeners          []ListenerConfig
	network            string
	networkMagic       uint32
	peerSharing        bool
	outboundSourcePort int
	topologyConfig     *ouroboros.TopologyConfig
	tracing            bool
	tracingStdout      bool
}

// configPopulateNetworkMagic uses the named network (if specified) to determine the network magic value (if not specified)
func (n *Node) configPopulateNetworkMagic() error {
	if n.config.networkMagic == 0 && n.config.network != "" {
		tmpCfg := n.config
		tmpNetwork := ouroboros.NetworkByName(n.config.network)
		if tmpNetwork == ouroboros.NetworkInvalid {
			return fmt.Errorf("unknown network name: %s", n.config.network)
		}
		tmpCfg.networkMagic = tmpNetwork.NetworkMagic
		n.config = tmpCfg
	}
	return nil
}

func (n *Node) configValidate() error {
	if n.config.networkMagic == 0 {
		return fmt.Errorf(
			"invalid network magic value: %d",
			n.config.networkMagic,
		)
	}
	if len(n.config.listeners) == 0 {
		return fmt.Errorf("no listeners defined")
	}
	for _, listener := range n.config.listeners {
		if listener.Listener != nil {
			continue
		}
		if listener.ListenNetwork != "" && listener.ListenAddress != "" {
			continue
		}
		return fmt.Errorf(
			"listener must provide net.Listener or listen network/address values",
		)
	}
	return nil
}

// ConfigOptionFunc is a type that represents functions that modify the Connection config
type ConfigOptionFunc func(*Config)

// NewConfig creates a new node config with the specified options
func NewConfig(opts ...ConfigOptionFunc) Config {
	c := Config{
		// Default logger will throw away logs
		// We do this so we don't have to add guards around every log operation
		logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		// TODO: add defaults
	}
	// Apply options
	for _, opt := range opts {
		opt(&c)
	}
	return c
}

// WithDataDir specifies the persistent data directory to use. The default is to store everything in memory
func WithDataDir(dataDir string) ConfigOptionFunc {
	return func(c *Config) {
		c.dataDir = dataDir
	}
}

// WithLogger specifies the logger to use. This defaults to discarding log output
func WithLogger(logger *slog.Logger) ConfigOptionFunc {
	return func(c *Config) {
		c.logger = logger
	}
}

// WithListeners specifies the listener config(s) to use
func WithListeners(listeners ...ListenerConfig) ConfigOptionFunc {
	return func(c *Config) {
		c.listeners = append(c.listeners, listeners...)
	}
}

// WithNetwork specifies the named network to operate on. This will automatically set the appropriate network magic value
func WithNetwork(network string) ConfigOptionFunc {
	return func(c *Config) {
		c.network = network
	}
}

// WithNetworkMagic specifies the network magic value to use. This will override any named network specified
func WithNetworkMagic(networkMagic uint32) ConfigOptionFunc {
	return func(c *Config) {
		c.networkMagic = networkMagic
	}
}

// WithPeerSharing specifies whether to enable peer sharing. This is disabled by default
func WithPeerSharing(peerSharing bool) ConfigOptionFunc {
	return func(c *Config) {
		c.peerSharing = peerSharing
	}
}

// WithOutboundSourcePort specifies the source port to use for outbound connections. This defaults to dynamic source ports
func WithOutboundSourcePort(port int) ConfigOptionFunc {
	return func(c *Config) {
		c.outboundSourcePort = port
	}
}

// WithTopologyConfig specifies an ouroboros.TopologyConfig to use for outbound peers
func WithTopologyConfig(
	topologyConfig *ouroboros.TopologyConfig,
) ConfigOptionFunc {
	return func(c *Config) {
		c.topologyConfig = topologyConfig
	}
}

// WithTracing enables tracing. By default, spans are submitted to a HTTP(s) endpoint using OTLP. This can be configured
// using the OTEL_EXPORTER_OTLP_* env vars documented in the README for [go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp]
func WithTracing(tracing bool) ConfigOptionFunc {
	return func(c *Config) {
		c.tracing = tracing
	}
}

// WithTracingStdout enables tracing output to stdout. This also requires tracing to enabled separately. This is mostly useful for debugging
func WithTracingStdout(stdout bool) ConfigOptionFunc {
	return func(c *Config) {
		c.tracingStdout = stdout
	}
}
