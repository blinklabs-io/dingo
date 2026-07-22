// Copyright 2026 Blink Labs Software
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

//go:build dingo_extra_plugins

package gcs

import (
	"context"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/plugin"
)

type Config struct {
	Bucket string `yaml:"bucket"`
}

func RegisterProvider(host *plugin.Host) error {
	return plugin.Register(host, plugin.Descriptor{Capability: plugin.CapabilityStorageBlob, Name: "gcs", Description: "Google Cloud Storage blob store"}, func() Config { return Config{} },
		func(_ context.Context, cfg Config, deps blob.ProviderDependencies) (*BlobStoreGCS, plugin.Instance, error) {
			store, err := NewWithOptions(WithBucket(cfg.Bucket), WithLogger(deps.Logger), WithPromRegistry(deps.PromRegistry))
			if err != nil {
				return nil, nil, err
			}
			return store, plugin.Lifecycle{StartFunc: func(context.Context) error { return store.Start() }, StopFunc: func(context.Context) error { return store.Stop() }}, nil //nolint:contextcheck
		})
}
