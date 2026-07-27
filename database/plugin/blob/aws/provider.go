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

package aws

import (
	"context"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/plugin"
)

type Config struct {
	Endpoint string        `yaml:"endpoint"`
	Bucket   string        `yaml:"bucket"`
	Region   string        `yaml:"region"`
	Prefix   string        `yaml:"prefix"`
	Timeout  time.Duration `yaml:"timeout"`
}

func RegisterProvider(host *plugin.Host) error {
	return plugin.Register(host, plugin.Descriptor{Capability: plugin.CapabilityStorageBlob, Name: "s3", Description: "AWS S3 blob store"},
		func() Config { return Config{} },
		func(_ context.Context, cfg Config, deps blob.ProviderDependencies) (*BlobStoreS3, plugin.Instance, error) {
			store, err := NewWithOptions(WithEndpoint(cfg.Endpoint), WithBucket(cfg.Bucket), WithRegion(cfg.Region), WithPrefix(cfg.Prefix), WithTimeout(cfg.Timeout), WithLogger(deps.Logger), WithPromRegistry(deps.PromRegistry))
			if err != nil {
				return nil, nil, err
			}
			return store, plugin.Lifecycle{
				StartFunc: func(ctx context.Context) error {
					if err := ctx.Err(); err != nil {
						return err
					}
					return store.Start() //nolint:contextcheck
				},
				StopFunc: func(context.Context) error { return store.Stop() }, //nolint:contextcheck
			}, nil
		})
}
