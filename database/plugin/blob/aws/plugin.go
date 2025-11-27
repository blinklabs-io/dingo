// Copyright 2025 Blink Labs Software
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

package aws

import (
	"sync"

	"github.com/blinklabs-io/dingo/database/plugin"
)

var (
	// CmdlineOptions holds S3 plugin options populated from command‑line flags.
	// It is exported so tests and external wiring can configure the plugin.
	CmdlineOptions struct {
		Bucket string
		Region string
		Prefix string
	}
	cmdlineOptionsMutex sync.RWMutex
)

// Register plugin
func init() {
	plugin.Register(
		plugin.PluginEntry{
			Type:               plugin.PluginTypeBlob,
			Name:               "s3",
			Description:        "AWS S3 blob store",
			NewFromOptionsFunc: NewFromCmdlineOptions,
			Options: []plugin.PluginOption{
				{
					Name:         "bucket",
					Type:         plugin.PluginOptionTypeString,
					Description:  "S3 bucket name",
					DefaultValue: "",
					Dest:         &(CmdlineOptions.Bucket),
				},
				{
					Name:         "region",
					Type:         plugin.PluginOptionTypeString,
					Description:  "AWS region",
					DefaultValue: "",
					Dest:         &(CmdlineOptions.Region),
				},
				{
					Name:         "prefix",
					Type:         plugin.PluginOptionTypeString,
					Description:  "S3 object key prefix",
					DefaultValue: "",
					Dest:         &(CmdlineOptions.Prefix),
				},
			},
		},
	)
}

func NewFromCmdlineOptions() plugin.Plugin {
	cmdlineOptionsMutex.RLock()
	bucket := CmdlineOptions.Bucket
	region := CmdlineOptions.Region
	prefix := CmdlineOptions.Prefix
	cmdlineOptionsMutex.RUnlock()

	opts := []BlobStoreS3OptionFunc{
		WithBucket(bucket),
		WithRegion(region),
		WithPrefix(prefix),
	}
	p, err := NewWithOptions(opts...)
	if err != nil {
		// Return a plugin that defers the error to Start()
		return plugin.NewErrorPlugin(err)
	}
	return p
}
