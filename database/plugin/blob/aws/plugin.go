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
	cmdlineOptions struct {
		endpoint string
		bucket   string
		region   string
		prefix   string
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
					Name:         "endpoint",
					Type:         plugin.PluginOptionTypeString,
					Description:  "S3 endpoint",
					DefaultValue: "",
					Dest:         &(cmdlineOptions.endpoint),
				},
				{
					Name:         "bucket",
					Type:         plugin.PluginOptionTypeString,
					Description:  "S3 bucket name",
					DefaultValue: "",
					Dest:         &(cmdlineOptions.bucket),
				},
				{
					Name:         "region",
					Type:         plugin.PluginOptionTypeString,
					Description:  "AWS region",
					DefaultValue: "",
					Dest:         &(cmdlineOptions.region),
				},
				{
					Name:         "prefix",
					Type:         plugin.PluginOptionTypeString,
					Description:  "S3 object key prefix",
					DefaultValue: "",
					Dest:         &(cmdlineOptions.prefix),
				},
			},
		},
	)
}

func NewFromCmdlineOptions() plugin.Plugin {
	cmdlineOptionsMutex.RLock()
	endpoint := cmdlineOptions.endpoint
	bucket := cmdlineOptions.bucket
	region := cmdlineOptions.region
	prefix := cmdlineOptions.prefix
	cmdlineOptionsMutex.RUnlock()

	opts := []BlobStoreS3OptionFunc{
		WithEndpoint(endpoint),
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
