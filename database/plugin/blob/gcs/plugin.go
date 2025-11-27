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

package gcs

import (
	"fmt"
	"os"
	"sync"

	"github.com/blinklabs-io/dingo/database/plugin"
)

// ValidateCredentials checks if the credentials file exists and is readable
func ValidateCredentials(credentialsFile string) error {
	if credentialsFile == "" {
		return nil // Empty credentials file is allowed (will use ADC)
	}
	info, err := os.Stat(credentialsFile)
	if os.IsNotExist(err) {
		return fmt.Errorf(
			"GCS credentials file does not exist: %s",
			credentialsFile,
		)
	}
	if err != nil || info.Mode().Perm()&0o400 == 0 {
		return fmt.Errorf(
			"GCS credentials file is not readable: %s",
			credentialsFile,
		)
	}
	return nil
}

var (
	cmdlineOptions struct {
		Bucket          string
		CredentialsFile string
	}
	cmdlineOptionsMutex sync.RWMutex
)

// initCmdlineOptions sets default values for cmdlineOptions
func initCmdlineOptions() {
	cmdlineOptionsMutex.Lock()
	defer cmdlineOptionsMutex.Unlock()
	// No defaults needed for GCS - bucket and credentials are required
}

// Register plugin
func init() {
	initCmdlineOptions()
	plugin.Register(
		plugin.PluginEntry{
			Type:               plugin.PluginTypeBlob,
			Name:               "gcs",
			Description:        "Google Cloud Storage blob store",
			NewFromOptionsFunc: NewFromCmdlineOptions,
			Options: []plugin.PluginOption{
				{
					Name:         "bucket",
					Type:         plugin.PluginOptionTypeString,
					Description:  "GCS bucket name",
					DefaultValue: "",
					Dest:         &(cmdlineOptions.Bucket),
				},
				{
					Name:         "credentials-file",
					Type:         plugin.PluginOptionTypeString,
					Description:  "Path to GCS service account credentials file",
					DefaultValue: "",
					Dest:         &(cmdlineOptions.CredentialsFile),
				},
			},
		},
	)
}

func NewFromCmdlineOptions() plugin.Plugin {
	cmdlineOptionsMutex.RLock()
	bucket := cmdlineOptions.Bucket
	credentialsFile := cmdlineOptions.CredentialsFile
	cmdlineOptionsMutex.RUnlock()

	opts := []BlobStoreGCSOptionFunc{
		WithBucket(bucket),
		WithCredentialsFile(credentialsFile),
	}
	p, err := NewWithOptions(opts...)
	if err != nil {
		// Return a plugin that defers the error to Start()
		return plugin.NewErrorPlugin(err)
	}
	return p
}
