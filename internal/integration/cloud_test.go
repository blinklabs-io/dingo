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

//go:build dingo_extra_plugins

package integration

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/plugin/blob/aws"
	_ "github.com/blinklabs-io/dingo/database/plugin/blob/aws"
	"github.com/blinklabs-io/dingo/database/plugin/blob/gcs"
	_ "github.com/blinklabs-io/dingo/database/plugin/blob/gcs"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
)

// cloudStorageBenchmarkBackends returns GCS and S3 storage backends for the
// storage benchmarks when their credentials are configured. Cloud blob stores
// read the bucket (and S3 prefix) from provider config, so DataDir is consumed
// only by the sqlite metadata store and must be a local filesystem path.
func cloudStorageBenchmarkBackends(
	diskDataDir, benchName string,
) []storageBenchBackend {
	var backends []storageBenchBackend
	if hasGCSCredentials() {
		bucket := os.Getenv("DINGO_TEST_GCS_BUCKET")
		if bucket == "" {
			bucket = "dingo-test-bucket"
		}
		backends = append(backends, storageBenchBackend{
			name: "GCS",
			opts: dbtest.Options{
				Config: &database.Config{
					DataDir: filepath.Join(diskDataDir, "gcs-metadata"),
				},
				Blob: dbtest.StorageProvider{
					Name:     "gcs",
					Config:   map[string]any{"bucket": bucket},
					Register: gcs.RegisterProvider,
				},
			},
		})
	}
	if hasS3Credentials() {
		bucket := os.Getenv("DINGO_TEST_S3_BUCKET")
		if bucket == "" {
			bucket = "dingo-test-bucket"
		}
		region := os.Getenv("AWS_REGION")
		if region == "" {
			region = "us-east-1"
		}
		s3Config := map[string]any{
			"bucket": bucket,
			// A path prefix isolates concurrent benchmark runs within one
			// bucket instead of requiring a unique bucket per run.
			"prefix": strings.ReplaceAll(benchName, "/", "-") + "/",
			"region": region,
		}
		if endpoint := os.Getenv("AWS_ENDPOINT"); endpoint != "" {
			s3Config["endpoint"] = endpoint
		}
		backends = append(backends, storageBenchBackend{
			name: "S3",
			opts: dbtest.Options{
				Config: &database.Config{
					DataDir: filepath.Join(diskDataDir, "s3-metadata"),
				},
				Blob: dbtest.StorageProvider{
					Name:     "s3",
					Config:   s3Config,
					Register: aws.RegisterProvider,
				},
			},
		})
	}
	return backends
}

func hasGCSCredentials() bool {
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") != "" {
		return true
	}
	home := os.Getenv("HOME")
	if home != "" {
		adcPath := filepath.Join(
			home,
			".config",
			"gcloud",
			"application_default_credentials.json",
		)
		if _, err := os.Stat(adcPath); err == nil {
			return true
		}
	}
	return false
}

func hasS3Credentials() bool {
	if os.Getenv("AWS_ACCESS_KEY_ID") != "" && os.Getenv("AWS_SECRET_ACCESS_KEY") != "" {
		return true
	}
	home := os.Getenv("HOME")
	if home != "" {
		if _, err := os.Stat(filepath.Join(home, ".aws", "credentials")); err == nil {
			return true
		}
	}
	return false
}

func TestCloudPluginGCS(t *testing.T) {
	if !hasGCSCredentials() {
		t.Skip("GCS credentials not found, skipping test")
	}
	testBucket := os.Getenv("DINGO_TEST_GCS_BUCKET")
	if testBucket == "" {
		testBucket = "dingo-test-bucket"
	}
	gcsPlugin, err := gcs.NewWithOptions(
		gcs.WithBucket(testBucket),
	)
	if err != nil {
		t.Fatalf("failed to create GCS plugin: %v", err)
	}
	if err := gcsPlugin.Start(); err != nil {
		t.Fatalf("failed to start GCS plugin: %v", err)
	}
	defer func() {
		if err := gcsPlugin.Stop(); err != nil {
			t.Errorf("failed to stop GCS plugin: %v", err)
		}
	}()
}

func TestCloudPluginS3(t *testing.T) {
	if !hasS3Credentials() {
		t.Skip("S3 credentials not found, skipping test")
	}
	testBucket := os.Getenv("DINGO_TEST_S3_BUCKET")
	if testBucket == "" {
		testBucket = "dingo-test-bucket"
	}
	opts := []aws.BlobStoreS3OptionFunc{
		aws.WithBucket(testBucket),
	}
	if os.Getenv("AWS_ENDPOINT") != "" {
		opts = append(opts, aws.WithEndpoint(os.Getenv("AWS_ENDPOINT")))
	}
	s3Plugin, err := aws.NewWithOptions(opts...)
	if err != nil {
		t.Fatalf("failed to create S3 plugin: %v", err)
	}
	if err := s3Plugin.Start(); err != nil {
		t.Fatalf("failed to start S3 plugin: %v", err)
	}
	defer func() {
		if err := s3Plugin.Stop(); err != nil {
			t.Errorf("failed to stop S3 plugin: %v", err)
		}
	}()
}
