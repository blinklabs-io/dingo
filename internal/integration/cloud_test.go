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
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/blob/aws"
	_ "github.com/blinklabs-io/dingo/database/plugin/blob/aws"
	"github.com/blinklabs-io/dingo/database/plugin/blob/gcs"
	_ "github.com/blinklabs-io/dingo/database/plugin/blob/gcs"
)

func additionalBlobPlugins() []string {
	return []string{"gcs", "s3"}
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
	if os.Getenv("AWS_REGION") != "" {
		return true
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
