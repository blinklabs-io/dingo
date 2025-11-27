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

package gcs_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/blob/gcs"
)

func TestCredentialValidation(t *testing.T) {
	// Create temp directory for test files
	tempDir := t.TempDir()

	tests := []struct {
		name            string
		CredentialsFile string
		expectError     bool
		errorMessage    string
	}{
		{
			name: "valid credentials file",
			CredentialsFile: func() string {
				// Create a temporary file that exists
				tempFile, err := os.CreateTemp(tempDir, "credentials-*.json")
				if err != nil {
					t.Fatalf("Failed to create temp file: %v", err)
				}
				tempFile.Close()
				return tempFile.Name()
			}(),
			expectError: false,
		},
		{
			name: "nonexistent credentials file",
			CredentialsFile: filepath.Join(
				tempDir,
				"nonexistent-credentials.json",
			),
			expectError:  true,
			errorMessage: "GCS credentials file does not exist",
		},
		{
			name:            "empty credentials file path",
			CredentialsFile: "",
			expectError:     false, // Should not error when empty
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := gcs.ValidateCredentials(tt.CredentialsFile)
			if tt.expectError {
				if err == nil {
					t.Errorf(
						"Expected error containing %q, but got no error",
						tt.errorMessage,
					)
				} else if !strings.Contains(err.Error(), tt.errorMessage) {
					t.Errorf("Expected error message containing %q, got %q", tt.errorMessage, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, but got %q", err.Error())
				}
			}
		})
	}
}
