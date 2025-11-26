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
	"testing"
)

func TestNewFromCmdlineOptions(t *testing.T) {
	// Save original cmdlineOptions
	originalOptions := cmdlineOptions
	cmdlineOptions.bucket = "test-bucket"
	cmdlineOptions.region = "us-east-1"
	cmdlineOptions.prefix = "test-prefix"

	// This should succeed
	plugin := NewFromCmdlineOptions()
	if plugin == nil {
		t.Error("Expected plugin to be created, got nil")
	}

	// Restore original options
	cmdlineOptions = originalOptions
}
