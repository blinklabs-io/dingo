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

package aws_test

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/blob/aws"
)

func TestNewFromCmdlineOptions(t *testing.T) {
	// Save original aws.CmdlineOptions
	originalOptions := aws.CmdlineOptions
	aws.CmdlineOptions.Bucket = "test-bucket"
	aws.CmdlineOptions.Region = "us-east-1"
	aws.CmdlineOptions.Prefix = "test-prefix"

	// This should succeed
	plugin := aws.NewFromCmdlineOptions()
	if plugin == nil {
		t.Error("Expected plugin to be created, got nil")
	}

	// Restore original options
	aws.CmdlineOptions = originalOptions
}
