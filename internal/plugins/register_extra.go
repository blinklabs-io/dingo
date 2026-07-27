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

package plugins

import (
	"fmt"

	"github.com/blinklabs-io/dingo/database/plugin/blob/aws"
	"github.com/blinklabs-io/dingo/database/plugin/blob/gcs"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/mysql"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/postgres"
	"github.com/blinklabs-io/dingo/plugin"
)

func registerExtra(host *plugin.Host) error {
	registrations := []struct {
		name     string
		register func(*plugin.Host) error
	}{
		{"s3", aws.RegisterProvider}, {"gcs", gcs.RegisterProvider}, {"mysql", mysql.RegisterProvider}, {"postgres", postgres.RegisterProvider},
	}
	for _, item := range registrations {
		if err := item.register(host); err != nil {
			return fmt.Errorf("register %s provider: %w", item.name, err)
		}
	}
	return nil
}
