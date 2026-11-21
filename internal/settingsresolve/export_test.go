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

package settingsresolve

import "github.com/blinklabs-io/dingo/plugin"

// SetNewHost replaces the plugin host factory the persisted-settings read
// uses and returns a function restoring the original.
func SetNewHost(f func() (*plugin.Host, error)) func() {
	saved := newHost
	newHost = f
	return func() { newHost = saved }
}
