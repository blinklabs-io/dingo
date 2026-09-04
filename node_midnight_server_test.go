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

package dingo

import "testing"

func TestMidnightServerActiveRequiresExplicitEnablement(t *testing.T) {
	tests := []struct {
		name        string
		storageMode StorageMode
		config      MidnightConfig
		want        bool
	}{
		{
			name:        "disabled despite configured default port",
			storageMode: StorageModeAPI,
			config:      MidnightConfig{Port: 50051},
		},
		{
			name:        "indexer enabled without server",
			storageMode: StorageModeAPI,
			config: MidnightConfig{
				Enabled: true,
				Port:    50051,
			},
		},
		{
			name:        "enabled in api mode",
			storageMode: StorageModeAPI,
			config: MidnightConfig{
				ServerEnabled: true,
				Port:          50051,
			},
			want: true,
		},
		{
			name:        "enabled in core mode",
			storageMode: StorageModeCore,
			config: MidnightConfig{
				ServerEnabled: true,
				Port:          50051,
			},
		},
		{
			name:        "enabled with zero port",
			storageMode: StorageModeAPI,
			config:      MidnightConfig{ServerEnabled: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := midnightServerActive(tt.storageMode, tt.config); got != tt.want {
				t.Fatalf("midnightServerActive() = %v, want %v", got, tt.want)
			}
		})
	}
}
