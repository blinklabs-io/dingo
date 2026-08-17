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

package badger

// Default cache and value sizes for BadgerDB, in bytes.
const (
	DefaultBlockCacheSize         = 268435456
	DefaultIndexCacheSize         = 0
	DefaultValueLogFileSize       = 1073741824
	DefaultMemTableSize           = 134217728
	DefaultValueThreshold         = 1048576
	DefaultCoreBlockCacheSize     = 0
	DefaultCoreIndexCacheSize     = 0
	DefaultCoreCompressionEnabled = false
	DefaultAPIBlockCacheSize      = DefaultBlockCacheSize
	DefaultAPIIndexCacheSize      = DefaultIndexCacheSize
	DefaultAPICompressionEnabled  = true
	DefaultCompressionLevel       = 1
)

func useCompactBlockMetadata(runMode, storageMode string) bool {
	return (runMode == "serve" || runMode == "leios") && storageMode == "core"
}
