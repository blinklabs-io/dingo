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

package migrations_test

// testDBPragmas relaxes durability for throwaway per-test SQLite databases:
// each one is created, migrated, asserted against, and deleted inside a
// single test, so an fsync'd rollback journal buys nothing and is expensive
// on a contended CI runner (dingo#4171). No test in this package kills a
// connection mid-transaction, simulates crash recovery, or inspects a
// journal/WAL file, so relaxing durability does not change what any
// assertion observes. This is a twin of the identical constant in package
// migrations (runner_test.go); Go test files in the internal and external
// test packages for one directory compile separately and cannot share it.
const testDBPragmas = "_pragma=journal_mode(MEMORY)&_pragma=synchronous(OFF)"
