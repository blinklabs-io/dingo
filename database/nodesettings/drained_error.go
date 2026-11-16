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

package nodesettings

import "errors"

// ErrFamilyDrained is wrapped by the error of a read or write that needs the
// SQL rows of a ledger-state family the metadata store no longer holds: the
// live UTxO set is the only copy, and a row written to the store would
// reappear beside a set that no longer matches it.
var ErrFamilyDrained = errors.New(
	"ledger-state family is drained from the metadata store",
)
