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

package database

import (
	"reflect"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/stretchr/testify/require"
)

// TestGovernanceFacadeDependsOnNarrowStore is the "used by callers" half of
// the domain split. The governance, committee, DRep, and constitution facade
// files reach their backend through governanceStore() rather than d.metadata,
// so the compiler -- not review -- is what stops a governance method here
// from reaching for an unrelated storage domain.
//
// Asserting on the declared return type rather than on the returned value is
// deliberate: widening the accessor back to metadata.MetadataStore would keep
// every call site compiling and silently undo the narrowing.
func TestGovernanceFacadeDependsOnNarrowStore(t *testing.T) {
	// A method expression rather than a method value on a nil *Database:
	// this needs no receiver at all, so it cannot be read as a nil
	// dereference.
	accessor := reflect.TypeOf((*Database).governanceStore)

	require.Equal(t, 1, accessor.NumOut())
	require.Equal(
		t,
		reflect.TypeFor[metadata.GovernanceStore](),
		accessor.Out(0),
		"governanceStore() must return the narrow governance interface; "+
			"returning MetadataStore re-widens every governance call site",
	)
}

// TestGovernanceStoreAccessorReturnsBackingStore checks the accessor actually
// hands back the configured metadata store rather than a nil placeholder that
// would satisfy the type assertion above while breaking at runtime.
func TestGovernanceStoreAccessorReturnsBackingStore(t *testing.T) {
	d := &Database{metadata: stubGovernanceMetadataStore{}}
	require.NotNil(t, d.governanceStore())
}

// stubGovernanceMetadataStore is a nil-method placeholder: the test only
// needs a non-nil value of the composed interface type, never a call.
type stubGovernanceMetadataStore struct {
	metadata.MetadataStore
}
