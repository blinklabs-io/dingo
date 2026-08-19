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

// domainAccessors pairs each narrowing accessor with the interface it is
// supposed to hand out. Method expressions rather than method values: these
// need no receiver, so they cannot be read as a nil dereference.
var domainAccessors = []struct {
	name     string
	accessor any
	want     reflect.Type
}{
	{
		"certificateStore",
		(*Database).certificateStore,
		reflect.TypeFor[metadata.CertificateStore](),
	},
	{
		"epochStore",
		(*Database).epochStore,
		reflect.TypeFor[metadata.EpochStore](),
	},
	{
		"governanceStore",
		(*Database).governanceStore,
		reflect.TypeFor[metadata.GovernanceStore](),
	},
	{
		"stakeSnapshotStore",
		(*Database).stakeSnapshotStore,
		reflect.TypeFor[metadata.StakeSnapshotStore](),
	},
	{
		"transactionStore",
		(*Database).transactionStore,
		reflect.TypeFor[metadata.TransactionStore](),
	},
	{
		"utxoStore",
		(*Database).utxoStore,
		reflect.TypeFor[metadata.UtxoStore](),
	},
}

// TestFacadesDependOnNarrowStores is the "used by callers" half of the
// domain split. Facade methods reach their backend through these accessors
// rather than through d.metadata, so the compiler -- not review -- is what
// stops a method in one domain from reaching into another.
//
// Asserting on each accessor's declared return type rather than on the
// value it returns is deliberate: widening one back to
// metadata.MetadataStore would keep every call site compiling and silently
// undo the narrowing, and a returned *sqlstore.Store satisfies the narrow
// interface either way.
func TestFacadesDependOnNarrowStores(t *testing.T) {
	for _, a := range domainAccessors {
		t.Run(a.name, func(t *testing.T) {
			typ := reflect.TypeOf(a.accessor)
			require.Equal(t, 1, typ.NumOut())
			require.Equalf(
				t,
				a.want,
				typ.Out(0),
				"%s() must return its narrow domain interface; returning "+
					"MetadataStore re-widens every call site it serves",
				a.name,
			)
		})
	}
}

// TestDomainAccessorsReturnBackingStore checks the accessors hand back the
// configured metadata store rather than a nil placeholder that would satisfy
// the type assertions above while breaking at runtime.
func TestDomainAccessorsReturnBackingStore(t *testing.T) {
	d := &Database{metadata: stubDomainMetadataStore{}}

	require.NotNil(t, d.certificateStore())
	require.NotNil(t, d.epochStore())
	require.NotNil(t, d.governanceStore())
	require.NotNil(t, d.stakeSnapshotStore())
	require.NotNil(t, d.transactionStore())
	require.NotNil(t, d.utxoStore())
}

// stubDomainMetadataStore is a nil-method placeholder: the tests only need a
// non-nil value of the composed interface type, never a call.
type stubDomainMetadataStore struct {
	metadata.MetadataStore
}
