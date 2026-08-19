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

package sqlstore

import "github.com/blinklabs-io/dingo/database/plugin/metadata"

// Store must satisfy the composed metadata interface and every domain
// interface split out of it. The sqlite, mysql, and postgres providers are
// dialect shims that each construct and return a *Store of their own --
// sqlite through NewSQLStore, mysql and postgres through their package-local
// openStore -- so asserting on *Store here covers all three backends at once
// rather than one.
//
// Listing the domains individually rather than relying on the MetadataStore
// assertion alone is what makes a botched extraction a build failure: a
// domain interface that picks up a method with a signature the store does
// not actually implement fails here, and it fails naming the domain.
//
// What these cannot show is that a narrowed interface works against a live
// database. That is storagetest.RunMetadataStoreConformance, which every
// provider package runs from its own conformance_test.go.
var (
	_ metadata.MetadataStore = (*Store)(nil)

	_ metadata.CertificateStore   = (*Store)(nil)
	_ metadata.EpochStore         = (*Store)(nil)
	_ metadata.GovernanceStore    = (*Store)(nil)
	_ metadata.LifecycleStore     = (*Store)(nil)
	_ metadata.SettingsStore      = (*Store)(nil)
	_ metadata.SlotRangeStore     = (*Store)(nil)
	_ metadata.StakeSnapshotStore = (*Store)(nil)
	_ metadata.TransactionStore   = (*Store)(nil)
	_ metadata.TxnStore           = (*Store)(nil)
	_ metadata.UtxoStore          = (*Store)(nil)
)
