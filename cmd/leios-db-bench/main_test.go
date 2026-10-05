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

package main

import (
	"strings"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

func TestTxPayloadSizeHandlesCBORHeaderBoundaries(t *testing.T) {
	tests := []struct {
		name           string
		serializedSize int
		wantPayload    int
		wantErr        bool
	}{
		{name: "byte-string short form", serializedSize: 258, wantPayload: 255},
		{name: "first unrepresentable size", serializedSize: 259, wantErr: true},
		{name: "byte-string uint16 form", serializedSize: 260, wantPayload: 256},
		{name: "last uint16 form", serializedSize: 65539, wantPayload: 65535},
		{name: "first uint32 gap", serializedSize: 65540, wantErr: true},
		{name: "second uint32 gap", serializedSize: 65541, wantErr: true},
		{name: "byte-string uint32 form", serializedSize: 65542, wantPayload: 65536},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := txPayloadSize(tt.serializedSize)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("txPayloadSize(%d) succeeded with payload %d", tt.serializedSize, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("txPayloadSize(%d): %v", tt.serializedSize, err)
			}
			if got != tt.wantPayload {
				t.Fatalf("txPayloadSize(%d) = %d, want %d", tt.serializedSize, got, tt.wantPayload)
			}
		})
	}
}

func TestValidateConfigRejectsUnrepresentableTransactionSize(t *testing.T) {
	config := benchConfig{
		prePopulatedEbs: 1,
		txsPerEb:        1,
		txSizeBytes:     259,
		fetchClients:    1,
		ebsPerClient:    1,
		fetchServers:    1,
		chainSelReads:   1,
		gcTicks:         0,
		runs:            1,
	}
	if err := validateConfig(config); err == nil || !strings.Contains(err.Error(), "cannot be encoded") {
		t.Fatalf("validateConfig(%d-byte transaction) error = %v, want CBOR size error", config.txSizeBytes, err)
	}
}

func TestGenManifestReferencesSerializedTransactions(t *testing.T) {
	config := benchConfig{txsPerEb: 3, txSizeBytes: 16_384}
	txs, err := genTxs(42, config)
	if err != nil {
		t.Fatalf("genTxs: %v", err)
	}
	manifest, err := genManifest(txs)
	if err != nil {
		t.Fatalf("genManifest: %v", err)
	}
	var block lcommon.LeiosEndorserBlock
	if bytesRead, err := cbor.Decode(manifest, &block); err != nil {
		t.Fatalf("decode manifest: %v", err)
	} else if bytesRead != len(manifest) {
		t.Fatalf("manifest consumed %d bytes, want %d", bytesRead, len(manifest))
	}
	if err := block.Validate(); err != nil {
		t.Fatalf("validate manifest: %v", err)
	}
	if len(block.TransactionReferences) != len(txs) {
		t.Fatalf("manifest has %d references, want %d", len(block.TransactionReferences), len(txs))
	}
	for i, tx := range txs {
		ref := block.TransactionReferences[i]
		if got, want := int(ref.TransactionSize), len(tx); got != want {
			t.Errorf("transaction %d reference size = %d, want %d", i, got, want)
		}
		if got, want := ref.TransactionHash, lcommon.Blake2b256Hash(tx); got != want {
			t.Errorf("transaction %d reference hash does not match serialized transaction", i)
		}
		var items []cbor.RawMessage
		if bytesRead, err := cbor.Decode(tx, &items); err != nil {
			t.Errorf("transaction %d is not a CBOR envelope: %v", i, err)
		} else if bytesRead != len(tx) || len(items) != 1 {
			t.Errorf("transaction %d decoded %d bytes and %d items; want %d bytes and one item", i, bytesRead, len(items), len(tx))
		}
	}
}
