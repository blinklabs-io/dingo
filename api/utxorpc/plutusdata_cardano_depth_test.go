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

package utxorpc

import (
	"math/big"
	"testing"

	pdata "github.com/blinklabs-io/plutigo/data"
	cardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func TestPlutusDatumCBORToCardano_DeepNestingRoundTrip(t *testing.T) {
	t.Parallel()
	const depth = 500
	var datum pdata.PlutusData = &pdata.Integer{Inner: big.NewInt(0)}
	for range depth {
		datum = &pdata.List{Items: []pdata.PlutusData{datum}}
	}

	raw, err := pdata.Encode(datum)
	if err != nil {
		t.Fatal(err)
	}
	mapped, err := plutusDatumCBORToCardano(raw)
	if err != nil {
		t.Fatalf("CBOR consumer rejected depth %d (%d bytes): %v", depth, len(raw), err)
	}
	if got := cardanoListDepth(mapped); got != depth {
		t.Fatalf("CBOR consumer projected depth %d as %d", depth, got)
	}
	wire, err := proto.Marshal(mapped)
	if err != nil {
		t.Fatal(err)
	}
	var binaryRoundTrip cardano.PlutusData
	if err := proto.Unmarshal(wire, &binaryRoundTrip); err != nil {
		t.Fatalf("binary consumer rejected depth %d (%d bytes): %v", depth, len(wire), err)
	}
	if !proto.Equal(mapped, &binaryRoundTrip) {
		t.Fatal("binary round trip changed the mapped datum")
	}
	jsonWire, err := protojson.Marshal(mapped)
	if err != nil {
		t.Fatal(err)
	}
	var jsonRoundTrip cardano.PlutusData
	if err := protojson.Unmarshal(jsonWire, &jsonRoundTrip); err != nil {
		t.Fatalf("JSON consumer rejected depth %d (%d bytes): %v", depth, len(jsonWire), err)
	}
	if !proto.Equal(mapped, &jsonRoundTrip) {
		t.Fatal("JSON round trip changed the mapped datum")
	}
}

func cardanoListDepth(d *cardano.PlutusData) int {
	depth := 0
	for d != nil {
		array := d.GetArray()
		if array == nil || len(array.GetItems()) != 1 {
			return depth
		}
		depth++
		d = array.GetItems()[0]
	}
	return depth
}
