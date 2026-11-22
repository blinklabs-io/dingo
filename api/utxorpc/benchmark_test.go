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
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/query"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/query/queryconnect"
)

// BenchmarkReadParamsRequest measures one utxorpc QueryService/ReadParams
// round trip over a real HTTP connection: client marshal, connect handler,
// the Utxorpc query implementation, and response marshal. The ledger behind it
// is the in-package stub that already backs the ReadParams tests, so the
// number tracks request-path cost and does not vary with storage.
func BenchmarkReadParamsRequest(b *testing.B) {
	stub := &shelleyLedgerStub{
		byronLedgerStub: byronLedgerStub{
			tip: ochainsync.Tip{
				Point: ocommon.NewPoint(42, []byte{0xab, 0xcd}),
			},
		},
		pparams: testShelleyPParams(),
	}
	u := NewUtxorpc(UtxorpcConfig{
		Logger:        slog.New(slog.NewJSONHandler(io.Discard, nil)),
		LedgerState:   stub,
		QueryDatabase: stubQueryDB(stub),
	})
	mux := http.NewServeMux()
	mux.Handle(
		queryconnect.NewQueryServiceHandler(&queryServiceServer{utxorpc: u}),
	)
	srv := httptest.NewServer(mux)
	b.Cleanup(srv.Close)
	client := queryconnect.NewQueryServiceClient(srv.Client(), srv.URL)
	ctx := context.Background()

	b.ReportAllocs()
	for b.Loop() {
		resp, err := client.ReadParams(
			ctx,
			connect.NewRequest(&query.ReadParamsRequest{}),
		)
		if err != nil {
			b.Fatalf("ReadParams: %v", err)
		}
		if resp.Msg.GetLedgerTip().GetSlot() != 42 {
			b.Fatalf(
				"unexpected tip slot %d",
				resp.Msg.GetLedgerTip().GetSlot(),
			)
		}
	}
}
