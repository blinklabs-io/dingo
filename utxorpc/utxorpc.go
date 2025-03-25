// Copyright 2025 Blink Labs Software
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
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/grpchealth"
	"connectrpc.com/grpcreflect"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/dingo/state"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/query/queryconnect"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/submit/submitconnect"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync/syncconnect"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/watch/watchconnect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type Utxorpc struct {
	config UtxorpcConfig
}

type UtxorpcConfig struct {
	Logger          *slog.Logger
	EventBus        *event.EventBus
	LedgerState     *state.LedgerState
	Mempool         *mempool.Mempool
	Host            string
	Port            uint
	TlsCertFilePath string
	TlsKeyFilePath  string
}

func NewUtxorpc(cfg UtxorpcConfig) *Utxorpc {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	cfg.Logger = cfg.Logger.With("component", "utxorpc")
	if cfg.Host == "" {
		cfg.Host = "0.0.0.0"
	}
	if cfg.Port == 0 {
		cfg.Port = 9090
	}
	return &Utxorpc{
		config: cfg,
	}
}

func (u *Utxorpc) Start() error {
	mux := http.NewServeMux()
	compress1KB := connect.WithCompressMinBytes(1024)
	queryPath, queryHandler := queryconnect.NewQueryServiceHandler(
		&queryServiceServer{utxorpc: u},
		compress1KB,
	)
	submitPath, submitHandler := submitconnect.NewSubmitServiceHandler(
		&submitServiceServer{utxorpc: u},
		compress1KB,
	)
	syncPath, syncHandler := syncconnect.NewSyncServiceHandler(
		&syncServiceServer{utxorpc: u},
		compress1KB,
	)
	watchPath, watchHandler := watchconnect.NewWatchServiceHandler(
		&watchServiceServer{utxorpc: u},
		compress1KB,
	)
	mux.Handle(queryPath, queryHandler)
	mux.Handle(submitPath, submitHandler)
	mux.Handle(syncPath, syncHandler)
	mux.Handle(watchPath, watchHandler)
	mux.Handle(
		grpchealth.NewHandler(
			grpchealth.NewStaticChecker(
				queryconnect.QueryServiceName,
				submitconnect.SubmitServiceName,
				syncconnect.SyncServiceName,
				watchconnect.WatchServiceName,
			),
			compress1KB,
		),
	)
	mux.Handle(
		grpcreflect.NewHandlerV1(
			grpcreflect.NewStaticReflector(
				queryconnect.QueryServiceName,
				submitconnect.SubmitServiceName,
				syncconnect.SyncServiceName,
				watchconnect.WatchServiceName,
			),
			compress1KB,
		),
	)
	mux.Handle(
		grpcreflect.NewHandlerV1Alpha(
			grpcreflect.NewStaticReflector(
				queryconnect.QueryServiceName,
				submitconnect.SubmitServiceName,
				syncconnect.SyncServiceName,
				watchconnect.WatchServiceName,
			),
			compress1KB,
		),
	)
	if u.config.TlsCertFilePath != "" && u.config.TlsKeyFilePath != "" {
		u.config.Logger.Info(
			fmt.Sprintf(
				"starting gRPC TLS listener on %s:%d",
				u.config.Host,
				u.config.Port,
			),
		)
		utxorpc := &http.Server{
			Addr: fmt.Sprintf(
				"%s:%d",
				u.config.Host,
				u.config.Port,
			),
			Handler:           mux,
			ReadHeaderTimeout: 60 * time.Second,
		}
		return utxorpc.ListenAndServeTLS(
			u.config.TlsCertFilePath,
			u.config.TlsKeyFilePath,
		)
	} else {
		u.config.Logger.Info(
			fmt.Sprintf(
				"starting gRPC listener on %s:%d",
				u.config.Host,
				u.config.Port,
			),
		)
		utxorpc := &http.Server{
			Addr: fmt.Sprintf(
				"%s:%d",
				u.config.Host,
				u.config.Port,
			),
			// Use h2c so we can serve HTTP/2 without TLS
			Handler:           h2c.NewHandler(mux, &http2.Server{}),
			ReadHeaderTimeout: 60 * time.Second,
		}
		return utxorpc.ListenAndServe()
	}
}
