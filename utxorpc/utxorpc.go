// Copyright 2024 Blink Labs Software
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

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/utxorpc"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/dingo/state"
)

type Utxorpc struct {
	config UtxorpcConfig
}

type UtxorpcConfig struct {
	Logger      *slog.Logger
	EventBus    *event.EventBus
	LedgerState *state.LedgerState
	Mempool     *mempool.Mempool
	Host        string
	Port        uint
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
	return utxorpc.Start(
		fmt.Sprintf(
			"%s:%d",
			u.config.Host,
			u.config.Port,
		),
		u.config.Logger,
	)
}
