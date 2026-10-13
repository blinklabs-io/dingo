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

package ledger

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

func TestCalculateCertificateDepositUsesPublishedPParams(t *testing.T) {
	t.Parallel()

	const (
		firstDeposit  = 2_000_000
		secondDeposit = 4_000_000
	)
	first := &shelley.ShelleyProtocolParameters{KeyDeposit: firstDeposit}
	second := &shelley.ShelleyProtocolParameters{KeyDeposit: secondDeposit}
	ls := &LedgerState{currentPParams: first}
	ls.publishSnapshotsLocked()
	cert := &lcommon.StakeRegistrationCertificate{}

	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		for range 10_000 {
			deposit, err := ls.calculateCertificateDeposit(
				cert,
				shelley.EraIdShelley,
				ls.loadConsensusSnapshot().currentPParams,
			)
			switch {
			case err != nil:
				errs <- err
				return
			case deposit == nil:
				errs <- errors.New(
					"certificate deposit reported unknown",
				)
				return
			case *deposit != firstDeposit && *deposit != secondDeposit:
				errs <- fmt.Errorf(
					"unexpected certificate deposit: got %d",
					*deposit,
				)
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := range 10_000 {
			ls.Lock()
			if i%2 == 0 {
				ls.currentPParams = second
			} else {
				ls.currentPParams = first
			}
			ls.publishSnapshotsLocked()
			ls.Unlock()
		}
	}()
	close(start)
	wg.Wait()
	select {
	case err := <-errs:
		t.Fatal(err)
	default:
	}
}
