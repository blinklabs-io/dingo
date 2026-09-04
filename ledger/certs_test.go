package ledger

import (
	"fmt"
	"sync"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
)

func TestCalculateCertificateDepositUsesPublishedPParams(t *testing.T) {
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
		for i := 0; i < 10_000; i++ {
			deposit, err := ls.calculateCertificateDeposit(
				cert,
				shelley.EraIdShelley,
				ls.loadConsensusSnapshot().currentPParams,
			)
			if err != nil || (deposit != firstDeposit && deposit != secondDeposit) {
				if err != nil {
					errs <- err
				} else {
					errs <- fmt.Errorf("unexpected certificate deposit: got %d", deposit)
				}
				return
			}
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 10_000; i++ {
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
