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
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"
)

// epochBoundaryBenchShape is the row-count shape of a synthetic mainnet-like
// ledger. The defaults follow the mainnet 655->656 boundary: 1,309,350
// delegators across 2,676 pools and 1,053 DReps.
type epochBoundaryBenchShape struct {
	pools             int
	delegators        int
	dreps             int
	utxosPerDelegator int
	proposals         int
	drepVotes         int
	spoVotes          int
	ccMembers         int
}

func epochBoundaryBenchShapeFromEnv(tb testing.TB) epochBoundaryBenchShape {
	tb.Helper()
	shape := epochBoundaryBenchShape{
		pools:             2_676,
		delegators:        1_309_350,
		dreps:             1_053,
		utxosPerDelegator: 2,
		proposals:         40,
		drepVotes:         400,
		spoVotes:          300,
		ccMembers:         7,
	}
	envInt := func(name string, dst *int) {
		raw := os.Getenv(name)
		if raw == "" {
			return
		}
		v, err := strconv.Atoi(raw)
		require.NoError(tb, err, name)
		*dst = v
	}
	envInt("DINGO_BENCH_POOLS", &shape.pools)
	envInt("DINGO_BENCH_DELEGATORS", &shape.delegators)
	envInt("DINGO_BENCH_DREPS", &shape.dreps)
	envInt("DINGO_BENCH_UTXOS_PER_DELEGATOR", &shape.utxosPerDelegator)
	envInt("DINGO_BENCH_PROPOSALS", &shape.proposals)
	return shape
}

const (
	epochBoundaryBenchEpochLength = uint64(432_000)
	// The rollover under measurement ends this epoch, so it applies the
	// reward round whose snapshot, performance and pots epochs are 8, 9 and
	// 10.
	epochBoundaryBenchEndedEpoch = uint64(10)
	epochBoundaryBenchMaxSupply  = uint64(45_000_000_000_000_000)
	epochBoundaryBenchReserves   = uint64(7_600_000_000_000_000)
	epochBoundaryBenchTreasury   = uint64(1_600_000_000_000_000)
	epochBoundaryBenchFees       = uint64(31_000_000_000)
	epochBoundaryBenchBlocks     = 21_600
)

func epochBoundaryBenchStart(epoch uint64) uint64 {
	return epoch * epochBoundaryBenchEpochLength
}

func epochBoundaryBenchNodeConfig(tb testing.TB) *cardano.CardanoNodeConfig {
	tb.Helper()
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("5a", 32),
	}
	require.NoError(tb, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": 0.05,
		"epochLength": 432000,
		"maxLovelaceSupply": 45000000000000000,
		"securityParam": 2160,
		"slotLength": 1,
		"updateQuorum": 5,
		"systemStart": "2017-09-23T21:44:51Z"
	}`)))
	return cfg
}

func epochBoundaryBenchPParams() *conway.ConwayProtocolParameters {
	rat := func(n, d int64) *cbor.Rat { return &cbor.Rat{Rat: big.NewRat(n, d)} }
	p := donationTestConwayPParams(10)
	p.MinFeeA = 44
	p.MinFeeB = 155_381
	p.MaxBlockBodySize = 90_112
	p.MaxTxSize = 16_384
	p.MaxBlockHeaderSize = 1_100
	p.KeyDeposit = 2_000_000
	p.PoolDeposit = 500_000_000
	p.MaxEpoch = 18
	p.NOpt = 500
	p.A0 = rat(3, 10)
	p.Rho = rat(3, 1000)
	p.Tau = rat(1, 5)
	p.MinPoolCost = 170_000_000
	p.AdaPerUtxoByte = 4_310
	p.MinCommitteeSize = 7
	p.CommitteeTermLimit = 146
	p.GovActionValidityPeriod = 6
	p.GovActionDeposit = 100_000_000_000
	p.DRepDeposit = 500_000_000
	p.DRepInactivityPeriod = 20
	p.MinFeeRefScriptCostPerByte = rat(15, 1)
	return p
}

// epochBoundaryBenchHash returns a deterministic 28-byte hash in its own
// domain, so credentials, pools and DReps never collide.
func epochBoundaryBenchHash(domain byte, index uint64) []byte {
	h := make([]byte, 28)
	h[0] = domain
	binary.BigEndian.PutUint64(h[20:], index)
	return h
}

// splitmix64 gives the fixture a heavy-tailed but reproducible stake
// distribution without seeding math/rand.
func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

// epochBoundaryBenchStake is log-uniform between 1 ADA and 100,000 ADA,
// which puts 1.3M delegators at roughly 11B ADA of active stake.
func epochBoundaryBenchStake(index uint64) uint64 {
	u := float64(splitmix64(index)>>11) / float64(1<<53)
	return uint64(math.Pow(10, 6+5*u))
}

type epochBoundaryBenchFixture struct {
	ls      *LedgerState
	db      *database.Database
	shape   epochBoundaryBenchShape
	pparams *conway.ConwayProtocolParameters
	epochs  map[uint64]models.Epoch
	phases  *epochBoundaryPhaseRecorder
}

// epochBoundaryPhaseRecorder collects the "epoch rollover phase" Debug records
// timeRolloverPhase emits, so the benchmark reports the same per-phase
// durations an operator reads from the log.
type epochBoundaryPhaseRecorder struct {
	mu     sync.Mutex
	phases []epochBoundaryPhase
}

type epochBoundaryPhase struct {
	name     string
	duration time.Duration
}

func (r *epochBoundaryPhaseRecorder) Enabled(
	context.Context,
	slog.Level,
) bool {
	return true
}

func (r *epochBoundaryPhaseRecorder) Handle(
	_ context.Context,
	rec slog.Record,
) error {
	if rec.Message != "epoch rollover phase" {
		return nil
	}
	var name string
	var seconds float64
	rec.Attrs(func(a slog.Attr) bool {
		switch a.Key {
		case "phase":
			name = a.Value.String()
		case "duration_seconds":
			seconds = a.Value.Float64()
		}
		return true
	})
	r.mu.Lock()
	r.phases = append(r.phases, epochBoundaryPhase{
		name:     name,
		duration: time.Duration(seconds * float64(time.Second)),
	})
	r.mu.Unlock()
	return nil
}

func (r *epochBoundaryPhaseRecorder) WithAttrs([]slog.Attr) slog.Handler {
	return r
}

func (r *epochBoundaryPhaseRecorder) WithGroup(string) slog.Handler {
	return r
}

func (r *epochBoundaryPhaseRecorder) reset() {
	r.mu.Lock()
	r.phases = nil
	r.mu.Unlock()
}

func (r *epochBoundaryPhaseRecorder) snapshot() []epochBoundaryPhase {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]epochBoundaryPhase(nil), r.phases...)
}

// newEpochBoundaryBenchFixture seeds a fresh database, or copies the seeded
// template in templateDir when one is named.
func newEpochBoundaryBenchFixture(
	tb testing.TB,
	shape epochBoundaryBenchShape,
	templateDir string,
) *epochBoundaryBenchFixture {
	tb.Helper()
	dataDir := tb.TempDir()
	if template := templateDir; template != "" {
		epochBoundaryBenchTemplate(tb, template, shape)
		require.NoError(tb, os.CopyFS(
			dataDir, os.DirFS(filepath.Join(template, "data")),
		))
	}
	db, err := dbtest.NewDatabase(tb, &database.Config{DataDir: dataDir})
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = dbtest.CloseDatabase(db) })
	f := &epochBoundaryBenchFixture{
		db:      db,
		shape:   shape,
		pparams: epochBoundaryBenchPParams(),
		epochs:  epochBoundaryBenchEpochs(),
		phases:  &epochBoundaryPhaseRecorder{},
	}
	if templateDir == "" {
		f.seed(tb)
	}
	f.wire(tb)
	return f
}

// epochBoundaryBenchTemplate seeds the shared template once, so repeated
// runs -- and runs of two different trees -- measure the same database.
func epochBoundaryBenchTemplate(
	tb testing.TB,
	dir string,
	shape epochBoundaryBenchShape,
) {
	tb.Helper()
	ready := filepath.Join(dir, "ready")
	want := fmt.Sprintf("%+v", shape)
	if raw, err := os.ReadFile(ready); err == nil {
		require.Equal(tb, want, string(raw), "template shape mismatch")
		return
	}
	dataDir := filepath.Join(dir, "data")
	require.NoError(tb, os.RemoveAll(dataDir))
	require.NoError(tb, os.MkdirAll(dataDir, 0o755))
	db, err := dbtest.NewDatabase(tb, &database.Config{DataDir: dataDir})
	require.NoError(tb, err)
	f := &epochBoundaryBenchFixture{
		db:      db,
		shape:   shape,
		pparams: epochBoundaryBenchPParams(),
		epochs:  epochBoundaryBenchEpochs(),
	}
	f.seed(tb)
	require.NoError(tb, dbtest.CloseDatabase(db))
	require.NoError(tb, os.WriteFile(ready, []byte(want), 0o644))
}

func (f *epochBoundaryBenchFixture) seed(tb testing.TB) {
	tb.Helper()
	start := time.Now()
	f.seedEpochs(tb)
	f.seedBulk(tb)
	bulk := time.Now()
	f.seedGovernance(tb)
	tb.Logf(
		"seeded: bulk %.1fs, governance %.1fs",
		bulk.Sub(start).Seconds(), time.Since(bulk).Seconds(),
	)
}

func (f *epochBoundaryBenchFixture) wire(tb testing.TB) {
	tb.Helper()
	db := f.db
	f.ls = &LedgerState{
		db:             db,
		currentEra:     eras.ConwayEraDesc,
		currentEpoch:   f.epochs[epochBoundaryBenchEndedEpoch],
		currentPParams: f.pparams,
		config: LedgerStateConfig{
			CardanoNodeConfig: epochBoundaryBenchNodeConfig(tb),
			Logger:            slog.New(f.phases),
		},
	}
	mgr := snapshot.NewManager(db, nil, slog.New(slog.NewTextHandler(
		io.Discard, nil,
	)))
	f.ls.SetEpochBoundarySnapshotStakeHook(
		func(txn *database.Txn, evt event.EpochTransitionEvent) error {
			return mgr.ComputeEpochBoundarySnapshot(
				context.Background(),
				txn,
				evt,
			)
		},
	)
	f.ls.SetEpochBoundarySnapshotHook(
		func(txn *database.Txn, evt event.EpochTransitionEvent) error {
			return mgr.CaptureEpochBoundarySnapshot(
				context.Background(),
				txn,
				evt,
			)
		},
	)
	epochBoundaryBenchWireDeferred(f.ls, mgr)
	wireDeferredBoundarySnapshot(f.ls, mgr)
	f.ls.SetCurrentBoundarySPOStakeHook(
		func(
			txn *database.Txn,
			evt event.EpochTransitionEvent,
		) ([]*models.PoolStakeSnapshot, error) {
			return mgr.CurrentBoundarySPOStakeRows(
				context.Background(),
				txn,
				evt,
			)
		},
	)
}

func epochBoundaryBenchNonce(epoch uint64) []byte {
	nonce := make([]byte, 32)
	binary.BigEndian.PutUint64(nonce[24:], epoch+1)
	return nonce
}

func epochBoundaryBenchEpochs() map[uint64]models.Epoch {
	epochs := make(map[uint64]models.Epoch)
	for epoch := uint64(0); epoch <= epochBoundaryBenchEndedEpoch; epoch++ {
		nonce := epochBoundaryBenchNonce(epoch)
		epochs[epoch] = models.Epoch{
			EpochId:             epoch,
			StartSlot:           epochBoundaryBenchStart(epoch),
			Nonce:               nonce,
			EvolvingNonce:       nonce,
			CandidateNonce:      nonce,
			LastEpochBlockNonce: nonce,
			EraId:               eras.ConwayEraDesc.Id,
			SlotLength:          1_000,
			LengthInSlots:       uint(epochBoundaryBenchEpochLength),
		}
	}
	return epochs
}

func (f *epochBoundaryBenchFixture) seedEpochs(tb testing.TB) {
	tb.Helper()
	pparamsCbor, err := cbor.Encode(f.pparams)
	require.NoError(tb, err)
	for epoch := uint64(0); epoch <= epochBoundaryBenchEndedEpoch; epoch++ {
		e := f.epochs[epoch]
		require.NoError(tb, f.db.SetEpoch(
			e.StartSlot, epoch, e.Nonce, e.EvolvingNonce, e.CandidateNonce,
			e.LastEpochBlockNonce, e.EraId, e.SlotLength, e.LengthInSlots,
			nil,
		))
		require.NoError(tb, f.db.SetPParams(
			pparamsCbor, e.StartSlot, epoch, eras.ConwayEraDesc.Id, nil,
		))
	}
	require.NoError(tb, f.db.Metadata().SetNetworkState(
		epochBoundaryBenchTreasury,
		epochBoundaryBenchReserves,
		epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch),
		nil,
	))
}

type epochBoundaryBenchPool struct {
	key           []byte
	rewardAccount []byte
	margin        string
	cost          uint64
	pledge        uint64
	stake         uint64
	delegators    int
}

// seedBulk writes the delegator-scaled tables directly through SQL: at 1.3M
// rows, the model writers' per-row bookkeeping would dominate setup.
func (f *epochBoundaryBenchFixture) seedBulk(tb testing.TB) {
	tb.Helper()
	raw, err := dbtest.RawSQLiteMetadata(tb, f.db)
	require.NoError(tb, err)
	defer raw.Close()
	tx, err := raw.Begin()
	require.NoError(tb, err)
	defer func() { _ = tx.Rollback() }()
	prepare := func(query string) *sql.Stmt {
		stmt, err := tx.Prepare(query)
		require.NoError(tb, err)
		return stmt
	}
	poolStmt := prepare(`
INSERT INTO pool (id, pool_key_hash, vrf_key_hash, reward_account,
    reward_account_credential_tag, margin, pledge, cost)
VALUES (?, ?, ?, ?, 0, ?, ?, ?)`)
	poolRegStmt := prepare(`
INSERT INTO pool_registration (id, pool_id, pool_key_hash, vrf_key_hash,
    reward_account, reward_account_credential_tag, margin, pledge, cost,
    added_slot, deposit_amount, deposit_held)
VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, 1, '500000000', '500000000')`)
	ownerStmt := prepare(`
INSERT INTO pool_registration_owner (key_hash, pool_registration_id, pool_id)
VALUES (?, ?, ?)`)
	accountStmt := prepare(`
INSERT INTO account (staking_key, credential_tag, pool, drep, drep_type,
    added_slot, created_slot, reward, active, expiration_epoch)
VALUES (?, 0, ?, ?, ?, 1, 1, ?, 1, 0)`)
	liveStmt := prepare(`
INSERT INTO reward_live_stake (pool_key_hash, staking_key, credential_tag,
    utxo_stake, reward_stake, total_stake, registered, pool_delegation_slot,
    updated_slot, calculation_version)
VALUES (?, ?, 0, ?, ?, ?, 1, 1, 1, ?)`)
	utxoStmt := prepare(`
INSERT INTO utxo (tx_id, output_idx, payment_key, staking_key, credential_tag,
    added_slot, deleted_slot, amount, payment_script)
VALUES (?, ?, ?, ?, 0, 1, 0, ?, 0)`)
	stakeInputStmt := prepare(`
INSERT INTO reward_stake_input (pool_key_hash, staking_key, epoch,
    credential_tag, stake, owner, registered, captured_slot, boundary_slot)
VALUES (?, ?, ?, 0, ?, ?, 1, ?, ?)`)
	poolInputStmt := prepare(`
INSERT INTO reward_pool_input (margin, pool_key_hash, reward_account, epoch,
    pledge, delegated_stake, owner_stake, cost, delegator_count,
    reward_account_credential_tag, captured_slot, boundary_slot)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)`)
	poolSnapStmt := prepare(`
INSERT INTO pool_stake_snapshot (epoch, snapshot_type, pool_key_hash,
    total_stake, delegator_count, captured_slot, calculation_version)
VALUES (?, 'mark', ?, ?, ?, ?, ?)`)
	blockStmt := prepare(`
INSERT INTO pool_opcert_sequence (pool_key_hash, slot, sequence)
VALUES (?, ?, 0)`)

	shape := f.shape
	pools := make([]epochBoundaryBenchPool, shape.pools)
	for p := range pools {
		pool := &pools[p]
		pool.key = epochBoundaryBenchHash(0x10, uint64(p)+1)
		pool.rewardAccount = epochBoundaryBenchHash(0x20, uint64(p)+1)
		// Margins span the reference's rounding extremes: 0, 1 and
		// ordinary fractions.
		switch p % 7 {
		case 0:
			pool.margin = "0/1"
		case 1:
			pool.margin = "1/1"
		default:
			pool.margin = fmt.Sprintf("%d/1000", 5+(p%40))
		}
		pool.cost = 170_000_000 + uint64(p%5)*85_000_000
		pool.pledge = 1_000_000_000 * (1 + uint64(p%50))
	}
	// Every pool's own reward account delegates its pledge to the pool, so
	// the pledge check passes and owners are exercised.
	calcVersion := models.RewardStakeCalculationVersion
	var nextUtxo uint64
	delegatorPool := func(d int) int {
		// A skewed assignment: low-numbered pools attract more delegators,
		// and the last pool attracts none, so a zero-stake pool is present.
		u := float64(splitmix64(uint64(d)^0xabcdef)>>11) / float64(1<<53)
		p := int(float64(shape.pools-1) * u * u)
		if p >= shape.pools-1 {
			p = shape.pools - 2
		}
		return p
	}
	type stakeRow struct {
		pool  int
		key   []byte
		stake uint64
		owner bool
	}
	rows := make([]stakeRow, 0, shape.delegators+shape.pools)
	writeAccount := func(
		key []byte, pool []byte, stake uint64, reward uint64, d uint64,
	) {
		var drep any
		drepType := models.DrepTypeAddrKeyHash
		switch r := splitmix64(d^0x77) % 20; {
		case r < 11 && shape.dreps > 0:
			drep = epochBoundaryBenchHash(
				0x40,
				splitmix64(d)%uint64(shape.dreps)+1,
			)
		case r < 14:
			drepType = models.DrepTypeAlwaysAbstain
		case r < 15:
			drepType = models.DrepTypeAlwaysNoConfidence
		default:
			drepType = 0
		}
		_, err := accountStmt.Exec(
			key, pool, drep, drepType, strconv.FormatUint(reward, 10),
		)
		require.NoError(tb, err)
		utxoStake := stake - reward
		_, err = liveStmt.Exec(
			pool, key,
			strconv.FormatUint(utxoStake, 10),
			strconv.FormatUint(reward, 10),
			strconv.FormatUint(stake, 10),
			calcVersion,
		)
		require.NoError(tb, err)
		n := max(shape.utxosPerDelegator, 1)
		remaining := utxoStake
		for i := range n {
			amount := remaining / uint64(n-i)
			remaining -= amount
			nextUtxo++
			txID := make([]byte, 32)
			binary.BigEndian.PutUint64(txID[24:], nextUtxo)
			_, err := utxoStmt.Exec(
				txID, 0, epochBoundaryBenchHash(0x50, nextUtxo), key,
				strconv.FormatUint(amount, 10),
			)
			require.NoError(tb, err)
		}
	}
	for p := range pools {
		pool := &pools[p]
		_, err := poolStmt.Exec(
			p+1, pool.key, epochBoundaryBenchHash(0x11, uint64(p)+1),
			pool.rewardAccount, pool.margin,
			strconv.FormatUint(pool.pledge, 10),
			strconv.FormatUint(pool.cost, 10),
		)
		require.NoError(tb, err)
		_, err = poolRegStmt.Exec(
			p+1, p+1, pool.key, epochBoundaryBenchHash(0x11, uint64(p)+1),
			pool.rewardAccount, pool.margin,
			strconv.FormatUint(pool.pledge, 10),
			strconv.FormatUint(pool.cost, 10),
		)
		require.NoError(tb, err)
		_, err = ownerStmt.Exec(pool.rewardAccount, p+1, p+1)
		require.NoError(tb, err)
		if p == shape.pools-1 {
			// The zero-stake pool: registered, no delegators, not even its
			// owner.
			continue
		}
		ownerStake := pool.pledge
		writeAccount(
			pool.rewardAccount, pool.key, ownerStake, 0,
			uint64(p)+0x1_0000_0000,
		)
		rows = append(rows, stakeRow{
			pool: p, key: pool.rewardAccount, stake: ownerStake, owner: true,
		})
		pool.stake += ownerStake
		pool.delegators++
	}
	for d := range shape.delegators {
		p := delegatorPool(d)
		key := epochBoundaryBenchHash(0x30, uint64(d)+1)
		stake := epochBoundaryBenchStake(uint64(d))
		reward := stake / 200
		writeAccount(key, pools[p].key, stake, reward, uint64(d))
		rows = append(rows, stakeRow{pool: p, key: key, stake: stake})
		pools[p].stake += stake
		pools[p].delegators++
	}
	var totalStake uint64
	for _, pool := range pools {
		totalStake += pool.stake
	}
	// The go, set and mark reward bases (snapshot epochs 8, 9, 10) and
	// their leader-election rows. Stake inputs are identical across the
	// three epochs: only the row count matters to the boundary's cost.
	for _, epoch := range []uint64{8, 9, 10} {
		boundary := epochBoundaryBenchStart(epoch)
		captured := boundary - 1
		for _, row := range rows {
			_, err := stakeInputStmt.Exec(
				pools[row.pool].key, row.key, epoch,
				strconv.FormatUint(row.stake, 10), row.owner,
				captured, boundary,
			)
			require.NoError(tb, err)
		}
		var poolCount, delegatorCount int
		for p := range pools {
			pool := &pools[p]
			if pool.delegators == 0 {
				continue
			}
			poolCount++
			delegatorCount += pool.delegators
			_, err := poolInputStmt.Exec(
				pool.margin, pool.key, pool.rewardAccount, epoch,
				strconv.FormatUint(pool.pledge, 10),
				strconv.FormatUint(pool.stake, 10),
				strconv.FormatUint(pool.pledge, 10),
				strconv.FormatUint(pool.cost, 10),
				pool.delegators, captured, boundary,
			)
			require.NoError(tb, err)
			_, err = poolSnapStmt.Exec(
				epoch, pool.key, strconv.FormatUint(pool.stake, 10),
				pool.delegators, captured, calcVersion,
			)
			require.NoError(tb, err)
		}
		_, err := tx.Exec(`
INSERT INTO reward_snapshot (epoch, snapshot_type, total_active_stake,
    total_pool_count, total_delegators, captured_slot, boundary_slot,
    epoch_nonce, protocol_version, authoritative, calculation_version,
    excluded_active_stake)
VALUES (?, 'mark', ?, ?, ?, ?, ?, ?, 10, 1, ?, '0')`,
			epoch, strconv.FormatUint(totalStake, 10), poolCount,
			delegatorCount, captured, boundary,
			f.epochs[epoch].Nonce, calcVersion,
		)
		require.NoError(tb, err)
		_, err = tx.Exec(`
INSERT INTO epoch_summary (epoch, total_active_stake, total_pool_count,
    total_delegators, epoch_nonce, boundary_slot, snapshot_ready)
VALUES (?, ?, ?, ?, ?, ?, 1)`,
			epoch, strconv.FormatUint(totalStake, 10), poolCount,
			delegatorCount, f.epochs[epoch].Nonce, boundary,
		)
		require.NoError(tb, err)
	}
	// Performance epoch 9: blocks in proportion to stake.
	perfStart := epochBoundaryBenchStart(9)
	slot := perfStart
	for p := range pools {
		share := uint64(
			float64(epochBoundaryBenchBlocks) *
				float64(pools[p].stake) / float64(totalStake),
		)
		for range share {
			_, err := blockStmt.Exec(pools[p].key, slot)
			require.NoError(tb, err)
			slot += 20
		}
	}
	_, err = tx.Exec(`
INSERT INTO reward_ada_pots (epoch, treasury, reserves, fees, rewards,
    captured_slot)
VALUES (?, ?, ?, ?, '0', ?)`,
		epochBoundaryBenchEndedEpoch,
		strconv.FormatUint(epochBoundaryBenchTreasury, 10),
		strconv.FormatUint(epochBoundaryBenchReserves, 10),
		strconv.FormatUint(epochBoundaryBenchFees, 10),
		epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch),
	)
	require.NoError(tb, err)
	_, err = tx.Exec(
		`INSERT INTO tip (hash, slot, block_number) VALUES (?, ?, ?)`,
		make([]byte, 32),
		epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch+1)-1,
		10_000_000,
	)
	require.NoError(tb, err)
	require.NoError(tb, tx.Commit())
}

func (f *epochBoundaryBenchFixture) seedGovernance(tb testing.TB) {
	tb.Helper()
	shape := f.shape
	raw, err := dbtest.RawSQLiteMetadata(tb, f.db)
	require.NoError(tb, err)
	defer raw.Close()
	tx, err := raw.Begin()
	require.NoError(tb, err)
	defer func() { _ = tx.Rollback() }()
	drepStmt, err := tx.Prepare(`
INSERT INTO drep (credential, credential_tag, added_slot, last_activity_epoch,
    expiry_epoch, active)
VALUES (?, 0, 1, ?, ?, 1)`)
	require.NoError(tb, err)
	drepRegStmt, err := tx.Prepare(`
INSERT INTO registration_drep (drep_credential, credential_tag, added_slot,
    deposit_amount)
VALUES (?, 0, 1, '500000000')`)
	require.NoError(tb, err)
	for r := range shape.dreps {
		cred := epochBoundaryBenchHash(0x40, uint64(r)+1)
		_, err := drepStmt.Exec(cred, epochBoundaryBenchEndedEpoch, 40)
		require.NoError(tb, err)
		_, err = drepRegStmt.Exec(cred)
		require.NoError(tb, err)
	}
	for c := range shape.ccMembers {
		_, err := tx.Exec(`
INSERT INTO auth_committee_hot (cold_credential, host_credential,
    certificate_id, added_slot)
VALUES (?, ?, ?, 1)`,
			epochBoundaryBenchHash(0x60, uint64(c)+1),
			epochBoundaryBenchHash(0x61, uint64(c)+1),
			c+1,
		)
		require.NoError(tb, err)
	}
	require.NoError(tb, tx.Commit())

	members := make([]*models.CommitteeMember, 0, shape.ccMembers)
	for c := range shape.ccMembers {
		members = append(members, &models.CommitteeMember{
			ColdCredHash: epochBoundaryBenchHash(0x60, uint64(c)+1),
			ExpiresEpoch: 100,
			AddedSlot:    1,
		})
	}
	require.NoError(tb, f.db.SetCommitteeMembers(members, nil))
	require.NoError(tb, f.db.SetCommitteeQuorum(big.NewRat(2, 3), 1, nil))

	for i := range shape.proposals {
		returnKey := epochBoundaryBenchHash(0x30, uint64(i)*997+1)
		returnAddr, err := lcommon.NewAddressFromParts(
			lcommon.AddressTypeNoneKey, lcommon.AddressNetworkMainnet,
			nil, returnKey,
		)
		require.NoError(tb, err)
		returnAddrBytes, err := returnAddr.Bytes()
		require.NoError(tb, err)
		var actionType lcommon.GovActionType
		var actionCbor []byte
		if i%4 == 0 {
			actionType = lcommon.GovActionTypeTreasuryWithdrawal
			actionCbor, err = cbor.Encode(&lcommon.TreasuryWithdrawalGovAction{
				Type: uint(lcommon.GovActionTypeTreasuryWithdrawal),
				Withdrawals: map[*lcommon.Address]uint64{
					&returnAddr: 1_000_000_000_000,
				},
			})
		} else {
			actionType = lcommon.GovActionTypeInfo
			actionCbor, err = cbor.Encode(&lcommon.InfoGovAction{
				Type: uint(lcommon.GovActionTypeInfo),
			})
		}
		require.NoError(tb, err)
		txHash := make([]byte, 32)
		binary.BigEndian.PutUint64(txHash[24:], uint64(i)+1)
		txHash[0] = 0x70
		proposal := &models.GovernanceProposal{
			TxHash:        txHash,
			ActionIndex:   0,
			ActionType:    uint8(actionType),
			ProposedEpoch: epochBoundaryBenchEndedEpoch - uint64(i%3),
			ExpiresEpoch:  epochBoundaryBenchEndedEpoch + 6,
			AnchorURL:     "https://example.invalid/proposal",
			AnchorHash:    txHash,
			Deposit:       f.pparams.GovActionDeposit,
			ReturnAddress: returnAddrBytes,
			GovActionCbor: actionCbor,
			AddedSlot: epochBoundaryBenchStart(
				epochBoundaryBenchEndedEpoch,
			) + 100,
		}
		require.NoError(tb, f.db.SetGovernanceProposal(proposal, nil))
		vote := func(voterType uint8, cred []byte, choice uint8) {
			require.NoError(tb, f.db.SetGovernanceVote(&models.GovernanceVote{
				ProposalID:      proposal.ID,
				VoterType:       voterType,
				VoterCredential: cred,
				Vote:            choice,
				AddedSlot:       proposal.AddedSlot + 1,
			}, nil))
		}
		for v := range min(shape.drepVotes, shape.dreps) {
			r := (uint64(i)*131 + uint64(v)) % uint64(shape.dreps)
			vote(
				models.VoterTypeDRep,
				epochBoundaryBenchHash(0x40, r+1),
				uint8(splitmix64(r^uint64(i))%3),
			)
		}
		for v := range min(shape.spoVotes, shape.pools) {
			p := (uint64(i)*17 + uint64(v)) % uint64(shape.pools)
			vote(
				models.VoterTypeSPO,
				epochBoundaryBenchHash(0x10, p+1),
				uint8(splitmix64(p^uint64(i))%3),
			)
		}
		for c := range shape.ccMembers {
			vote(
				models.VoterTypeCC,
				epochBoundaryBenchHash(0x61, uint64(c)+1),
				models.VoteYes,
			)
		}
	}
}

// rollover runs the real boundary in one write transaction, exactly as the
// block pipeline does, and returns the time spent inside the transaction body
// and in its commit.
func (f *epochBoundaryBenchFixture) rollover(
	tb testing.TB,
) (time.Duration, time.Duration, []epochBoundaryPhase) {
	tb.Helper()
	f.phases.reset()
	start := time.Now()
	f.ls.fenceRewardPrecompute()
	var bodyDone time.Time
	txn := f.db.Transaction(true)
	err := txn.Do(func(txn *database.Txn) error {
		_, err := f.ls.processEpochRollover(
			txn,
			f.epochs[epochBoundaryBenchEndedEpoch],
			eras.ConwayEraDesc,
			f.pparams,
			false,
		)
		bodyDone = time.Now()
		return err
	})
	require.NoError(tb, err)
	end := time.Now()
	return bodyDone.Sub(start), end.Sub(bodyDone), f.phases.snapshot()
}

// precomputeEvent is the epoch transition into the ended epoch: the event
// the reward precompute for the measured boundary is queued from.
func epochBoundaryBenchPrecomputeEvent() event.EpochTransitionEvent {
	return event.EpochTransitionEvent{
		PreviousEpoch: epochBoundaryBenchEndedEpoch - 1,
		NewEpoch:      epochBoundaryBenchEndedEpoch,
		BoundarySlot:  epochBoundaryBenchStart(epochBoundaryBenchEndedEpoch),
		SnapshotSlot: epochBoundaryBenchStart(
			epochBoundaryBenchEndedEpoch,
		) - 1,
	}
}

func reportEpochBoundaryPhases(
	b *testing.B,
	label string,
	body, commit time.Duration,
	phases []epochBoundaryPhase,
) {
	b.Helper()
	sorted := append([]epochBoundaryPhase(nil), phases...)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].duration > sorted[j].duration
	})
	var sb strings.Builder
	fmt.Fprintf(
		&sb, "%s: whole boundary %.3fs (body %.3fs, commit %.3fs)",
		label, (body + commit).Seconds(), body.Seconds(), commit.Seconds(),
	)
	for _, phase := range sorted {
		fmt.Fprintf(&sb, "; %s %.3fs", phase.name, phase.duration.Seconds())
	}
	b.Log(sb.String())
	b.ReportMetric((body + commit).Seconds(), "boundary_s")
	for _, phase := range phases {
		b.ReportMetric(phase.duration.Seconds(), phase.name+"_s")
	}
}

// BenchmarkEpochBoundaryMainnetShape measures the whole epoch boundary --
// every phase of processEpochRollover plus its commit -- on a mainnet-shaped
// ledger, with the reward precompute complete, partial and missing. Run it
// with -benchtime=1x: each sub-benchmark seeds its own database, which takes
// longer than the boundary it measures. DINGO_BENCH_DELEGATORS and
// DINGO_BENCH_POOLS scale the shape down for a quick run.
func BenchmarkEpochBoundaryMainnetShape(b *testing.B) {
	shape := epochBoundaryBenchShapeFromEnv(b)
	for _, state := range []string{"complete", "partial", "missing"} {
		b.Run("precompute="+state, func(b *testing.B) {
			for range b.N {
				b.StopTimer()
				f := newEpochBoundaryBenchFixture(
					b, shape, os.Getenv("DINGO_BENCH_TEMPLATE_DIR"),
				)
				precomputeStart := time.Now()
				switch state {
				case "complete":
					require.NoError(
						b,
						f.ls.precomputeStakeRewardsAfterEpochTransition(
							epochBoundaryBenchPrecomputeEvent(),
						),
					)
				case "partial":
					epochBoundaryBenchPartialPrecompute(b, f)
				}
				b.Logf(
					"precompute (%s, off the apply path): %.3fs",
					state, time.Since(precomputeStart).Seconds(),
				)
				b.StartTimer()
				body, commit, phases := f.rollover(b)
				b.StopTimer()
				reportEpochBoundaryPhases(
					b, "precompute="+state, body, commit, phases,
				)
				completion := time.Now()
				f.ls.waitEpochBoundaryBenchBackground()
				b.Logf(
					"background completion after the boundary: %.3fs",
					time.Since(completion).Seconds(),
				)
			}
		})
	}
}
