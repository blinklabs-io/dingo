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

package koios

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/btcsuite/btcd/btcutil/bech32"
	"gorm.io/gorm"
)

// Mismatch records a single field discrepancy between Dingo and Koios.
type Mismatch struct {
	// Epoch is always set.
	Epoch uint64
	// Pool is set for pool-level mismatches (bech32 pool ID).
	Pool string
	// Account is set for account-level mismatches (bech32 stake address).
	Account string
	// Field is the name of the mismatched attribute.
	Field string
	// Dingo is the value held in the local Dingo database.
	Dingo string
	// Koios is the value returned by the Koios API.
	Koios string
}

func (m Mismatch) String() string {
	who := fmt.Sprintf("epoch=%d", m.Epoch)
	if m.Pool != "" {
		who += " pool=" + m.Pool
	}
	if m.Account != "" {
		who += " account=" + m.Account
	}
	return fmt.Sprintf("%s field=%s dingo=%s koios=%s", who, m.Field, m.Dingo, m.Koios)
}

// Report is the result of a full parity run.
type Report struct {
	EpochsChecked   int
	PoolsChecked    int
	AccountsChecked int
	Mismatches      []Mismatch
}

// Checker compares Dingo's closed-epoch reward state against Koios.
// Open the Dingo SQLite database with gorm before creating a Checker:
//
//	db, _ := gorm.Open(sqlite.Open(path+"?mode=ro"), &gorm.Config{...})
//	checker, _ := koios.NewChecker(db, koios.NetworkPreview, "")
type Checker struct {
	db      *gorm.DB
	koios   *Client
	network string
}

// NewChecker constructs a Checker using an open GORM connection and the named
// Koios network (preview|preprod). Pass apiKey="" for anonymous (rate-limited)
// access.
func NewChecker(db *gorm.DB, network, apiKey string) (*Checker, error) {
	client, err := NewClient(network, apiKey)
	if err != nil {
		return nil, err
	}
	return &Checker{db: db, koios: client, network: network}, nil
}

// Run checks all closed epochs in the Dingo database against Koios.
//
// Only epochs with EpochSummary.SnapshotReady = true are examined (closed
// epochs). The current (live) epoch is never included even if it happens to
// appear in the table.
//
// maxPoolsPerEpoch limits how many RewardPoolInputs are sampled per epoch (0
// disables pool-level checks). maxAccounts limits the sampled stake addresses
// for balance checks (0 disables account-level checks). A courtesy delay is
// inserted between Koios requests to stay within public rate limits.
func (c *Checker) Run(
	ctx context.Context,
	maxPoolsPerEpoch int,
	maxAccounts int,
) (*Report, error) {
	report := &Report{}

	// Fetch all closed epoch summaries from Dingo.
	var summaries []models.EpochSummary
	if err := c.db.
		Where("snapshot_ready = ?", true).
		Order("epoch asc").
		Find(&summaries).Error; err != nil {
		return nil, fmt.Errorf("query epoch summaries: %w", err)
	}

	// Sample account addresses once; reuse across the full check.
	var sampleAccounts []models.Account
	if maxAccounts > 0 {
		if err := c.db.
			Where("active = ?", true).
			Order("staking_key asc").
			Limit(maxAccounts).
			Find(&sampleAccounts).Error; err != nil {
			return nil, fmt.Errorf("query sample accounts: %w", err)
		}
	}

	// Run epoch-level and pool-level checks for every closed epoch.
	for _, summary := range summaries {
		ms, poolsChecked, err := c.checkEpoch(ctx, summary, maxPoolsPerEpoch)
		if err != nil {
			return nil, fmt.Errorf("epoch %d: %w", summary.Epoch, err)
		}
		report.EpochsChecked++
		report.PoolsChecked += poolsChecked
		report.Mismatches = append(report.Mismatches, ms...)
	}

	// Run account-level checks (current withdrawable balance only; per-epoch
	// reward history requires #1875 before it can be compared).
	if len(sampleAccounts) > 0 {
		ms, err := c.checkAccounts(ctx, sampleAccounts)
		if err != nil {
			return nil, fmt.Errorf("check accounts: %w", err)
		}
		report.AccountsChecked = len(sampleAccounts)
		report.Mismatches = append(report.Mismatches, ms...)
	}

	return report, nil
}

// checkEpoch compares EpochSummary aggregate fields and a sample of
// RewardPoolInputs against Koios for a single closed epoch.
func (c *Checker) checkEpoch(
	ctx context.Context,
	summary models.EpochSummary,
	maxPools int,
) (mismatches []Mismatch, poolsChecked int, _ error) {
	epoch := summary.Epoch

	// Courtesy delay between epochs.
	time.Sleep(300 * time.Millisecond)

	info, err := c.koios.GetEpochInfo(ctx, epoch)
	if err != nil {
		return nil, 0, fmt.Errorf("GetEpochInfo: %w", err)
	}
	if info == nil {
		// Koios does not have data for this epoch yet; skip silently.
		return nil, 0, nil
	}

	// active_stake
	if info.ActiveStake != nil {
		kStake := *info.ActiveStake
		dStake := strconv.FormatUint(uint64(summary.TotalActiveStake), 10)
		if kStake != dStake {
			mismatches = append(mismatches, Mismatch{
				Epoch: epoch,
				Field: "total_active_stake",
				Dingo: dStake,
				Koios: kStake,
			})
		}
	}

	// pool_cnt
	if info.PoolCnt != nil {
		kCnt := strconv.FormatInt(*info.PoolCnt, 10)
		dCnt := strconv.FormatUint(summary.TotalPoolCount, 10)
		if kCnt != dCnt {
			mismatches = append(mismatches, Mismatch{
				Epoch: epoch,
				Field: "pool_count",
				Dingo: dCnt,
				Koios: kCnt,
			})
		}
	}

	// delegator_cnt
	if info.DelegatorCnt != nil {
		kCnt := strconv.FormatInt(*info.DelegatorCnt, 10)
		dCnt := strconv.FormatUint(summary.TotalDelegators, 10)
		if kCnt != dCnt {
			mismatches = append(mismatches, Mismatch{
				Epoch: epoch,
				Field: "delegator_count",
				Dingo: dCnt,
				Koios: kCnt,
			})
		}
	}

	// Pool-level checks.
	poolMs, poolsChecked, err := c.checkEpochPools(ctx, epoch, maxPools)
	if err != nil {
		return nil, 0, err
	}
	mismatches = append(mismatches, poolMs...)

	return mismatches, poolsChecked, nil
}

// checkEpochPools compares up to maxPools RewardPoolInputs for epoch against
// Koios /pool_history.
func (c *Checker) checkEpochPools(
	ctx context.Context,
	epoch uint64,
	maxPools int,
) ([]Mismatch, int, error) {
	if maxPools <= 0 {
		return nil, 0, nil
	}

	var inputs []models.RewardPoolInput
	if err := c.db.
		Where("epoch = ?", epoch).
		Order("pool_key_hash asc").
		Limit(maxPools).
		Find(&inputs).Error; err != nil {
		return nil, 0, fmt.Errorf("query reward pool inputs: %w", err)
	}

	var mismatches []Mismatch
	for _, input := range inputs {
		poolID := lcommon.PoolId(lcommon.NewBlake2b224(input.PoolKeyHash)).String()

		// Courtesy delay per pool request.
		time.Sleep(150 * time.Millisecond)

		entry, err := c.koios.GetPoolHistory(ctx, poolID, epoch)
		if err != nil {
			return nil, 0, fmt.Errorf("GetPoolHistory %s epoch %d: %w", poolID, epoch, err)
		}
		if entry == nil {
			continue // pool not present in Koios for this epoch
		}

		// delegated_stake / active_stake (lovelace)
		dStake := strconv.FormatUint(uint64(input.DelegatedStake), 10)
		if entry.ActiveStake != dStake {
			mismatches = append(mismatches, Mismatch{
				Epoch: epoch,
				Pool:  poolID,
				Field: "delegated_stake",
				Dingo: dStake,
				Koios: entry.ActiveStake,
			})
		}

		// delegator_count
		if entry.Delegators != nil {
			kDel := strconv.FormatUint(*entry.Delegators, 10)
			dDel := strconv.FormatUint(input.DelegatorCount, 10)
			if kDel != dDel {
				mismatches = append(mismatches, Mismatch{
					Epoch: epoch,
					Pool:  poolID,
					Field: "delegator_count",
					Dingo: dDel,
					Koios: kDel,
				})
			}
		}

		// blocks_produced
		if input.BlocksProduced != nil && entry.BlockCnt != nil {
			kBlk := strconv.FormatUint(*entry.BlockCnt, 10)
			dBlk := strconv.FormatUint(*input.BlocksProduced, 10)
			if kBlk != dBlk {
				mismatches = append(mismatches, Mismatch{
					Epoch: epoch,
					Pool:  poolID,
					Field: "blocks_produced",
					Dingo: dBlk,
					Koios: kBlk,
				})
			}
		}
	}

	return mismatches, len(inputs), nil
}

// checkAccounts compares the current withdrawable reward balance (Account.Reward)
// for each sampled account against Koios /account_info.
//
// Per-epoch reward history is not yet persisted by Dingo (#1875) and therefore
// cannot be compared here. Only the aggregate withdrawable balance is checked.
func (c *Checker) checkAccounts(
	ctx context.Context,
	accounts []models.Account,
) ([]Mismatch, error) {
	// Build stake address strings; skip accounts whose keys cannot be encoded.
	type entry struct {
		addr  string
		model models.Account
	}
	entries := make([]entry, 0, len(accounts))
	for _, acc := range accounts {
		addr, err := stakeAddressBech32(c.network, acc.CredentialTag, acc.StakingKey)
		if err != nil {
			continue
		}
		entries = append(entries, entry{addr: addr, model: acc})
	}
	if len(entries) == 0 {
		return nil, nil
	}

	// Koios /account_info accepts up to 50 addresses per request.
	const batchSize = 50
	var mismatches []Mismatch

	for i := 0; i < len(entries); i += batchSize {
		batch := entries[i:min(i+batchSize, len(entries))]

		addrs := make([]string, len(batch))
		for j, e := range batch {
			addrs[j] = e.addr
		}

		// Courtesy delay between batch requests.
		time.Sleep(300 * time.Millisecond)

		infos, err := c.koios.GetAccountInfo(ctx, addrs)
		if err != nil {
			return nil, fmt.Errorf("GetAccountInfo batch %d: %w", i/batchSize, err)
		}

		byAddr := make(map[string]AccountInfo, len(infos))
		for _, info := range infos {
			byAddr[info.StakeAddress] = info
		}

		for _, e := range batch {
			info, ok := byAddr[e.addr]
			if !ok || info.Status != "registered" {
				// Account unknown to Koios or already deregistered; skip.
				continue
			}

			dReward := strconv.FormatUint(uint64(e.model.Reward), 10)
			if info.RewardsAvailable != dReward {
				mismatches = append(mismatches, Mismatch{
					Account: e.addr,
					Field:   "reward_balance",
					Dingo:   dReward,
					Koios:   info.RewardsAvailable,
				})
			}
		}
	}

	return mismatches, nil
}

// stakeAddressBech32 encodes a staking key as a bech32 reward address.
//
// credentialTag 0 = key hash, 1 = script hash. The header byte and HRP are
// chosen per the Cardano address spec:
//
//	key:    mainnet 0xe0 / testnet 0xe1, HRP "stake" / "stake_test"
//	script: mainnet 0xf0 / testnet 0xf1, HRP "stake"  / "stake_test"
func stakeAddressBech32(network string, credentialTag uint8, stakingKey []byte) (string, error) {
	if len(stakingKey) != 28 {
		return "", fmt.Errorf("staking key must be 28 bytes, got %d", len(stakingKey))
	}

	isMainnet := network == "mainnet"

	var header byte
	var hrp string
	switch {
	case isMainnet && credentialTag == 0:
		header, hrp = 0xe0, "stake"
	case isMainnet && credentialTag == 1:
		header, hrp = 0xf0, "stake"
	case !isMainnet && credentialTag == 0:
		header, hrp = 0xe1, "stake_test"
	default: // !isMainnet && credentialTag == 1
		header, hrp = 0xf1, "stake_test"
	}

	addrBytes := make([]byte, 29)
	addrBytes[0] = header
	copy(addrBytes[1:], stakingKey)

	conv, err := bech32.ConvertBits(addrBytes, 8, 5, true)
	if err != nil {
		return "", fmt.Errorf("bech32 convert bits: %w", err)
	}
	return bech32.Encode(hrp, conv)
}
