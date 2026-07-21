//go:build devnet

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

package devnet

import (
	"encoding/hex"
	"fmt"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	olsq "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

// StakeReward is the delegation and reward-balance view of one stake
// credential, as read over LocalStateQuery.
type StakeReward struct {
	PoolID    []byte // delegated pool key hash, empty if not delegated
	Reward    uint64 // reward account balance in lovelace
	Delegated bool
}

// RewardAccountsByNtc opens a node-to-client TCP connection to addr
// (host:port), acquires the volatile tip, and queries the delegation
// target and reward-account balance for each supplied stake credential
// via GetFilteredDelegationsAndRewardAccounts.
func RewardAccountsByNtc(
	addr string,
	magic uint32,
	creds []olsq.StakeCredential,
) (map[olsq.StakeCredential]StakeReward, error) {
	conn, err := ouroboros.New(
		ouroboros.WithNetworkMagic(magic),
		ouroboros.WithNodeToNode(false),
	)
	if err != nil {
		return nil, fmt.Errorf("ouroboros.New: %w", err)
	}
	defer conn.Close() //nolint:errcheck

	if err := conn.DialTimeout("tcp", addr, 10*time.Second); err != nil {
		return nil, fmt.Errorf("dial tcp %s: %w", addr, err)
	}

	lsq := conn.LocalStateQuery()
	if lsq == nil || lsq.Client == nil {
		return nil, fmt.Errorf("LocalStateQuery client unavailable on %s", addr)
	}
	if err := lsq.Client.AcquireVolatileTip(); err != nil {
		return nil, fmt.Errorf("acquire tip on %s: %w", addr, err)
	}
	defer lsq.Client.Release() //nolint:errcheck

	res, err := lsq.Client.GetFilteredDelegationsAndRewardAccounts(creds)
	if err != nil {
		return nil, fmt.Errorf("reward accounts query on %s: %w", addr, err)
	}

	out := make(map[olsq.StakeCredential]StakeReward, len(creds))
	for _, c := range creds {
		sr := StakeReward{Reward: res.Rewards[c]}
		if pool, ok := res.Delegations[c]; ok {
			sr.PoolID = pool.Bytes()
			sr.Delegated = len(sr.PoolID) > 0
		}
		out[c] = sr
	}
	return out, nil
}

// RewardResult pairs a stake credential with its reward view for callers
// that should not import the gouroboros localstatequery package directly.
type RewardResult struct {
	Reward    uint64
	Delegated bool
	PoolID    []byte
}

// RewardAccountsByNtcForCreds is a scenario-friendly wrapper keyed by the
// hex-encoded credential bytes, so callers need not import gouroboros types.
func RewardAccountsByNtcForCreds(
	addr string,
	magic uint32,
	creds []olsq.StakeCredential,
) (map[string]RewardResult, error) {
	raw, err := RewardAccountsByNtc(addr, magic, creds)
	if err != nil {
		return nil, err
	}
	out := make(map[string]RewardResult, len(raw))
	for c, sr := range raw {
		out[hex.EncodeToString(c.Bytes.Bytes())] = RewardResult{
			Reward: sr.Reward, Delegated: sr.Delegated, PoolID: sr.PoolID,
		}
	}
	return out, nil
}
