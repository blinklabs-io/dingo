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

package txpump

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// Pump orchestrates the main transaction-generation loop.
type Pump struct {
	cfg          *Config
	wallet       *Wallet
	logger       *slog.Logger
	txlog        *TxLogger
	genesisTime  time.Time
	plutusLocked []UTxO
}

// NewPump creates a new Pump from the provided Config.
func NewPump(
	cfg *Config,
	wallet *Wallet,
	logger *slog.Logger,
	txlog *TxLogger,
	genesisTime time.Time,
) *Pump {
	return &Pump{
		cfg:         cfg,
		wallet:      wallet,
		logger:      logger,
		txlog:       txlog,
		genesisTime: genesisTime,
	}
}

// Run executes the transaction-pump loop until ctx is cancelled.
//
// On each iteration it:
//  1. Picks a random batch size in [TxCountMin, TxCountMax].
//  2. Connects to the primary node (falling back to the secondary on failure).
//  3. Submits the batch, one transaction at a time.
//  4. Waits a random cooldown in [CooldownMin, CooldownMax] milliseconds.
func (p *Pump) Run(ctx context.Context) error {
	var startupTimer *time.Timer
	var startup <-chan time.Time
	if p.cfg.StartupTimeout > 0 {
		startupTimer = time.NewTimer(p.cfg.StartupTimeout)
		defer startupTimer.Stop()
		startup = startupTimer.C
	}
	ready := false
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-startup:
			return fmt.Errorf(
				"txpump readiness timeout after %s: no transaction was successfully submitted",
				p.cfg.StartupTimeout,
			)
		default:
		}

		batchSize := IntRange(p.cfg.TxCountMin, p.cfg.TxCountMax)

		client, err := p.dialPrimary()
		if err != nil {
			p.logger.Error(
				"failed to connect to primary node, trying fallback",
				"primary", p.cfg.NodeAddr,
				"err", err,
			)
			client, err = p.dialFallback()
			if err != nil {
				p.logger.Error(
					"fallback connection also failed, skipping batch",
					"fallback", p.cfg.FallbackAddr,
					"err", err,
				)
				if !p.cooldown(ctx) {
					return ctx.Err()
				}
				continue
			}
		}

		submitted := p.runBatch(ctx, client, batchSize)
		client.Close() //nolint:errcheck // best-effort close
		if submitted > 0 && !ready {
			ready = true
			stopStartupTimeout(&startupTimer, &startup)
			p.logger.Info(
				"txpump ready",
				"workload_types", p.cfg.Types,
				"submitted", submitted,
			)
			if p.txlog != nil {
				if logErr := p.txlog.Log(TxLog{
					Event:         "ready",
					Status:        "ready",
					NodeAddr:      client.Addr(),
					WorkloadTypes: append([]string(nil), p.cfg.Types...),
				}); logErr != nil {
					p.logger.Error(
						"txlog readiness write failed",
						"err",
						logErr,
					)
				}
			}
		}

		if !p.cooldown(ctx) {
			return ctx.Err()
		}
	}
}

// stopStartupTimeout disables the readiness deadline after the first
// successful submission. The deadline only protects the pre-ready phase.
func stopStartupTimeout(timer **time.Timer, deadline *<-chan time.Time) {
	if *timer != nil {
		(*timer).Stop()
		*timer = nil
	}
	*deadline = nil
}

// dialPrimary connects to the primary node address.
func (p *Pump) dialPrimary() (*NodeClient, error) {
	return NewNodeClient(p.cfg.NodeAddr, p.cfg.NetworkMagic, p.logger)
}

// dialFallback connects to the fallback node address. Returns an error if no
// fallback is configured.
func (p *Pump) dialFallback() (*NodeClient, error) {
	if p.cfg.FallbackAddr == "" {
		return nil, errors.New("no fallback node address configured")
	}
	return NewNodeClient(p.cfg.FallbackAddr, p.cfg.NetworkMagic, p.logger)
}

// epochFromSlot returns the epoch number for a given slot.
func (p *Pump) epochFromSlot(slot uint64) uint64 {
	el := p.cfg.EpochLength
	if el == 0 {
		el = 500
	}
	return slot / el
}

// enabledTypes returns the subset of types that are permitted at the given
// epoch.  Types are gated to avoid submitting transactions that the node
// cannot process until the relevant era/rules are active.
//
//   - payment:    always enabled
//   - delegation: enabled from epoch 1 when delegation credentials are configured
//   - governance: enabled from epoch 2
//   - plutus:     enabled from epoch 3
func enabledTypes(
	types []string,
	epoch uint64,
	delegationEnabled bool,
) []string {
	var enabled []string
	for _, t := range types {
		switch t {
		case "payment":
			enabled = append(enabled, t)
		case "delegation":
			if epoch >= 1 && delegationEnabled {
				enabled = append(enabled, t)
			}
		case "governance":
			if epoch >= 2 {
				enabled = append(enabled, t)
			}
		case "plutus":
			if epoch >= 3 {
				enabled = append(enabled, t)
			}
		}
	}
	return enabled
}

// currentSlot returns the number of 1-second slots elapsed since the Pump was
// created.  Using elapsed time (rather than Unix epoch seconds) ensures that
// the slot counter starts near 0 and epoch gating works correctly for devnet
// testing, where epochs are only 500 slots long.
func (p *Pump) currentSlot() uint64 {
	elapsed := time.Since(p.genesisTime)
	return uint64(elapsed.Seconds())
}

// runBatch submits batchSize transactions, selecting a random type for each.
func (p *Pump) runBatch(
	ctx context.Context,
	client *NodeClient,
	batchSize int,
) int {
	slot := p.currentSlot()
	epoch := p.epochFromSlot(slot)
	active := enabledTypes(p.cfg.Types, epoch, p.cfg.delegationEnabled())
	if len(active) == 0 {
		return 0
	}

	submitted := 0
	for i := 0; i < batchSize; i++ {
		select {
		case <-ctx.Done():
			return submitted
		default:
		}

		txType := active[IntRange(0, len(active)-1)]
		switch txType {
		case "payment":
			if p.submitPayment(client, batchSize) {
				submitted++
			}
		case "delegation":
			if p.submitDelegation(client, batchSize) {
				submitted++
			}
		case "governance":
			if p.submitGovernance(client, batchSize) {
				submitted++
			}
		case "plutus":
			if p.submitPlutus(client, batchSize) {
				submitted++
			}
		default:
			p.logger.Warn(
				"unknown tx type in config, skipping",
				"type", txType,
			)
		}
	}
	return submitted
}

// submitPayment builds and submits a single payment transaction.
func (p *Pump) submitPayment(client *NodeClient, batchSize int) bool {
	// Determine a send amount between minSendAmount and half the wallet balance
	// (leaving room for fees and change).  Fall back to minSendAmount when the
	// balance is very small.
	balance := p.wallet.Balance()
	maxSend := balance / 2
	if maxSend < minSendAmount+MinFee {
		p.logger.Debug(
			"wallet balance too low for payment, skipping",
			"balance_lovelace", balance,
		)
		return false
	}

	upper := maxSend - MinFee
	if upper < minSendAmount {
		upper = minSendAmount
	}
	sendAmount := uint64(
		IntRange(int(minSendAmount), int(upper)),
	) //nolint:gosec // IntRange always returns non-negative

	required := sendAmount + MinFee
	inputs, change, err := p.wallet.SelectCoins(required)
	if err != nil {
		p.logger.Warn(
			"coin selection failed",
			"required_lovelace", required,
			"err", err,
		)
		return false
	}

	// Collect a witness key for every distinct signing key among the inputs.
	// SelectCoins may return UTxOs from different genesis keys; each one needs
	// its own VKey witness or the transaction will be rejected.
	var signingKey *UTxOKey
	witnessKeys := make([]*UTxOKey, 0, len(inputs))
	seenWitnessKeys := make(map[*UTxOKey]struct{}, len(inputs))
	for _, u := range inputs {
		if u.SigningKey != nil {
			if signingKey == nil {
				signingKey = u.SigningKey
			}
			if _, ok := seenWitnessKeys[u.SigningKey]; !ok {
				witnessKeys = append(witnessKeys, u.SigningKey)
				seenWitnessKeys[u.SigningKey] = struct{}{}
			}
		}
	}
	addr := deterministicAddr(inputs[0].TxHash)
	if signingKey != nil {
		addr = signingKey.Address
	}

	params := PaymentParams{
		Inputs:      inputs,
		ToAddr:      addr,
		ChangeAddr:  addr,
		SendAmount:  sendAmount,
		Change:      change,
		WitnessKeys: witnessKeys,
	}

	txBytes, txID, err := BuildPayment(params)
	if err != nil {
		p.logger.Error("build payment failed", "err", err)
		p.wallet.ReturnUTxOs(inputs)
		return false
	}

	submitErr := client.SubmitTx(conwayEraID, txBytes)
	entry := TxLog{
		TxID:      txID,
		TxType:    "payment",
		EraID:     conwayEraID,
		NodeAddr:  client.Addr(),
		BatchSize: batchSize,
	}
	if submitErr != nil {
		entry.Status = "rejected"
		entry.ErrorMsg = submitErr.Error()
		p.logger.Warn(
			"tx rejected",
			"tx_id", txID,
			"err", submitErr,
		)
		// Do not retry rejected inputs. A repeated retry loop can turn one
		// stale input into thousands of identical validation failures.
	} else {
		entry.Status = "submitted"
		p.logger.Info(
			"tx submitted",
			"tx_id", txID,
			"send_lovelace", sendAmount,
		)
		// Quarantine submitted outputs for the configured confirmation window
		// so an early fork cannot invalidate an immediate dependency chain.
		confirmationDelay := p.cfg.confirmationDelay()
		if signingKey != nil {
			p.wallet.AddAfter(confirmationDelay, UTxO{TxHash: txID, Index: 0, Amount: sendAmount, SigningKey: signingKey})
		}
		if change > 0 {
			changeUTxO := UTxO{TxHash: txID, Index: 1, Amount: change}
			if signingKey != nil {
				changeUTxO.SigningKey = signingKey
			}
			p.wallet.AddAfter(confirmationDelay, changeUTxO)
		}
	}

	if p.txlog != nil {
		if logErr := p.txlog.Log(entry); logErr != nil {
			p.logger.Error("txlog write failed", "err", logErr)
		}
	}
	return submitErr == nil
}

// submitDelegation builds and submits a single stake-delegation transaction.
func (p *Pump) submitDelegation(client *NodeClient, batchSize int) bool {
	if !p.cfg.delegationEnabled() {
		return false
	}
	stakeKeyHash, err := decodeConfiguredHash(
		"TXPUMP_DELEGATION_STAKE_KEY_HASH",
		p.cfg.DelegationStakeKeyHash,
		28,
	)
	if err != nil {
		p.logger.Error("invalid delegation stake key hash", "err", err)
		return false
	}
	poolKeyHash, err := decodeConfiguredHash(
		"TXPUMP_DELEGATION_POOL_KEY_HASH",
		p.cfg.DelegationPoolKeyHash,
		28,
	)
	if err != nil {
		p.logger.Error("invalid delegation pool key hash", "err", err)
		return false
	}

	required := MinFee
	inputs, change, err := p.wallet.SelectCoins(required)
	if err != nil {
		p.logger.Warn(
			"coin selection failed for delegation",
			"required_lovelace", required,
			"err", err,
		)
		return false
	}

	changeAddr := deterministicAddr(inputs[0].TxHash)

	txBytes, err := BuildDelegationTx(
		inputs,
		stakeKeyHash,
		poolKeyHash,
		MinFee,
		changeAddr,
	)
	if err != nil {
		p.logger.Error("build delegation failed", "err", err)
		p.wallet.ReturnUTxOs(inputs)
		return false
	}

	txID := deriveTestTxID(txBytes)
	submitErr := client.SubmitTx(conwayEraID, txBytes)
	entry := TxLog{
		TxID:      txID,
		TxType:    "delegation",
		EraID:     conwayEraID,
		NodeAddr:  client.Addr(),
		BatchSize: batchSize,
	}
	if submitErr != nil {
		entry.Status = "rejected"
		entry.ErrorMsg = submitErr.Error()
		p.logger.Warn("delegation tx rejected", "tx_id", txID, "err", submitErr)
		p.wallet.ReturnUTxOs(inputs)
	} else {
		entry.Status = "submitted"
		p.logger.Info("delegation tx submitted", "tx_id", txID)
		// Return the change output to the wallet so future transactions can
		// spend it.
		if change > 0 {
			p.wallet.AddAfter(p.cfg.confirmationDelay(), UTxO{TxHash: txID, Index: 0, Amount: change})
		}
	}
	if p.txlog != nil {
		if logErr := p.txlog.Log(entry); logErr != nil {
			p.logger.Error("txlog write failed", "err", logErr)
		}
	}
	return submitErr == nil
}

// submitGovernance builds and submits either a DRep registration or a vote
// transaction (chosen randomly).
func (p *Pump) submitGovernance(client *NodeClient, batchSize int) bool {
	// Decide the type first so we can compute the correct coin selection.
	// DRep registration needs fee + deposit; votes only need the fee.
	var txKind string
	if IntRange(0, 1) == 0 {
		txKind = "drep_reg"
	} else {
		txKind = "vote"
	}

	var required uint64
	if txKind == "drep_reg" {
		required = MinFee + 500_000_000 // registration deposit
	} else {
		required = MinFee
	}

	inputs, change, err := p.wallet.SelectCoins(required)
	if err != nil {
		p.logger.Warn(
			"coin selection failed for governance",
			"required_lovelace", required,
			"kind", txKind,
			"err", err,
		)
		return false
	}

	raw, _ := hex.DecodeString(inputs[0].TxHash)
	drepKeyHash := make([]byte, 28)
	copy(drepKeyHash, raw)
	changeAddr := deterministicAddr(inputs[0].TxHash)

	var txBytes []byte
	var buildErr error

	if txKind == "drep_reg" {
		txBytes, buildErr = BuildDRepRegistrationTx(
			inputs, drepKeyHash, 500_000_000, MinFee, changeAddr,
		)
	} else {
		govActionHash := make([]byte, 32)
		copy(govActionHash, raw)
		txBytes, buildErr = BuildVoteTx(
			inputs, drepKeyHash, govActionHash, 0, MinFee, changeAddr,
		)
	}

	if buildErr != nil {
		p.logger.Error(
			"build governance tx failed",
			"kind",
			txKind,
			"err",
			buildErr,
		)
		p.wallet.ReturnUTxOs(inputs)
		return false
	}

	txID := deriveTestTxID(txBytes)
	submitErr := client.SubmitTx(conwayEraID, txBytes)
	entry := TxLog{
		TxID:      txID,
		TxType:    "governance",
		EraID:     conwayEraID,
		NodeAddr:  client.Addr(),
		BatchSize: batchSize,
	}
	if submitErr != nil {
		entry.Status = "rejected"
		entry.ErrorMsg = submitErr.Error()
		p.logger.Warn(
			"governance tx rejected",
			"kind", txKind,
			"tx_id", txID,
			"err", submitErr,
		)
		p.wallet.ReturnUTxOs(inputs)
	} else {
		entry.Status = "submitted"
		p.logger.Info("governance tx submitted", "kind", txKind, "tx_id", txID)
		// Return the change output to the wallet so future transactions can
		// spend it.
		if change > 0 {
			p.wallet.AddAfter(p.cfg.confirmationDelay(), UTxO{TxHash: txID, Index: 0, Amount: change})
		}
	}
	if p.txlog != nil {
		if logErr := p.txlog.Log(entry); logErr != nil {
			p.logger.Error("txlog write failed", "err", logErr)
		}
	}
	return submitErr == nil
}

// submitPlutus builds and submits either a Plutus lock or unlock transaction
// (chosen randomly).
func (p *Pump) submitPlutus(client *NodeClient, batchSize int) bool {
	txKind := "plutus_lock"
	var lockedInput UTxO
	if len(p.plutusLocked) > 0 && IntRange(0, 1) == 1 {
		txKind = "plutus_unlock"
		var ok bool
		lockedInput, ok = p.takeLockedPlutusUTxO()
		if !ok {
			txKind = "plutus_lock"
		}
	}

	var (
		inputs []UTxO
		change uint64
		err    error
	)
	if txKind == "plutus_lock" {
		required := minSendAmount + MinFee
		inputs, change, err = p.wallet.SelectCoins(required)
		if err != nil {
			p.logger.Warn(
				"coin selection failed for plutus",
				"kind", txKind,
				"required_lovelace", required,
				"err", err,
			)
			return false
		}
	} else {
		inputs = []UTxO{lockedInput}
		if lockedInput.Amount < MinFee {
			p.logger.Error(
				"locked plutus UTxO cannot cover fee",
				"tx_hash", lockedInput.TxHash,
				"index", lockedInput.Index,
				"amount", lockedInput.Amount,
				"fee", MinFee,
			)
			p.addLockedPlutusUTxO(lockedInput)
			return false
		}
		change = lockedInput.Amount - MinFee
	}

	script := alwaysSucceedsScript()
	h := sha256.Sum256(script)
	scriptHash := h[:28]
	changeAddr := deterministicAddr(inputs[0].TxHash)

	var txBytes []byte
	var buildErr error

	if txKind == "plutus_lock" {
		txBytes, buildErr = BuildPlutusLockTx(
			inputs, scriptHash, minSendAmount, MinFee, changeAddr,
		)
	} else {
		txBytes, buildErr = BuildPlutusUnlockTx(
			inputs, alwaysSucceedsScript(), MinFee, changeAddr,
		)
	}

	if buildErr != nil {
		p.logger.Error(
			"build plutus tx failed",
			"kind",
			txKind,
			"err",
			buildErr,
		)
		if txKind == "plutus_lock" {
			p.wallet.ReturnUTxOs(inputs)
		} else {
			p.addLockedPlutusUTxO(lockedInput)
		}
		return false
	}

	txID := deriveTestTxID(txBytes)
	submitErr := client.SubmitTx(conwayEraID, txBytes)
	entry := TxLog{
		TxID:      txID,
		TxType:    "plutus",
		EraID:     conwayEraID,
		NodeAddr:  client.Addr(),
		BatchSize: batchSize,
	}
	if submitErr != nil {
		entry.Status = "rejected"
		entry.ErrorMsg = submitErr.Error()
		p.logger.Warn(
			"plutus tx rejected",
			"kind", txKind,
			"tx_id", txID,
			"err", submitErr,
		)
		if txKind == "plutus_lock" {
			p.wallet.ReturnUTxOs(inputs)
		} else {
			p.addLockedPlutusUTxO(lockedInput)
		}
	} else {
		entry.Status = "submitted"
		p.logger.Info("plutus tx submitted", "kind", txKind, "tx_id", txID)
		if txKind == "plutus_lock" {
			p.addLockedPlutusUTxOAfter(p.cfg.confirmationDelay(), UTxO{
				TxHash: txID,
				Index:  0,
				Amount: minSendAmount,
			})
		}
		// Return the change output to the wallet so future transactions can
		// spend it. For plutus_lock the script output is at index 0 and
		// change is at index 1. For plutus_unlock change is at index 0.
		if change > 0 {
			changeIdx := uint32(0)
			if txKind == "plutus_lock" {
				changeIdx = 1
			}
			p.wallet.AddAfter(p.cfg.confirmationDelay(), UTxO{TxHash: txID, Index: changeIdx, Amount: change})
		}
	}
	if p.txlog != nil {
		if logErr := p.txlog.Log(entry); logErr != nil {
			p.logger.Error("txlog write failed", "err", logErr)
		}
	}
	return submitErr == nil
}

func (p *Pump) addLockedPlutusUTxO(utxo UTxO) {
	p.plutusLocked = append(p.plutusLocked, utxo)
}

func (p *Pump) addLockedPlutusUTxOAfter(delay time.Duration, utxo UTxO) {
	if delay > 0 {
		utxo.availableAt = time.Now().Add(delay)
	}
	p.addLockedPlutusUTxO(utxo)
}

func (p *Pump) takeLockedPlutusUTxO() (UTxO, bool) {
	now := time.Now()
	for i := len(p.plutusLocked) - 1; i >= 0; i-- {
		utxo := p.plutusLocked[i]
		if !utxo.availableAt.IsZero() && utxo.availableAt.After(now) {
			continue
		}
		p.plutusLocked = append(
			p.plutusLocked[:i],
			p.plutusLocked[i+1:]...,
		)
		return utxo, true
	}
	return UTxO{}, false
}

// cooldown waits for a random duration in [CooldownMin, CooldownMax]ms.
// It returns true if the wait completed normally, false if ctx was cancelled.
func (p *Pump) cooldown(ctx context.Context) bool {
	ms := IntRange(p.cfg.CooldownMin, p.cfg.CooldownMax)
	timer := time.NewTimer(time.Duration(ms) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// deterministicAddr derives a 29-byte enterprise address from a tx hash string
// for use as a test recipient / change address.  The address is not a real
// key-derived address; it is sufficient for devnet transaction structure tests.
func deterministicAddr(txHash string) []byte {
	raw, _ := hex.DecodeString(txHash)
	addr := make([]byte, 29)
	addr[0] = 0x60 // enterprise address discriminant (devnet)
	copy(addr[1:], raw)
	return addr
}

// deriveTestTxID returns a 32-byte (64-char hex) identifier for the
// transaction derived from a SHA-256 hash of the CBOR payload.
func deriveTestTxID(txBytes []byte) string {
	h := sha256.Sum256(txBytes)
	return hex.EncodeToString(h[:])
}
