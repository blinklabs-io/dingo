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
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
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

		p.runBatch(ctx, client, batchSize)
		client.Close() //nolint:errcheck // best-effort close

		if !p.cooldown(ctx) {
			return ctx.Err()
		}
	}
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
) {
	slot := p.currentSlot()
	epoch := p.epochFromSlot(slot)
	active := enabledTypes(p.cfg.Types, epoch, p.cfg.delegationEnabled())
	if len(active) == 0 {
		return
	}

	for i := 0; i < batchSize; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		txType := active[IntRange(0, len(active)-1)]
		switch txType {
		case "payment":
			p.submitPayment(client, batchSize)
		case "delegation":
			p.submitDelegation(client, batchSize)
		case "governance":
			p.submitGovernance(client, batchSize)
		case "plutus":
			p.submitPlutus(client, batchSize)
		default:
			p.logger.Warn(
				"unknown tx type in config, skipping",
				"type", txType,
			)
		}
	}
}

// submitPayment builds and submits a single payment transaction.
func (p *Pump) submitPayment(client *NodeClient, batchSize int) {
	// Determine a send amount between minSendAmount and half the wallet balance
	// (leaving room for fees and change).  Fall back to minSendAmount when the
	// balance is very small.
	balance := p.wallet.Balance()
	maxSend := balance / 2
	if maxSend < minSendAmount+MinFee {
		p.logger.Warn(
			"wallet balance too low for payment, skipping",
			"balance_lovelace", balance,
		)
		return
	}

	upper := maxSend - MinFee
	if upper < minSendAmount {
		upper = minSendAmount
	}
	sendAmount := uint64(IntRange(int(minSendAmount), int(upper))) //nolint:gosec // IntRange always returns non-negative

	required := sendAmount + MinFee
	inputs, change, err := p.wallet.SelectCoins(required)
	if err != nil {
		p.logger.Warn(
			"coin selection failed",
			"required_lovelace", required,
			"err", err,
		)
		return
	}

	// Use the first input's address as a stand-in for both recipient and
	// change.  In a real scenario these would come from key derivation.
	// For Antithesis testing the address just needs to be syntactically valid.
	addr := deterministicAddr(inputs[0].TxHash)

	params := PaymentParams{
		Inputs:     inputs,
		ToAddr:     addr,
		ChangeAddr: addr,
		SendAmount: sendAmount,
		Change:     change,
	}

	txBytes, err := BuildPayment(params)
	if err != nil {
		p.logger.Error("build payment failed", "err", err)
		p.wallet.ReturnUTxOs(inputs)
		return
	}

	txID := deriveTestTxID(txBytes)

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
		// Return inputs so future batches can reuse them.
		p.wallet.ReturnUTxOs(inputs)
	} else {
		entry.Status = "submitted"
		p.logger.Info(
			"tx submitted",
			"tx_id", txID,
			"send_lovelace", sendAmount,
		)
		// Return change output to wallet so future transactions can spend it.
		if change > 0 {
			p.wallet.Add(UTxO{TxHash: txID, Index: 1, Amount: change})
		}
	}

	if p.txlog != nil {
		if logErr := p.txlog.Log(entry); logErr != nil {
			p.logger.Error("txlog write failed", "err", logErr)
		}
	}
}

// submitDelegation builds and submits a single stake-delegation transaction.
func (p *Pump) submitDelegation(client *NodeClient, batchSize int) {
	if !p.cfg.delegationEnabled() {
		return
	}
	stakeKeyHash, err := decodeConfiguredHash(
		"TXPUMP_DELEGATION_STAKE_KEY_HASH",
		p.cfg.DelegationStakeKeyHash,
		28,
	)
	if err != nil {
		p.logger.Error("invalid delegation stake key hash", "err", err)
		return
	}
	poolKeyHash, err := decodeConfiguredHash(
		"TXPUMP_DELEGATION_POOL_KEY_HASH",
		p.cfg.DelegationPoolKeyHash,
		28,
	)
	if err != nil {
		p.logger.Error("invalid delegation pool key hash", "err", err)
		return
	}

	required := MinFee
	inputs, change, err := p.wallet.SelectCoins(required)
	if err != nil {
		p.logger.Warn(
			"coin selection failed for delegation",
			"required_lovelace", required,
			"err", err,
		)
		return
	}

	changeAddr := deterministicAddr(inputs[0].TxHash)

	txBytes, err := BuildDelegationTx(inputs, stakeKeyHash, poolKeyHash, MinFee, changeAddr)
	if err != nil {
		p.logger.Error("build delegation failed", "err", err)
		p.wallet.ReturnUTxOs(inputs)
		return
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
			p.wallet.Add(UTxO{TxHash: txID, Index: 0, Amount: change})
		}
	}
	if p.txlog != nil {
		if logErr := p.txlog.Log(entry); logErr != nil {
			p.logger.Error("txlog write failed", "err", logErr)
		}
	}
}

// submitGovernance builds and submits either a DRep registration or a vote
// transaction (chosen randomly).
func (p *Pump) submitGovernance(client *NodeClient, batchSize int) {
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
		return
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
		p.logger.Error("build governance tx failed", "kind", txKind, "err", buildErr)
		p.wallet.ReturnUTxOs(inputs)
		return
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
			p.wallet.Add(UTxO{TxHash: txID, Index: 0, Amount: change})
		}
	}
	if p.txlog != nil {
		if logErr := p.txlog.Log(entry); logErr != nil {
			p.logger.Error("txlog write failed", "err", logErr)
		}
	}
}

// submitPlutus builds and submits either a Plutus lock or unlock transaction
// (chosen randomly).
func (p *Pump) submitPlutus(client *NodeClient, batchSize int) {
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
			return
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
			return
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
		p.logger.Error("build plutus tx failed", "kind", txKind, "err", buildErr)
		if txKind == "plutus_lock" {
			p.wallet.ReturnUTxOs(inputs)
		} else {
			p.addLockedPlutusUTxO(lockedInput)
		}
		return
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
			p.addLockedPlutusUTxO(UTxO{
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
			p.wallet.Add(UTxO{TxHash: txID, Index: changeIdx, Amount: change})
		}
	}
	if p.txlog != nil {
		if logErr := p.txlog.Log(entry); logErr != nil {
			p.logger.Error("txlog write failed", "err", logErr)
		}
	}
}

func (p *Pump) addLockedPlutusUTxO(utxo UTxO) {
	p.plutusLocked = append(p.plutusLocked, utxo)
}

func (p *Pump) takeLockedPlutusUTxO() (UTxO, bool) {
	if len(p.plutusLocked) == 0 {
		return UTxO{}, false
	}
	last := len(p.plutusLocked) - 1
	utxo := p.plutusLocked[last]
	p.plutusLocked = p.plutusLocked[:last]
	return utxo, true
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
