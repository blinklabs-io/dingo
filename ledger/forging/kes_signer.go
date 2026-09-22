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

package forging

// RemoteKESSigner is satisfied by a KES agent client operating in "sign"
// mode: PoolCredentials holds no local KES secret at all and delegates every
// signing operation to it instead of evolving and signing with a local
// *kes.SecretKey.
//
// It is a narrow seam deliberately: PoolCredentials/credentialGeneration
// still own the operational-certificate lifetime (validateKESPeriod,
// opCertStartKES/opCertExpiryKES) and the monotonic period-progression check
// (updateKESPeriod) for both the local-key and agent-backed paths alike.
// RemoteKESSigner.Sign is only ever reached from credentialGeneration.kesSign,
// which validates the period against that lifetime before calling it --
// including a defense-in-depth re-check inside kesSign itself, so a signer
// implementation can never be handed a period the operational certificate has
// not authorized, regardless of what its caller already checked.
type RemoteKESSigner interface {
	// Sign returns the KES signature for message at the given ABSOLUTE KES
	// period. The implementation is responsible for translating to whatever
	// period convention its own transport uses (a bursa KES agent's sign-mode
	// wire protocol also takes an absolute period, so a direct client
	// implementation needs no translation at all).
	Sign(period uint64, message []byte) ([]byte, error)
}

// AgentKESMaterial is validated serve-key material a KES agent client
// delivers: the raw KES secret key bytes at AbsolutePeriod, its verification
// key, and the operational certificate the agent served alongside it.
//
// ledger/forging depends on this shape rather than on a KES agent package's
// own wire types, so the transport (kesagent.Client, or any future one) can
// evolve without this credential model changing to match it.
type AgentKESMaterial struct {
	// AbsolutePeriod is the KES period the pushed secret key is already
	// evolved to.
	AbsolutePeriod uint64
	// KESSKeyData is the raw KES secret key bytes (608 bytes at Cardano's
	// depth 6) at AbsolutePeriod.
	KESSKeyData []byte
	// KESVKey is the 32-byte KES verification key matching KESSKeyData.
	KESVKey []byte
	// OpCert is the operational certificate the agent served alongside the
	// key. Its KESVKey must match KESVKey.
	OpCert OpCert
}
