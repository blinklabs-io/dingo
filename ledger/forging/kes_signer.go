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

// KESSigner is the block-forging seam for KES signing and the operational
// certificate. The forger and block builder depend on this interface instead
// of a concrete *PoolCredentials, so the KES signing key can come from either
// a local key file (the default *PoolCredentials implementation) or an
// external bursa KES agent.
//
// All periods are ABSOLUTE Cardano KES periods (slot / slotsPerKESPeriod);
// implementations translate to the relative period within the operational
// certificate window internally. *PoolCredentials satisfies this interface
// with no changes.
type KESSigner interface {
	// KESSign signs message with the KES key at the given absolute KES
	// period. Callers evolve the key first via UpdateKESPeriod.
	KESSign(period uint64, message []byte) ([]byte, error)
	// UpdateKESPeriod evolves the KES key forward to the given absolute KES
	// period. It never evolves backward.
	UpdateKESPeriod(period uint64) error
	// GetOpCert returns a copy of the operational certificate placed in the
	// block header, or nil if none is loaded.
	GetOpCert() *OpCert
	// OpCertExpiryPeriod returns the absolute KES period at which the
	// operational certificate expires.
	OpCertExpiryPeriod() uint64
	// PeriodsRemaining returns how many KES periods remain before the
	// operational certificate expires at the given current absolute period.
	PeriodsRemaining(currentPeriod uint64) uint64
}

// Compile-time assertion that the file-based credentials satisfy the seam.
var _ KESSigner = (*PoolCredentials)(nil)
