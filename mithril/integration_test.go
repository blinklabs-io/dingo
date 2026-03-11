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

package mithril

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestIntegrationVerifyCertificateChainPreview connects to the real
// Mithril preview aggregator and verifies the latest snapshot's
// certificate chain with full STM signature verification.
//
// Skipped unless MITHRIL_INTEGRATION=1 is set.
func TestIntegrationVerifyCertificateChainPreview(t *testing.T) {
	if os.Getenv("MITHRIL_INTEGRATION") == "" {
		t.Skip("set MITHRIL_INTEGRATION=1 to run")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		2*time.Minute,
	)
	defer cancel()

	netCfg, err := NetworkConfigForNetwork("preview")
	require.NoError(t, err)

	client := NewClient(netCfg.AggregatorURL)

	// List snapshots and pick the latest one.
	snapshots, err := client.ListSnapshots(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, snapshots, "no snapshots available")

	latest := snapshots[0]
	t.Logf(
		"verifying certificate chain for snapshot %s (epoch %d)",
		latest.Digest, latest.Beacon.Epoch,
	)

	// Structural verification — chain linkage + digest binding.
	result, err := VerifyCertificateChainWithMode(
		ctx,
		client,
		latest.CertificateHash,
		latest.Digest,
		VerificationModeStructural,
	)
	require.NoError(t, err, "structural verification failed")
	require.NotNil(t, result.LeafCertificate)
	require.NotNil(t, result.GenesisCertificate)
	t.Logf(
		"structural verification passed: %d certificates in chain",
		len(result.Certificates),
	)

	// Full STM signature verification.
	result, err = VerifyCertificateChainWithMode(
		ctx,
		client,
		latest.CertificateHash,
		latest.Digest,
		VerificationModeSTM,
	)
	require.NoError(t, err, "STM verification failed")
	t.Logf(
		"STM verification passed: %d certificates verified",
		len(result.Certificates),
	)
}
