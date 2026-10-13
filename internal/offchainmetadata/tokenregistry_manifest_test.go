package offchainmetadata

import (
	"crypto/ed25519"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVerifyTokenRegistryManifestFields(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*tokenRegistryManifest)
		want   string
	}{
		{"version", func(m *tokenRegistryManifest) { m.Version++ }, "version"},
		{"sequence", func(m *tokenRegistryManifest) { m.Sequence = 0 }, "sequence"},
		{"source binding", func(m *tokenRegistryManifest) { m.ArchiveURL = "https://other.example/archive" }, "archive URL"},
		{"size", func(m *tokenRegistryManifest) { m.ArchiveBytes = -1 }, "size"},
		{"digest encoding", func(m *tokenRegistryManifest) { m.ArchiveDigest = "zz" }, "digest"},
		{"digest length", func(m *tokenRegistryManifest) { m.ArchiveDigest = "00" }, "digest"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			manifest := tokenRegistryManifest{Version: tokenRegistryManifestVersion, Sequence: 1, ArchiveURL: "https://registry.example/archive", ArchiveBytes: 0, ArchiveDigest: strings.Repeat("00", 32)}
			tc.mutate(&manifest)
			_, err := verifyTokenRegistryManifest(signedTokenRegistryManifestValue(t, manifest), testTokenRegistryManifestPrivateKey.Public().(ed25519.PublicKey), "https://registry.example/archive")
			require.ErrorContains(t, err, tc.want)
		})
	}
}
