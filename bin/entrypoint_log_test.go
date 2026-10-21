//go:build linux

package bin_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func runEntrypointWithLogFile(
	t *testing.T,
	root string,
	logPath string,
) ([]byte, error) {
	t.Helper()
	fakeBin := filepath.Join(root, "bin")
	require.NoError(t, os.MkdirAll(fakeBin, 0o700))
	writeExecutable(
		t,
		filepath.Join(fakeBin, "dingo"),
		"#!/usr/bin/env bash\necho OUT stdout\necho ERR stderr >&2\n",
	)
	entrypointPath, err := filepath.Abs("entrypoint.sh")
	require.NoError(t, err)
	cmd := exec.Command("bash", entrypointPath, "serve") //nolint:gosec
	cmd.Env = append(
		cleanEnvironment(
			os.Environ(),
			"CARDANO_CONFIG",
			"CARDANO_DATABASE_PATH",
			"CARDANO_NETWORK",
			"DINGO_DEBUG",
			"DINGO_LOG_FILE",
			"DINGO_SOCKET_PATH",
			"PATH",
			"RESTORE_SNAPSHOT",
		),
		"PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"),
		"CARDANO_NETWORK=devnet",
		"CARDANO_DATABASE_PATH="+filepath.Join(root, "db"),
		"DINGO_SOCKET_PATH="+filepath.Join(root, "ipc", "dingo.socket"),
		"DINGO_LOG_FILE="+logPath,
	)
	return cmd.CombinedOutput()
}

func TestEntrypointLogFileCapturesStdoutAndStderr(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	logPath := filepath.Join(root, "logs", "dingo.log")

	out, err := runEntrypointWithLogFile(t, root, logPath)
	require.NoError(t, err, string(out))

	data, err := os.ReadFile(logPath)
	require.NoError(t, err)
	require.Contains(t, string(data), "OUT stdout")
	require.Contains(t, string(data), "ERR stderr")
	require.NotContains(t, string(out), "ERR stderr")
	info, err := os.Stat(logPath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o644), info.Mode().Perm())
}

func TestEntrypointLogFileRefusesExistingPath(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	existing := filepath.Join(root, "existing.log")
	require.NoError(t, os.WriteFile(existing, []byte("keep"), 0o600))
	link := filepath.Join(root, "link.log")
	require.NoError(t, os.Symlink(existing, link))
	dangling := filepath.Join(root, "dangling.log")
	require.NoError(t, os.Symlink(filepath.Join(root, "absent"), dangling))

	for _, path := range []string{existing, link, dangling} {
		out, err := runEntrypointWithLogFile(t, root, path)
		require.Error(t, err, string(out))
		require.Contains(t, string(out), "DINGO_LOG_FILE must name a new file")
	}
	data, err := os.ReadFile(existing)
	require.NoError(t, err)
	require.Equal(t, "keep", string(data))
	_, err = os.Lstat(filepath.Join(root, "absent"))
	require.True(t, os.IsNotExist(err))
}
