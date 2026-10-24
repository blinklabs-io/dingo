//go:build linux

package bin_test

import (
	"encoding/binary"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func runEntrypointWithLogFile(
	t *testing.T,
	root string,
	logPath string,
) ([]byte, error) {
	t.Helper()
	return runEntrypointWithLogFileRace(t, root, logPath, "")
}

// runEntrypointWithLogFileRace runs inject as another local actor would,
// immediately before the entrypoint's last external step ahead of the log
// open: after mkdir creates the log directory, and before perl starts.
func runEntrypointWithLogFileRace(
	t *testing.T,
	root string,
	logPath string,
	inject string,
) ([]byte, error) {
	t.Helper()
	fakeBin := filepath.Join(root, "bin")
	require.NoError(t, os.MkdirAll(fakeBin, 0o700))
	if inject != "" {
		realMkdir, err := exec.LookPath("mkdir")
		require.NoError(t, err)
		realPerl, err := exec.LookPath("perl")
		require.NoError(t, err)
		logDir := filepath.Dir(logPath)
		writeExecutable(
			t,
			filepath.Join(fakeBin, "mkdir"),
			"#!/usr/bin/env bash\n"+realMkdir+" \"$@\" || exit $?\n"+
				"if [[ \"${!#}\" == '"+logDir+"' ]]; then\n"+inject+"\nfi\n",
		)
		writeExecutable(
			t,
			filepath.Join(fakeBin, "perl"),
			"#!/usr/bin/env bash\n"+inject+"\nexec "+realPerl+" \"$@\"\n",
		)
	}
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

func TestEntrypointLogFileRefusesFifo(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	fifo := filepath.Join(root, "dingo.log")
	require.NoError(t, syscall.Mkfifo(fifo, 0o600))

	type result struct {
		out []byte
		err error
	}
	done := make(chan result, 1)
	go func() {
		out, err := runEntrypointWithLogFile(t, root, fifo)
		done <- result{out, err}
	}()
	select {
	case res := <-done:
		require.Error(t, res.err, string(res.out))
		require.Contains(
			t,
			string(res.out),
			"DINGO_LOG_FILE must name a new file",
		)
	case <-time.After(20 * time.Second):
		t.Fatal("entrypoint blocked opening a pre-existing FIFO")
	}
}

func TestEntrypointLogFileRefusesSymlinkedParent(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	evil := filepath.Join(root, "evil")
	require.NoError(t, os.MkdirAll(evil, 0o700))
	intended := filepath.Join(root, "logs")
	require.NoError(t, os.Symlink(evil, intended))

	out, err := runEntrypointWithLogFile(
		t,
		root,
		filepath.Join(intended, "dingo.log"),
	)
	require.Error(t, err, string(out))
	require.Contains(t, string(out), "must not traverse a symlink")
	_, err = os.Lstat(filepath.Join(evil, "dingo.log"))
	require.True(t, os.IsNotExist(err))
}

func TestEntrypointLogFileRefusesFifoCreatedBeforeOpen(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	logDir := filepath.Join(root, "logs")
	require.NoError(t, os.Mkdir(logDir, 0o755))
	logPath := filepath.Join(logDir, "dingo.log")
	inject := "[[ -e '" + logPath + "' ]] || mkfifo '" + logPath + "'"

	type result struct {
		out []byte
		err error
	}
	done := make(chan result, 1)
	go func() {
		out, err := runEntrypointWithLogFileRace(t, root, logPath, inject)
		done <- result{out, err}
	}()
	select {
	case res := <-done:
		require.Error(t, res.err, string(res.out))
		require.Contains(
			t,
			string(res.out),
			"DINGO_LOG_FILE must name a new file",
		)
		info, err := os.Lstat(logPath)
		require.NoError(t, err)
		require.Equal(t, os.ModeNamedPipe, info.Mode().Type())
	case <-time.After(20 * time.Second):
		// Unblock the entrypoint so the test process can exit.
		if f, err := os.OpenFile(logPath, os.O_RDONLY, 0); err == nil {
			_ = f.Close()
		}
		t.Fatal("entrypoint opened a FIFO created after its path checks")
	}
}

func TestEntrypointLogFileRefusesParentSwappedBeforeOpen(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	evil := filepath.Join(root, "evil")
	intended := filepath.Join(root, "logs")
	inject := "mkdir -p '" + evil + "'; [[ -L '" + intended + "' ]] || " +
		"{ rm -rf '" + intended + "' && ln -s '" + evil + "' '" +
		intended + "'; }"

	out, err := runEntrypointWithLogFileRace(
		t,
		root,
		filepath.Join(intended, "dingo.log"),
		inject,
	)
	require.Error(t, err, string(out))
	require.Contains(t, string(out), "must not traverse a symlink")
	info, err := os.Lstat(intended)
	require.NoError(t, err)
	require.Equal(t, os.ModeSymlink, info.Mode().Type())
	_, err = os.Lstat(filepath.Join(evil, "dingo.log"))
	require.True(t, os.IsNotExist(err))
}

// setDefaultACL gives dir a default POSIX ACL of user rwx, group rwx and
// other none, which replaces the umask for files created inside it.
func setDefaultACL(t *testing.T, dir string) {
	t.Helper()
	const undefinedID = 0xffffffff
	entries := []struct {
		tag, perm uint16
	}{
		{0x01, 7}, // ACL_USER_OBJ
		{0x04, 7}, // ACL_GROUP_OBJ
		{0x20, 0}, // ACL_OTHER
	}
	buf := binary.LittleEndian.AppendUint32(nil, 2)
	for _, e := range entries {
		buf = binary.LittleEndian.AppendUint16(buf, e.tag)
		buf = binary.LittleEndian.AppendUint16(buf, e.perm)
		buf = binary.LittleEndian.AppendUint32(buf, undefinedID)
	}
	err := syscall.Setxattr(dir, "system.posix_acl_default", buf, 0)
	if errors.Is(err, syscall.ENOTSUP) || errors.Is(err, syscall.EOPNOTSUPP) {
		t.Skipf("filesystem has no POSIX ACL support: %v", err)
	}
	require.NoError(t, err)
}

func TestEntrypointLogFileModeIgnoresDefaultACL(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	logDir := filepath.Join(root, "logs")
	require.NoError(t, os.Mkdir(logDir, 0o755))
	setDefaultACL(t, logDir)
	logPath := filepath.Join(logDir, "dingo.log")

	out, err := runEntrypointWithLogFile(t, root, logPath)
	require.NoError(t, err, string(out))
	info, err := os.Stat(logPath)
	require.NoError(t, err)
	require.Equal(
		t,
		os.FileMode(0o644),
		info.Mode().Perm(),
		strings.TrimSpace(string(out)),
	)
}
