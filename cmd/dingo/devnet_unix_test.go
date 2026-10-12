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

//go:build unix

package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSignalDevnetChildShutsDownOnHangup(t *testing.T) {
	const childEnv = "DINGO_DEVNET_SIGNAL_CHILD"
	if os.Getenv(childEnv) == "1" {
		signal.Ignore(syscall.SIGHUP)
		_, _ = fmt.Fprintln(os.Stdout, "ready")
		for {
			time.Sleep(time.Hour)
		}
	}

	command := exec.Command(os.Args[0], "-test.run=^TestSignalDevnetChildShutsDownOnHangup$")
	command.Env = append(os.Environ(), childEnv+"=1")
	command.Stderr = io.Discard
	stdout, err := command.StdoutPipe()
	require.NoError(t, err)
	prepareDevnetChild(command)
	require.NoError(t, command.Start())

	finished := make(chan error, 1)
	go func() {
		finished <- command.Wait()
	}()
	waited := false
	t.Cleanup(func() {
		if !waited {
			_ = command.Process.Kill()
			<-finished
		}
	})

	ready := make(chan error, 1)
	go func() {
		line, err := bufio.NewReader(stdout).ReadString('\n')
		if err == nil && line != "ready\n" {
			err = fmt.Errorf("unexpected child output %q", line)
		}
		ready <- err
	}()
	select {
	case err := <-ready:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for child to ignore SIGHUP")
	}

	require.NoError(t, signalDevnetChild(command.Process, syscall.SIGHUP))
	select {
	case err := <-finished:
		waited = true
		require.Error(t, err, "the child should exit after terminal hangup")
	case <-time.After(5 * time.Second):
		t.Fatal("child did not exit after terminal hangup")
	}
}
