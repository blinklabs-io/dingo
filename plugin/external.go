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

package plugin

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/blinklabs-io/dingo/plugin/pluginrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const (
	// ExternalProtocolVersion is the version of the control protocol the host
	// speaks with out-of-process plugins. The plugin SDK reports its own and
	// the two must be equal.
	ExternalProtocolVersion uint32 = 0

	externalTokenHeader = "dingo-plugin-token"
	// maxSocketPathBytes keeps the socket path inside the sun_path limit of
	// the strictest supported platform (104 bytes on macOS, NUL included).
	maxSocketPathBytes = 100
	// killWait bounds the wait for the process to disappear after SIGKILL.
	killWait = 5 * time.Second
	// stderrTailBytes is how much of a plugin's standard error is kept to
	// explain a failure.
	stderrTailBytes = 4096
)

// ExternalConfig is the typed configuration of an out-of-process provider
// registered with RegisterExternal.
type ExternalConfig struct {
	// Command is the plugin executable.
	Command string `yaml:"command"`
	// Args are the arguments passed to Command.
	Args []string `yaml:"args"`
	// Env is the plugin's entire environment. The host's own environment is
	// not inherited, so credentials never leak to a plugin by accident.
	Env map[string]string `yaml:"env"`
	// RuntimeDir is the directory the per-plugin runtime directory is created
	// in. Empty selects the system temporary directory. The socket path must
	// stay short: Unix socket paths are limited to about 100 bytes.
	RuntimeDir string `yaml:"runtimeDir"`
	// StartTimeout bounds launch plus negotiation.
	StartTimeout time.Duration `yaml:"startTimeout"`
	// StopTimeout bounds each stop phase: the Stop request, then SIGTERM,
	// then SIGKILL is sent without further waiting beyond a fixed bound.
	StopTimeout time.Duration `yaml:"stopTimeout"`
	// KeepaliveInterval is how often the host pings a running plugin.
	KeepaliveInterval time.Duration `yaml:"keepaliveInterval"`
	// KeepaliveTimeout bounds one ping. A failed or timed-out ping is a
	// plugin failure.
	KeepaliveTimeout time.Duration `yaml:"keepaliveTimeout"`
}

func defaultExternalConfig() ExternalConfig {
	return ExternalConfig{
		StartTimeout:      30 * time.Second,
		StopTimeout:       10 * time.Second,
		KeepaliveInterval: 5 * time.Second,
		KeepaliveTimeout:  10 * time.Second,
	}
}

func (c ExternalConfig) validate() error {
	if c.Command == "" {
		return errors.New("external plugin command is empty")
	}
	if c.StartTimeout <= 0 || c.StopTimeout <= 0 ||
		c.KeepaliveInterval <= 0 || c.KeepaliveTimeout <= 0 {
		return errors.New("external plugin timeouts must be positive")
	}
	return nil
}

// CommandFunc builds the process to launch for a plugin. It owns the command
// line and the environment, which makes it the place to wrap the launch, for
// example to run the plugin inside a sandbox. The host attaches standard
// input and, unless set, standard error; it starts the process itself.
type CommandFunc func(ExternalConfig) (*exec.Cmd, error)

// ExternalOption customizes RegisterExternal.
type ExternalOption func(*externalOptions)

type externalOptions struct {
	command CommandFunc
}

// WithCommand replaces how the plugin process is constructed.
func WithCommand(fn CommandFunc) ExternalOption {
	return func(o *externalOptions) { o.command = fn }
}

func defaultCommand(cfg ExternalConfig) (*exec.Cmd, error) {
	cmd := exec.Command(cfg.Command, cfg.Args...) //nolint:gosec // operator-configured executable
	cmd.Env = make([]string, 0, len(cfg.Env))
	for _, k := range slices.Sorted(maps.Keys(cfg.Env)) {
		cmd.Env = append(cmd.Env, k+"="+cfg.Env[k])
	}
	return cmd, nil
}

// Failure describes a running external plugin that stopped working.
type Failure struct {
	Capability Capability
	Provider   string
	Err        error
}

// SetFailureHandler sets the function the host calls, once per plugin, when a
// started external plugin exits or stops answering keepalives. The host logs
// the failure first. The handler runs on its own goroutine, so it may call
// Stop to unwind every provider in reverse start order. Failures during
// startup are returned by Resolve instead.
func (h *Host) SetFailureHandler(fn func(Failure)) {
	h.mu.Lock()
	h.failureHandler = fn
	h.mu.Unlock()
}

func (h *Host) reportFailure(f Failure) {
	h.log().Error(
		"plugin failed",
		"capability", f.Capability,
		"plugin", f.Provider,
		"error", f.Err,
	)
	h.mu.Lock()
	fn := h.failureHandler
	h.mu.Unlock()
	if fn != nil {
		go fn(f)
	}
}

// RegisterExternal registers a provider that runs as a separate process and
// serves its capability over gRPC. bind turns the connection to the plugin
// into the capability's service type, typically a generated client, so Resolve
// returns the same S a built-in provider returns and its callers do not
// change. D is the dependency bundle type of the built-in providers of the
// capability; an external provider ignores it.
func RegisterExternal[S, D any](
	host *Host,
	descriptor Descriptor,
	bind func(grpc.ClientConnInterface) S,
	opts ...ExternalOption,
) error {
	if bind == nil {
		return fmt.Errorf(
			"external plugin bind function is nil for %s/%s",
			descriptor.Capability.Name(),
			descriptor.Name,
		)
	}
	o := externalOptions{command: defaultCommand}
	for _, opt := range opts {
		opt(&o)
	}
	if o.command == nil {
		return errors.New("external plugin command function is nil")
	}
	return Register(
		host,
		descriptor,
		defaultExternalConfig,
		func(_ context.Context, cfg ExternalConfig, _ D) (S, Instance, error) {
			var zero S
			inst, err := newExternalInstance(host, descriptor, cfg, o)
			if err != nil {
				return zero, nil, err
			}
			return bind(inst.conn), inst, nil
		},
	)
}

type externalInstance struct {
	host       *Host
	descriptor Descriptor
	cfg        ExternalConfig
	opts       externalOptions

	runtimeDir string
	socketPath string
	token      string
	conn       *grpc.ClientConn
	control    pluginrpc.ControlClient

	cmd      *exec.Cmd
	stdin    io.Closer
	stderr   *tailBuffer
	exited   chan struct{}
	exitErr  error
	stopping atomic.Bool

	watchCancel context.CancelFunc
	watchDone   chan struct{}
	failOnce    sync.Once
	cleanupOnce sync.Once
	cleanupErr  error
}

func newExternalInstance(
	host *Host,
	descriptor Descriptor,
	cfg ExternalConfig,
	opts externalOptions,
) (*externalInstance, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return nil, fmt.Errorf("generate plugin token: %w", err)
	}
	dir, err := os.MkdirTemp(cfg.RuntimeDir, "dp")
	if err != nil {
		return nil, fmt.Errorf("create plugin runtime directory: %w", err)
	}
	i := &externalInstance{
		host:       host,
		descriptor: descriptor,
		cfg:        cfg,
		opts:       opts,
		runtimeDir: dir,
		socketPath: filepath.Join(dir, "p.sock"),
		token:      base64.RawURLEncoding.EncodeToString(tokenBytes),
		stderr:     &tailBuffer{max: stderrTailBytes},
		exited:     make(chan struct{}),
	}
	if len(i.socketPath) > maxSocketPathBytes {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf(
			"plugin socket path %q is longer than %d bytes; set a shorter runtimeDir",
			i.socketPath,
			maxSocketPathBytes,
		)
	}
	conn, err := grpc.NewClient(
		"unix://"+i.socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(i.attachTokenUnary),
		grpc.WithStreamInterceptor(i.attachTokenStream),
	)
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("create plugin connection: %w", err)
	}
	i.conn = conn
	i.control = pluginrpc.NewControlClient(conn)
	return i, nil
}

func (i *externalInstance) attachTokenUnary(
	ctx context.Context,
	method string,
	req, reply any,
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	return invoker(i.withToken(ctx), method, req, reply, cc, opts...)
}

func (i *externalInstance) attachTokenStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	cc *grpc.ClientConn,
	method string,
	streamer grpc.Streamer,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	return streamer(i.withToken(ctx), desc, cc, method, opts...)
}

func (i *externalInstance) withToken(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, externalTokenHeader, i.token)
}

func (i *externalInstance) name() string {
	return i.descriptor.Capability.Name() + "/" + i.descriptor.Name
}

// Start launches the plugin, hands it the handshake and negotiates versions.
// It fails fast when the process exits first.
func (i *externalInstance) Start(ctx context.Context) error {
	cmd, err := i.opts.command(i.cfg)
	if err != nil {
		return fmt.Errorf("build plugin command: %w", err)
	}
	if cmd == nil {
		return errors.New("plugin command function returned no command")
	}
	if cmd.Stderr == nil {
		cmd.Stderr = i.stderr
	}
	cmd.WaitDelay = time.Second
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("open plugin standard input: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("launch plugin: %w", err)
	}
	i.cmd = cmd
	i.stdin = stdin
	go func() {
		i.exitErr = cmd.Wait()
		close(i.exited)
	}()

	hs, err := json.Marshal(struct {
		ProtocolVersion uint32 `json:"protocol_version"`
		SocketPath      string `json:"socket_path"`
		Token           string `json:"token"`
	}{ExternalProtocolVersion, i.socketPath, i.token})
	if err != nil {
		return fmt.Errorf("encode plugin handshake: %w", err)
	}
	if _, err := stdin.Write(append(hs, '\n')); err != nil {
		return i.startError(fmt.Errorf("write plugin handshake: %w", err))
	}

	nctx, cancel := context.WithTimeout(ctx, i.cfg.StartTimeout)
	defer cancel()
	go func() {
		select {
		case <-i.exited:
			cancel()
		case <-nctx.Done():
		}
	}()
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return fmt.Errorf("generate negotiation nonce: %w", err)
	}
	resp, err := i.control.Negotiate(
		nctx,
		&pluginrpc.NegotiateRequest{
			ProtocolVersion: ExternalProtocolVersion,
			Capability:      string(i.descriptor.Capability),
			Nonce:           nonce,
		},
		grpc.WaitForReady(true),
	)
	if err != nil {
		return i.startError(fmt.Errorf("negotiate with plugin: %w", err))
	}
	if err := i.verify(resp, nonce); err != nil {
		return err
	}

	watchCtx, watchCancel := context.WithCancel(
		context.WithoutCancel(ctx),
	)
	i.watchCancel = watchCancel
	i.watchDone = make(chan struct{})
	go i.watch(watchCtx)
	return nil
}

// startError explains a startup failure, preferring the process's own exit.
func (i *externalInstance) startError(err error) error {
	select {
	case <-i.exited:
		return fmt.Errorf(
			"plugin exited during startup: %w",
			errors.Join(i.exitError(), err),
		)
	default:
		return err
	}
}

// exitError describes the plugin's exit with the tail of its standard error.
func (i *externalInstance) exitError() error {
	err := i.exitErr
	if err == nil {
		err = errors.New("exit status 0")
	}
	if tail := strings.TrimSpace(i.stderr.String()); tail != "" {
		return fmt.Errorf("%w (stderr: %s)", err, tail)
	}
	return err
}

func (i *externalInstance) verify(
	resp *pluginrpc.NegotiateResponse,
	nonce []byte,
) error {
	mac := hmac.New(sha256.New, []byte(i.token))
	mac.Write(nonce)
	if !hmac.Equal(mac.Sum(nil), resp.GetProof()) {
		return errors.New("plugin did not prove it holds the launch token")
	}
	if resp.GetProtocolVersion() != ExternalProtocolVersion {
		return fmt.Errorf(
			"plugin %q speaks control protocol v%d, host speaks v%d: versions must match exactly",
			resp.GetPluginName(),
			resp.GetProtocolVersion(),
			ExternalProtocolVersion,
		)
	}
	want := string(i.descriptor.Capability)
	if slices.Contains(resp.GetCapabilities(), want) {
		return nil
	}
	for _, got := range resp.GetCapabilities() {
		if Capability(got).Name() == i.descriptor.Capability.Name() {
			return fmt.Errorf(
				"plugin %q implements %q, host requires %q: contract versions must match exactly",
				resp.GetPluginName(),
				got,
				want,
			)
		}
	}
	return fmt.Errorf(
		"plugin %q does not implement %q (implements %v)",
		resp.GetPluginName(),
		want,
		resp.GetCapabilities(),
	)
}

func (i *externalInstance) watch(ctx context.Context) {
	defer close(i.watchDone)
	ticker := time.NewTicker(i.cfg.KeepaliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-i.exited:
			i.fail(fmt.Errorf("plugin process exited: %w", i.exitError()))
			return
		case <-ticker.C:
			pctx, cancel := context.WithTimeout(ctx, i.cfg.KeepaliveTimeout)
			_, err := i.control.Ping(pctx, &pluginrpc.PingRequest{})
			cancel()
			if err != nil && ctx.Err() == nil {
				select {
				case <-i.exited:
					i.fail(fmt.Errorf("plugin process exited: %w", i.exitError()))
				default:
					i.fail(fmt.Errorf("plugin keepalive failed: %w", err))
				}
				return
			}
		}
	}
}

func (i *externalInstance) fail(err error) {
	i.failOnce.Do(func() {
		if i.stopping.Load() {
			return
		}
		i.host.reportFailure(Failure{
			Capability: i.descriptor.Capability,
			Provider:   i.descriptor.Name,
			Err:        err,
		})
	})
}

// Stop asks the plugin to stop, then sends SIGTERM and finally SIGKILL, each
// phase bounded by StopTimeout and ctx. It reports an error when the plugin
// needed more than the Stop request. Stop releases the runtime directory in
// every case.
func (i *externalInstance) Stop(ctx context.Context) error {
	i.stopping.Store(true)
	if i.watchCancel != nil {
		i.watchCancel()
		<-i.watchDone
	}
	var stopErr error
	if i.cmd != nil {
		stopErr = i.terminate(ctx)
	}
	return errors.Join(stopErr, i.cleanup())
}

func (i *externalInstance) terminate(ctx context.Context) error {
	select {
	case <-i.exited:
		return nil
	default:
	}
	phase, cancel := context.WithTimeout(ctx, i.cfg.StopTimeout)
	_, _ = i.control.Stop(phase, &pluginrpc.StopRequest{})
	if i.waitExit(phase) {
		cancel()
		return nil
	}
	cancel()

	// A plugin that ignores Stop still gets a signal it can handle before the
	// one it cannot. Windows has no SIGTERM, so Signal fails and the kill
	// follows immediately.
	if err := i.cmd.Process.Signal(syscall.SIGTERM); err == nil {
		phase, cancel = context.WithTimeout(ctx, i.cfg.StopTimeout)
		exited := i.waitExit(phase)
		cancel()
		if exited {
			return fmt.Errorf(
				"plugin %s ignored Stop and exited after SIGTERM",
				i.name(),
			)
		}
	}
	_ = i.cmd.Process.Kill()
	killCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), killWait)
	defer cancel()
	if !i.waitExit(killCtx) {
		return fmt.Errorf("plugin %s did not exit after SIGKILL", i.name())
	}
	return fmt.Errorf("plugin %s ignored Stop and SIGTERM and was killed", i.name())
}

func (i *externalInstance) waitExit(ctx context.Context) bool {
	select {
	case <-i.exited:
		return true
	case <-ctx.Done():
		return false
	}
}

func (i *externalInstance) cleanup() error {
	i.cleanupOnce.Do(func() {
		var errs []error
		if i.stdin != nil {
			_ = i.stdin.Close()
		}
		if err := i.conn.Close(); err != nil {
			errs = append(errs, err)
		}
		if err := os.RemoveAll(i.runtimeDir); err != nil {
			errs = append(errs, err)
		}
		i.cleanupErr = errors.Join(errs...)
	})
	return i.cleanupErr
}

// tailBuffer keeps the last max bytes written to it.
type tailBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
	max int
}

func (t *tailBuffer) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf.Write(p)
	if over := t.buf.Len() - t.max; over > 0 {
		t.buf.Next(over)
	}
	return len(p), nil
}

func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.buf.String()
}
