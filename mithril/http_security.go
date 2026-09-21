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
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

const (
	maxMithrilRedirects       = 10
	defaultMithrilDialTimeout = 30 * time.Second
)

type restrictedHTTPTransport struct {
	*http.Transport
	allowPrivate bool
}

func newMithrilHTTPClient(
	timeout time.Duration,
	allowPrivate bool,
) *http.Client {
	client := &http.Client{Timeout: timeout}
	if allowPrivate {
		client.CheckRedirect = mithrilRedirectPolicy(nil, true)
		return client
	}
	client.Transport = newRestrictedHTTPTransport(nil, false)
	client.CheckRedirect = mithrilRedirectPolicy(nil, false)
	return client
}

func secureMithrilHTTPClient(
	client *http.Client,
	allowPrivate bool,
) (*http.Client, error) {
	if client == nil {
		return newMithrilHTTPClient(0, allowPrivate), nil
	}
	secured := *client
	secured.CheckRedirect = mithrilRedirectPolicy(
		client.CheckRedirect,
		allowPrivate,
	)
	if allowPrivate {
		return &secured, nil
	}
	transport, err := secureMithrilHTTPTransport(client.Transport)
	if err != nil {
		return nil, err
	}
	secured.Transport = transport
	return &secured, nil
}

func secureMithrilHTTPTransport(
	base http.RoundTripper,
) (http.RoundTripper, error) {
	if base == nil {
		return newRestrictedHTTPTransport(nil, false), nil
	}
	if restricted, ok := base.(*restrictedHTTPTransport); ok {
		if !restricted.allowPrivate {
			return restricted, nil
		}
		return newRestrictedHTTPTransport(restricted.Transport, false), nil
	}
	transport, ok := base.(*http.Transport)
	if !ok {
		return nil, fmt.Errorf(
			"mithril HTTP transport %T cannot enforce private-address restrictions",
			base,
		)
	}
	return newRestrictedHTTPTransport(transport, false), nil
}

func newRestrictedHTTPTransport(
	base *http.Transport,
	allowPrivate bool,
) *restrictedHTTPTransport {
	var transport *http.Transport
	if base == nil {
		if defaultTransport, ok := http.DefaultTransport.(*http.Transport); ok {
			transport = defaultTransport.Clone()
		} else {
			transport = &http.Transport{}
		}
	} else {
		transport = base.Clone()
	}
	applyMithrilTransportRestrictions(transport, allowPrivate)
	return &restrictedHTTPTransport{
		Transport:    transport,
		allowPrivate: allowPrivate,
	}
}

func applyMithrilTransportRestrictions(
	transport *http.Transport,
	allowPrivate bool,
) {
	dialer := &mithrilRestrictedDialer{
		dialer:       &net.Dialer{Timeout: defaultMithrilDialTimeout},
		allowPrivate: allowPrivate,
	}
	transport.Proxy = nil
	transport.DialContext = dialer.DialContext
	// Clear deprecated hooks that could bypass the restricted dialer.
	transport.Dial = nil    //nolint:staticcheck
	transport.DialTLS = nil //nolint:staticcheck
	transport.DialTLSContext = nil
}

func mithrilRedirectPolicy(
	next func(*http.Request, []*http.Request) error,
	allowPrivate bool,
) func(*http.Request, []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		if len(via) >= maxMithrilRedirects {
			return errors.New("too many redirects")
		}
		if err := requireSecureURL(
			req.URL.String(),
			"Mithril redirect URL",
			allowPrivate,
		); err != nil {
			return err
		}
		if next != nil {
			return next(req, via)
		}
		return nil
	}
}

type mithrilRestrictedDialer struct {
	dialer       *net.Dialer
	allowPrivate bool
}

func (d *mithrilRestrictedDialer) DialContext(
	ctx context.Context,
	network string,
	address string,
) (net.Conn, error) {
	if d.allowPrivate {
		return d.dialer.DialContext(ctx, network, address)
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	if isBlockedMithrilHost(host) {
		return nil, fmt.Errorf("host %q is not allowed", host)
	}
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no addresses resolved for host %q", host)
	}
	for _, addr := range addrs {
		if isBlockedMithrilIP(addr.IP) {
			return nil, fmt.Errorf("resolved IP %s is not allowed", addr.IP)
		}
	}
	var lastErr error
	for _, addr := range addrs {
		target := net.JoinHostPort(addr.IP.String(), port)
		conn, err := d.dialer.DialContext(ctx, network, target)
		if err == nil {
			return conn, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = errors.New("dial failed")
	}
	return nil, lastErr
}

func isBlockedMithrilHost(host string) bool {
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	return host == "localhost" || strings.HasSuffix(host, ".localhost")
}

func isBlockedMithrilIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	return ip.IsUnspecified() ||
		ip.IsLoopback() ||
		ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsMulticast() ||
		ip.IsInterfaceLocalMulticast() ||
		isSpecialUseMithrilIPv4(ip)
}

func isSpecialUseMithrilIPv4(ip net.IP) bool {
	v4 := ip.To4()
	if v4 == nil {
		return false
	}
	return v4[0] == 100 && v4[1]&0xc0 == 64 ||
		v4[0] == 192 && v4[1] == 0 && v4[2] == 0 ||
		v4[0] == 192 && v4[1] == 0 && v4[2] == 2 ||
		v4[0] == 198 && (v4[1] == 18 || v4[1] == 19) ||
		v4[0] == 198 && v4[1] == 51 && v4[2] == 100 ||
		v4[0] == 203 && v4[1] == 0 && v4[2] == 113 ||
		v4[0] >= 240 ||
		v4[0] == 255 && v4[1] == 255 && v4[2] == 255 && v4[3] == 255
}
