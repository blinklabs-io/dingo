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
	"time"

	"github.com/blinklabs-io/dingo/internal/netguard"
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
	return netguard.DialContext(
		ctx,
		network,
		address,
		d.dialer.DialContext,
		net.DefaultResolver.LookupIPAddr,
	)
}

func isBlockedMithrilHost(host string) bool {
	return netguard.IsBlockedHost(host)
}

func isBlockedMithrilIP(ip net.IP) bool {
	return netguard.IsBlockedIP(ip)
}
