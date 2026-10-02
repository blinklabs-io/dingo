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

// Package netguard provides shared destination checks for outbound HTTP clients.
package netguard

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
)

var (
	ErrBlockedDestination = errors.New("destination is private or special-use")
	ErrNoAddresses        = errors.New("no addresses resolved")
)

type DialContextFunc func(
	context.Context,
	string,
	string,
) (net.Conn, error)

type LookupIPAddrFunc func(context.Context, string) ([]net.IPAddr, error)

var specialUsePrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.31.196.0/24"),
	netip.MustParsePrefix("192.52.193.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("192.175.48.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("100:0:0:1::/64"),
	netip.MustParsePrefix("100::/64"),
	netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("2620:4f:8000::/48"),
	netip.MustParsePrefix("3fff::/20"),
	netip.MustParsePrefix("5f00::/16"),
}

func IsBlockedHost(host string) bool {
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	return host == "localhost" || strings.HasSuffix(host, ".localhost")
}

func IsBlockedIP(ip net.IP) bool {
	if ip == nil || !ip.IsGlobalUnicast() || ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsMulticast() || ip.IsInterfaceLocalMulticast() {
		return true
	}
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return true
	}
	addr = addr.Unmap()
	for _, prefix := range specialUsePrefixes {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

// DialContext resolves and checks every answer before dialing any of them, so
// one public answer cannot make a mixed public/private DNS response acceptable.
func DialContext(
	ctx context.Context,
	network string,
	address string,
	dial DialContextFunc,
	lookup LookupIPAddrFunc,
) (net.Conn, error) {
	if dial == nil {
		return nil, errors.New("destination dialer is required")
	}
	if lookup == nil {
		lookup = net.DefaultResolver.LookupIPAddr
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	if IsBlockedHost(host) {
		return nil, fmt.Errorf("host %q is not allowed: %w", host, ErrBlockedDestination)
	}
	addrs, err := lookup(ctx, host)
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("%w for host %q", ErrNoAddresses, host)
	}
	for _, addr := range addrs {
		if IsBlockedIP(addr.IP) {
			return nil, fmt.Errorf(
				"resolved IP %s is not allowed: %w",
				addr.IP,
				ErrBlockedDestination,
			)
		}
	}
	var lastErr error
	for _, addr := range addrs {
		conn, err := dial(
			ctx,
			network,
			net.JoinHostPort(addr.IP.String(), port),
		)
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
