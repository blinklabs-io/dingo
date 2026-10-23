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

import "sync"

// CertificateCache remembers the certificates of every chain a verification
// accepted, so a later verification under the same genesis key stops walking
// at the first certificate it has already verified.
//
// A certificate enters the cache only after its whole chain verified to a
// genesis certificate signed by the pinned key, the leaf bound to its
// artifact, and the stake distributions validated, so a cached certificate is
// as trustworthy as a freshly walked one. A new leaf still has every new
// certificate fetched and STM-verified, and the link from the newest of them
// to the cached one checked the same way as any other link: same-epoch AVK
// and parameters, or the parent's next AVK and next parameters across an
// epoch. A leaf the cache has already accepted is not walked at all.
//
// The cache holds each accepted chain once, as a walk already holds it while
// it runs, and grows by the few certificates the aggregator adds per epoch.
// A different genesis key empties it. It is safe for concurrent use; the
// zero value is not usable, so build one with NewCertificateCache.
type CertificateCache struct {
	mu         sync.Mutex
	genesisKey string
	certs      map[string]*Certificate
	leaves     map[string]struct{}
}

// NewCertificateCache returns an empty cache.
func NewCertificateCache() *CertificateCache {
	return &CertificateCache{
		certs:  map[string]*Certificate{},
		leaves: map[string]struct{}{},
	}
}

// resetForKeyLocked empties the cache when it was filled under another
// genesis key. Callers hold c.mu.
func (c *CertificateCache) resetForKeyLocked(genesisKey string) {
	if c.genesisKey == genesisKey {
		return
	}
	c.genesisKey = genesisKey
	c.certs = map[string]*Certificate{}
	c.leaves = map[string]struct{}{}
}

// acceptedLeaf returns the leaf certificate hash names when a verification
// under genesisKey already accepted it as a leaf, and nil otherwise.
func (c *CertificateCache) acceptedLeaf(genesisKey, hash string) *Certificate {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resetForKeyLocked(genesisKey)
	if _, ok := c.leaves[hash]; !ok {
		return nil
	}
	return c.certs[hash]
}

// chainFrom returns the cached chain from hash to its genesis certificate,
// or false when hash is not cached under genesisKey or its chain is not
// complete in the cache.
func (c *CertificateCache) chainFrom(
	genesisKey, hash string,
) ([]*Certificate, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resetForKeyLocked(genesisKey)
	var chain []*Certificate
	seen := map[string]struct{}{}
	for {
		cert, ok := c.certs[hash]
		if !ok {
			return nil, false
		}
		if _, loop := seen[hash]; loop {
			return nil, false
		}
		seen[hash] = struct{}{}
		chain = append(chain, cert)
		if cert.IsGenesis() || cert.IsChainingToItself() {
			return chain, true
		}
		hash = cert.PreviousHash
	}
}

// accept records a verified chain whose leaf was accepted for an artifact.
func (c *CertificateCache) accept(
	genesisKey string,
	result *CertificateChainVerificationResult,
) {
	if c == nil || result == nil || result.LeafCertificate == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resetForKeyLocked(genesisKey)
	for _, cert := range result.Certificates {
		if cert != nil {
			c.certs[cert.Hash] = cert
		}
	}
	c.leaves[result.LeafCertificate.Hash] = struct{}{}
}

// certificateAnchors lets a chain walk stop at an already-verified
// certificate. A nil value anchors nothing.
type certificateAnchors struct {
	cache      *CertificateCache
	genesisKey string
}

// chainFrom returns the verified chain from hash to genesis, when hash is a
// verified certificate.
func (a *certificateAnchors) chainFrom(hash string) ([]*Certificate, bool) {
	if a == nil {
		return nil, false
	}
	return a.cache.chainFrom(a.genesisKey, hash)
}
