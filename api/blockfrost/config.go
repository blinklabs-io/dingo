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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package blockfrost

import "github.com/blinklabs-io/dingo/internal/apiauth"

// BlockfrostConfig holds configuration for the Blockfrost
// API server.
type BlockfrostConfig struct {
	// ListenAddress is the address to listen on.
	// Defaults to ":3000".
	ListenAddress string
	// CORSAllowedOrigins configures Access-Control-Allow-Origin.
	// Empty disables CORS.
	CORSAllowedOrigins []string
	// TLSCertFilePath and TLSKeyFilePath enable TLS on the listener when
	// both are set; either one alone is treated as TLS disabled, matching
	// the built-in UTxO RPC provider's existing convention. Resolved from
	// the shared api: policy plus this provider's own overrides -- see
	// internal/config.ResolveAPISecurity (dingo #2996/#2998).
	TLSCertFilePath string
	TLSKeyFilePath  string
	// Auth configures in-process credential enforcement, shared with Mesh
	// and UTxO RPC via internal/apiauth. The zero value is apiauth.ModeNone
	// (no authentication).
	Auth apiauth.Policy
}
