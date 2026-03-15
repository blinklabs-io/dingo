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
	"errors"
	"fmt"

	bls12381 "github.com/consensys/gnark-crypto/ecc/bls12-381"
)

func decodeBLSG1Point(encoded string) (*bls12381.G1Affine, error) {
	raw, ok := decodePrimaryEncodedBytes(encoded)
	if !ok {
		return nil, errors.New("could not decode encoded bytes")
	}
	var point bls12381.G1Affine
	// SetBytes validates curve membership and subgroup order.
	if _, err := point.SetBytes(raw); err != nil {
		return nil, fmt.Errorf("decoding G1 point: %w", err)
	}
	if point.IsInfinity() {
		return nil, errors.New("point is infinity")
	}
	return &point, nil
}

func decodeBLSG2Point(encoded string) (*bls12381.G2Affine, error) { //nolint:unparam // return value used post-merge
	raw, ok := decodePrimaryEncodedBytes(encoded)
	if !ok {
		return nil, errors.New("could not decode encoded bytes")
	}
	var point bls12381.G2Affine
	// SetBytes validates curve membership and subgroup order.
	if _, err := point.SetBytes(raw); err != nil {
		return nil, fmt.Errorf("decoding G2 point: %w", err)
	}
	if point.IsInfinity() {
		return nil, errors.New("point is infinity")
	}
	return &point, nil
}
