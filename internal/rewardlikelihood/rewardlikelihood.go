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

package rewardlikelihood

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

const (
	SampleCount = 100
	decay       = float32(0.9)
)

func Encode(values []float32) ([]byte, error) {
	if len(values) != SampleCount {
		return nil, fmt.Errorf(
			"reward likelihood has %d samples, want %d",
			len(values),
			SampleCount,
		)
	}
	ret := make([]byte, SampleCount*4)
	for i, value := range values {
		binary.BigEndian.PutUint32(ret[i*4:], math.Float32bits(value))
	}
	return ret, nil
}

func Decode(data []byte) ([]float32, error) {
	if len(data) != SampleCount*4 {
		return nil, fmt.Errorf(
			"reward likelihood is %d bytes, want %d",
			len(data),
			SampleCount*4,
		)
	}
	ret := make([]float32, SampleCount)
	for i := range ret {
		ret[i] = math.Float32frombits(binary.BigEndian.Uint32(data[i*4:]))
		if math.IsNaN(float64(ret[i])) || math.IsInf(float64(ret[i]), 0) {
			return nil, errors.New(
				"reward likelihood contains a non-finite sample",
			)
		}
	}
	return ret, nil
}

func LeaderProbability(
	activeSlotsCoeff, relativeStake, decentralization float64,
) float64 {
	return (1 - math.Pow(1-activeSlotsCoeff, relativeStake)) *
		(1 - decentralization)
}

func Pool(blocks, slots uint64, leaderProbability float64) []float32 {
	ret := make([]float32, SampleCount)
	for i := range ret {
		x := (float64(i) + 0.5) / SampleCount
		// The reference computes n*log(x) + m*log(1-t*x) in binary64. The
		// float64 conversions round each product before the sum, which a
		// target with fused multiply-add would otherwise skip.
		hit := float64(float64(blocks) * math.Log(x))
		miss := float64(
			float64(slots-blocks) * math.Log(1-float64(leaderProbability*x)),
		)
		ret[i] = float32(hit + miss)
	}
	return ret
}

func Combine(previous, current []float32) ([]float32, error) {
	if len(previous) != SampleCount || len(current) != SampleCount {
		return nil, errors.New(
			"combine reward likelihood: invalid sample count",
		)
	}
	ret := make([]float32, SampleCount)
	minimum := float32(math.Inf(1))
	for i := range ret {
		// The reference rounds the decayed likelihood to binary32 before the
		// addition; the conversion keeps a fused multiply-add from skipping
		// that rounding.
		ret[i] = float32(decay*previous[i]) + current[i]
		if ret[i] < minimum {
			minimum = ret[i]
		}
	}
	for i := range ret {
		ret[i] -= minimum
	}
	return ret, nil
}

func Performance(likelihood []float32) (float64, error) {
	if len(likelihood) != SampleCount {
		return 0, errors.New(
			"reward performance estimate: invalid sample count",
		)
	}
	prior := make([]float32, len(likelihood))
	for i := range prior {
		x := (float64(i) + 0.5) / SampleCount
		prior[i] = float32(39 * math.Log(x))
	}
	prior, err := normalize(prior)
	if err != nil {
		return 0, err
	}
	posterior := make([]float32, len(likelihood))
	for i, value := range likelihood {
		posterior[i] = prior[i] + value
	}
	posterior, err = normalize(posterior)
	if err != nil {
		return 0, err
	}
	cumulative := 0.0
	for i, value := range posterior {
		if cumulative > 0.5 {
			return (float64(i) + 0.5) / SampleCount, nil
		}
		cumulative += math.Exp(float64(value))
	}
	return 1, nil
}

func normalize(values []float32) ([]float32, error) {
	maximum := float32(-math.MaxFloat32)
	for _, value := range values {
		if value > maximum {
			maximum = value
		}
	}
	normalized := make([]float32, len(values))
	area := 0.0
	for i, value := range values {
		normalized[i] = value - maximum
		area += float64(0.01 * math.Exp(float64(normalized[i])))
	}
	if area <= 0 || math.IsInf(area, 0) || math.IsNaN(area) {
		return nil, errors.New(
			"reward performance estimate: invalid histogram area",
		)
	}
	logArea := float32(math.Log(area))
	for i := range normalized {
		normalized[i] -= logArea
	}
	return normalized, nil
}
