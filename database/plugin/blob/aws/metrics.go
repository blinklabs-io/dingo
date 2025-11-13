// Copyright 2025 Blink Labs Software
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

package aws

import "github.com/prometheus/client_golang/prometheus"

const s3MetricNamePrefix = "database_blob_"

func (d *BlobStoreS3) registerBlobMetrics() {
	opsTotal := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: s3MetricNamePrefix + "ops_total",
			Help: "Total number of S3 blob operations",
		},
	)
	bytesTotal := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: s3MetricNamePrefix + "bytes_total",
			Help: "Total bytes read/written for S3 blob operations",
		},
	)

	d.promRegistry.MustRegister(opsTotal, bytesTotal)
}
