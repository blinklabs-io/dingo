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

package leios

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type voteManagerMetrics struct {
	votesReceivedTotal     prometheus.Counter
	votesRejectedTotal     *prometheus.CounterVec
	votesEquivocationTotal prometheus.Counter
	ebQuorumReachedTotal   prometheus.Counter
	certificatesBuiltTotal prometheus.Counter
	committeeSize          prometheus.Gauge
	voteRecordsCount       prometheus.Gauge
}

func initVoteManagerMetrics(reg prometheus.Registerer) *voteManagerMetrics {
	factory := promauto.With(reg)
	return &voteManagerMetrics{
		votesReceivedTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_metrics_leios_votes_received_total",
			Help: "number of leios votes received from peers or produced locally",
		}),
		votesRejectedTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "dingo_metrics_leios_votes_rejected_total",
			Help: "number of leios votes rejected, by reason",
		}, []string{"reason"}),
		votesEquivocationTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_metrics_leios_votes_equivocation_total",
			Help: "number of equivocating leios votes dropped (same voter and slot, different endorser block)",
		}),
		ebQuorumReachedTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_metrics_leios_eb_quorum_reached_total",
			Help: "number of endorser blocks whose verified votes met the stake quorum",
		}),
		certificatesBuiltTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_metrics_leios_certificates_built_total",
			Help: "number of leios EB certificates built from aggregated votes",
		}),
		committeeSize: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dingo_metrics_leios_committee_size_int",
			Help: "member count of the most recently computed voting committee",
		}),
		voteRecordsCount: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dingo_metrics_leios_vote_records_int",
			Help: "current size of the vote dedup record ledger",
		}),
	}
}

type pipelineMetrics struct {
	ebObservedTotal     prometheus.Counter
	ebEquivocationTotal prometheus.Counter
	ebCertifiedTotal    prometheus.Counter
	certsRejectedTotal  *prometheus.CounterVec
	instancesCount      prometheus.Gauge
	stagesCount         *prometheus.GaugeVec
}

func initPipelineManagerMetrics(reg prometheus.Registerer) *pipelineMetrics {
	factory := promauto.With(reg)
	return &pipelineMetrics{
		ebObservedTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_metrics_leios_pipeline_eb_observed_total",
			Help: "number of endorser blocks observed into the leios pipeline",
		}),
		ebEquivocationTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_metrics_leios_pipeline_eb_equivocation_total",
			Help: "number of slots with equivocating endorser blocks (more than one distinct EB for the same slot)",
		}),
		ebCertifiedTotal: factory.NewCounter(prometheus.CounterOpts{
			Name: "dingo_metrics_leios_pipeline_eb_certified_total",
			Help: "number of endorser blocks marked certified in the leios pipeline",
		}),
		certsRejectedTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "dingo_metrics_leios_pipeline_certs_rejected_total",
			Help: "number of leios EB certificates rejected by the pipeline, by reason",
		}, []string{"reason"}),
		instancesCount: factory.NewGauge(prometheus.GaugeOpts{
			Name: "dingo_metrics_leios_pipeline_instances_int",
			Help: "current number of tracked leios pipeline instances",
		}),
		stagesCount: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "dingo_metrics_leios_pipeline_ebs_by_stage_int",
			Help: "current count of tracked endorser blocks by pipeline stage",
		}, []string{"stage"}),
	}
}
