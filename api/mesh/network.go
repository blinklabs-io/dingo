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

package mesh

import (
	"net/http"
)

// handleNetworkList handles POST /network/list.
func (s *Server) handleNetworkList(
	w http.ResponseWriter,
	r *http.Request,
) {
	var req MetadataRequest
	if err := decodeRequest(w, r, &req); err != nil {
		writeError(w, ErrInvalidRequest)
		return
	}

	resp := &NetworkListResponse{
		NetworkIdentifiers: []*NetworkIdentifier{
			{
				Blockchain: blockchain,
				Network:    s.networkID.Network,
			},
		},
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleNetworkOptions handles POST /network/options.
func (s *Server) handleNetworkOptions(
	w http.ResponseWriter,
	r *http.Request,
) {
	var req NetworkRequest
	if meshErr := s.decodeAndValidate(
		w, r, &req,
	); meshErr != nil {
		writeError(w, meshErr)
		return
	}

	resp := &NetworkOptionsResponse{
		Version: &Version{
			RosettaVersion: rosettaVersion,
			NodeVersion:    nodeVersion,
		},
		Allow: &Allow{
			OperationStatuses:       OperationStatuses(),
			OperationTypes:          OperationTypes(),
			Errors:                  AllErrors(),
			HistoricalBalanceLookup: false,
			MempoolCoins:            false,
		},
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleNetworkStatus handles POST /network/status.
func (s *Server) handleNetworkStatus(
	w http.ResponseWriter,
	r *http.Request,
) {
	var req NetworkRequest
	if meshErr := s.decodeAndValidate(
		w, r, &req,
	); meshErr != nil {
		writeError(w, meshErr)
		return
	}

	tip := s.config.Chain.Tip()
	tipTimestamp := s.slotToTimestamp(tip.Point.Slot)
	synced := true

	resp := &NetworkStatusResponse{
		CurrentBlockIdentifier: s.tipBlockID(tip),
		CurrentBlockTimestamp:  tipTimestamp,
		GenesisBlockIdentifier: s.genesisID,
		SyncStatus: &SyncStatus{
			Synced: &synced,
		},
		Peers: []*Peer{},
	}
	writeJSON(w, http.StatusOK, resp)
}
