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

package kupo

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const jsonContentType = "application/json;charset=utf-8"

func (s *Server) handleMatches(w http.ResponseWriter, r *http.Request) {
	query, err := parseMatchQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	matches, err := s.node.Matches(r.Context(), query)
	if err != nil {
		s.writeNodeError(w, err)
		return
	}
	defer matches.Close()
	if notModifiedAt(w, r, matches.Tip()) {
		return
	}
	stringQuantities := strings.Contains(
		r.Header.Get("Accept"),
		"asset-quantity=string",
	)
	contentType := jsonContentType
	if stringQuantities {
		contentType = "application/json;charset=utf-8;asset-quantity=string"
	}
	w.Header().Set("Content-Type", contentType)

	first, ok, err := matches.Next()
	if err != nil {
		s.writeNodeError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
	if _, err := io.WriteString(w, "["); err != nil {
		return
	}
	written := 0
	for ok {
		if written > 0 {
			if _, err := io.WriteString(w, ","); err != nil {
				return
			}
		}
		encoded, err := json.Marshal(matchJSON(
			first,
			query.ResolveHashes,
			stringQuantities,
		))
		if err != nil {
			s.logger.Error("encode Kupo match", "error", err)
			return
		}
		if _, err := w.Write(encoded); err != nil {
			return
		}
		written++
		if written%128 == 0 {
			if err := http.NewResponseController(w).Flush(); err != nil {
				return
			}
		}
		first, ok, err = matches.Next()
		if err != nil {
			if r.Context().Err() == nil {
				s.logger.Error("stream Kupo matches", "error", err)
			}
			return
		}
	}
	_, _ = io.WriteString(w, "]\n")
}

func parseMatchQuery(r *http.Request) (MatchQuery, error) {
	values := r.URL.Query()
	allowed := map[string]bool{
		"resolve_hashes": true,
		"spent":          true,
		"unspent":        true,
		"order":          true,
		"created_after":  true,
		"created_before": true,
		"spent_after":    true,
		"spent_before":   true,
		"policy_id":      true,
		"asset_name":     true,
		"transaction_id": true,
		"output_index":   true,
	}
	if err := rejectUnknownQuery(values, allowed); err != nil {
		return MatchQuery{}, err
	}
	if err := validateQueryValues(
		values,
		map[string]bool{
			"resolve_hashes": true,
			"spent":          true,
			"unspent":        true,
		},
		map[string]bool{"asset_name": true},
	); err != nil {
		return MatchQuery{}, err
	}
	ret := MatchQuery{Pattern: requestPattern(r)}
	if ret.Pattern == "" {
		ret.Pattern = "*"
	}
	ret.ResolveHashes = hasFlag(values, "resolve_hashes")
	spent, unspent := hasFlag(values, "spent"), hasFlag(values, "unspent")
	if spent && unspent {
		return ret, fmt.Errorf(
			"%w: spent and unspent are mutually exclusive",
			ErrInvalidRequest,
		)
	}
	if spent {
		ret.Status = MatchStatusSpent
	} else if unspent {
		ret.Status = MatchStatusUnspent
	}
	switch values.Get("order") {
	case "", "most_recent_first":
	case "oldest_first":
		ret.OldestFirst = true
	default:
		return ret, fmt.Errorf("%w: invalid order", ErrInvalidRequest)
	}
	var err error
	if ret.CreatedAfter, err = parsePointSelector(values.Get("created_after")); err != nil {
		return ret, err
	}
	if ret.CreatedBefore, err = parsePointSelector(values.Get("created_before")); err != nil {
		return ret, err
	}
	if ret.SpentAfter, err = parsePointSelector(values.Get("spent_after")); err != nil {
		return ret, err
	}
	if ret.SpentBefore, err = parsePointSelector(values.Get("spent_before")); err != nil {
		return ret, err
	}
	if ret.CreatedAfter != nil && ret.SpentAfter != nil {
		return ret, fmt.Errorf("%w: multiple lower bounds", ErrInvalidRequest)
	}
	if ret.CreatedBefore != nil && ret.SpentBefore != nil {
		return ret, fmt.Errorf("%w: multiple upper bounds", ErrInvalidRequest)
	}
	if value := values.Get("policy_id"); value != "" {
		ret.PolicyID, err = decodeHexSize(value, 28, "policy_id")
		if err != nil {
			return ret, err
		}
	}
	if assetNames, ok := values["asset_name"]; ok {
		if len(ret.PolicyID) == 0 {
			return ret, fmt.Errorf(
				"%w: asset_name requires policy_id",
				ErrInvalidRequest,
			)
		}
		ret.AssetName, err = hex.DecodeString(assetNames[0])
		if err != nil || len(ret.AssetName) > 32 {
			return ret, fmt.Errorf("%w: invalid asset_name", ErrInvalidRequest)
		}
	}
	if value := values.Get("transaction_id"); value != "" {
		ret.TransactionID, err = decodeHexSize(value, 32, "transaction_id")
		if err != nil {
			return ret, err
		}
	}
	if value := values.Get("output_index"); value != "" {
		if len(ret.TransactionID) == 0 {
			return ret, fmt.Errorf(
				"%w: output_index requires transaction_id",
				ErrInvalidRequest,
			)
		}
		parsed, parseErr := strconv.ParseUint(value, 10, 32)
		if parseErr != nil {
			return ret, fmt.Errorf(
				"%w: invalid output_index",
				ErrInvalidRequest,
			)
		}
		idx := uint32(parsed)
		ret.OutputIndex = &idx
	}
	if len(ret.PolicyID) > 0 && len(ret.TransactionID) > 0 {
		return ret, fmt.Errorf(
			"%w: policy and transaction filters are mutually exclusive",
			ErrInvalidRequest,
		)
	}
	return ret, nil
}

func parsePointSelector(value string) (*PointSelector, error) {
	if value == "" {
		return nil, nil
	}
	parts := strings.Split(value, ".")
	if len(parts) > 2 {
		return nil, fmt.Errorf("%w: invalid point", ErrInvalidRequest)
	}
	slot, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid point slot", ErrInvalidRequest)
	}
	ret := &PointSelector{SlotNo: slot}
	if len(parts) == 2 {
		if _, err := decodeHexSize(parts[1], 32, "point header hash"); err != nil {
			return nil, err
		}
		ret.HeaderHash = strings.ToLower(parts[1])
	}
	return ret, nil
}

func matchJSON(match Match, resolve, stringQuantities bool) map[string]any {
	coins := any(match.Value.Coins)
	assets := make(map[string]any, len(match.Value.Assets))
	for asset, quantity := range match.Value.Assets {
		assets[asset] = quantity
	}
	if stringQuantities {
		coins = strconv.FormatUint(match.Value.Coins, 10)
		for asset, quantity := range match.Value.Assets {
			assets[asset] = strconv.FormatUint(quantity, 10)
		}
	}
	value := map[string]any{"coins": coins}
	if len(assets) > 0 {
		value["assets"] = assets
	}
	ret := map[string]any{
		"transaction_index": match.TransactionIndex,
		"transaction_id":    match.TransactionID,
		"output_index":      match.OutputIndex,
		"address":           match.Address,
		"value":             value,
		"datum_hash":        match.DatumHash,
		"script_hash":       match.ScriptHash,
		"created_at":        match.CreatedAt,
		"spent_at":          match.SpentAt,
	}
	if match.DatumType != "" {
		ret["datum_type"] = match.DatumType
	}
	if resolve {
		ret["datum"] = match.Datum
		ret["script"] = match.Script
	}
	return ret
}

func (s *Server) handleDatum(w http.ResponseWriter, r *http.Request) {
	hash, err := decodeHexSize(r.PathValue("datum_hash"), 32, "datum hash")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	datum, tip, err := s.node.Datum(r.Context(), hash)
	if err != nil {
		s.writeNodeError(w, err)
		return
	}
	if notModifiedAt(w, r, tip) {
		return
	}
	if datum == nil {
		writeError(w, http.StatusNotFound, ErrNotFound)
		return
	}
	writeJSON(w, http.StatusOK, datum)
}

func (s *Server) handleScript(w http.ResponseWriter, r *http.Request) {
	hash, err := decodeHexSize(r.PathValue("script_hash"), 28, "script hash")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	script, tip, err := s.node.Script(r.Context(), hash)
	if err != nil {
		s.writeNodeError(w, err)
		return
	}
	if notModifiedAt(w, r, tip) {
		return
	}
	if script == nil {
		writeError(w, http.StatusNotFound, ErrNotFound)
		return
	}
	writeJSON(w, http.StatusOK, script)
}

func (s *Server) handlePatterns(w http.ResponseWriter, r *http.Request) {
	if s.notModified(w, r) {
		return
	}
	writeJSON(w, http.StatusOK, []string{"*"})
}

func (s *Server) handlePattern(w http.ResponseWriter, r *http.Request) {
	if err := validatePatternText(requestPattern(r)); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, []string{"*"})
}

func (s *Server) handlePutPatterns(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Patterns []string `json:"patterns"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			fmt.Errorf("%w: invalid JSON body", ErrInvalidRequest),
		)
		return
	}
	for _, pattern := range body.Patterns {
		if err := validatePatternText(pattern); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
	}
	writeJSON(w, http.StatusOK, []string{"*"})
}

func (s *Server) handlePutPattern(w http.ResponseWriter, r *http.Request) {
	if err := validatePatternText(requestPattern(r)); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, []string{"*"})
}

func (s *Server) handleDeletePattern(w http.ResponseWriter, _ *http.Request) {
	writeError(
		w,
		http.StatusBadRequest,
		fmt.Errorf(
			"%w: Dingo's global pattern cannot be removed",
			ErrInvalidRequest,
		),
	)
}

func (s *Server) handleDeleteMatches(w http.ResponseWriter, _ *http.Request) {
	writeError(
		w,
		http.StatusBadRequest,
		fmt.Errorf(
			"%w: Dingo's indexed chain history cannot be deleted through Kupo",
			ErrInvalidRequest,
		),
	)
}

func (s *Server) handleCheckpoints(w http.ResponseWriter, r *http.Request) {
	points, tip, err := s.node.Checkpoints(r.Context())
	if err != nil {
		s.writeNodeError(w, err)
		return
	}
	if notModifiedAt(w, r, tip) {
		return
	}
	writeJSON(w, http.StatusOK, points)
}

func (s *Server) handleCheckpoint(w http.ResponseWriter, r *http.Request) {
	if err := rejectUnknownQuery(
		r.URL.Query(),
		map[string]bool{"strict": true},
	); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := validateQueryValues(
		r.URL.Query(),
		map[string]bool{"strict": true},
		nil,
	); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	slot, err := strconv.ParseUint(r.PathValue("slot_no"), 10, 64)
	if err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			fmt.Errorf("%w: invalid slot number", ErrInvalidRequest),
		)
		return
	}
	point, tip, err := s.node.Checkpoint(
		r.Context(),
		slot,
		hasFlag(r.URL.Query(), "strict"),
	)
	if err != nil {
		s.writeNodeError(w, err)
		return
	}
	if notModifiedAt(w, r, tip) {
		return
	}
	writeJSON(w, http.StatusOK, point)
}

func (s *Server) handleMetadata(w http.ResponseWriter, r *http.Request) {
	if err := rejectUnknownQuery(
		r.URL.Query(),
		map[string]bool{"transaction_id": true},
	); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := validateQueryValues(r.URL.Query(), nil, nil); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	slot, err := strconv.ParseUint(r.PathValue("slot_no"), 10, 64)
	if err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			fmt.Errorf("%w: invalid slot number", ErrInvalidRequest),
		)
		return
	}
	var transactionID []byte
	if value := r.URL.Query().Get("transaction_id"); value != "" {
		transactionID, err = decodeHexSize(value, 32, "transaction_id")
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
	}
	metadata, blockHash, tip, err := s.node.Metadata(
		r.Context(),
		slot,
		transactionID,
	)
	if err != nil {
		s.writeNodeError(w, err)
		return
	}
	if notModifiedAt(w, r, tip) {
		return
	}
	if blockHash != "" {
		w.Header().Set("X-Block-Header-Hash", blockHash)
	}
	writeJSON(w, http.StatusOK, metadata)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.handleHealthResponse(w, r, false)
}

func (s *Server) handleHealthResponse(
	w http.ResponseWriter,
	r *http.Request,
	alwaysOK bool,
) {
	health, tip, status, err := s.node.Health()
	if err != nil {
		s.writeNodeError(w, err)
		return
	}
	if alwaysOK {
		status = http.StatusOK
	}
	setTipHeaders(w, tip)
	accept := r.Header.Get("Accept")
	switch {
	case strings.Contains(accept, "text/plain"),
		strings.Contains(accept, "*/*"):
		writeMetrics(w, status, health)
	case accept == "", strings.Contains(accept, "application/json"):
		writeJSON(w, status, health)
	default:
		writeError(
			w,
			http.StatusBadRequest,
			fmt.Errorf(
				"%w: unsupported Accept header; expected application/json or text/plain",
				ErrInvalidRequest,
			),
		)
	}
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	s.handleHealthResponse(w, r, true)
}

func writeMetrics(w http.ResponseWriter, status int, health Health) {
	w.Header().Set("Content-Type", "text/plain;charset=utf-8")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w,
		"# TYPE kupo_configuration_indexes gauge\n"+
			"kupo_configuration_indexes 1.0\n\n"+
			"# TYPE kupo_connection_status gauge\n"+
			"kupo_connection_status %d.0\n",
		boolMetric(health.ConnectionStatus == "connected"),
	)
	if health.MostRecentCheckpoint != nil {
		_, _ = fmt.Fprintf(w,
			"\n# TYPE kupo_most_recent_checkpoint counter\n"+
				"kupo_most_recent_checkpoint %d\n",
			*health.MostRecentCheckpoint,
		)
	}
	if health.MostRecentNodeTip != nil {
		_, _ = fmt.Fprintf(w,
			"\n# TYPE kupo_most_recent_node_tip counter\n"+
				"kupo_most_recent_node_tip %d\n",
			*health.MostRecentNodeTip,
		)
	}
	if health.NetworkSynchronization != nil {
		_, _ = fmt.Fprintf(w,
			"\n# TYPE kupo_network_synchronization gauge\n"+
				"kupo_network_synchronization %g\n",
			*health.NetworkSynchronization,
		)
	}
	if health.SecondsSinceLastBlock != nil {
		_, _ = fmt.Fprintf(w,
			"\n# TYPE kupo_seconds_since_last_block gauge\n"+
				"kupo_seconds_since_last_block %d.0\n",
			*health.SecondsSinceLastBlock,
		)
	}
}

func (s *Server) notModified(w http.ResponseWriter, r *http.Request) bool {
	tip, err := s.node.Tip()
	if err != nil {
		s.writeNodeError(w, err)
		return true
	}
	return notModifiedAt(w, r, tip)
}

func notModifiedAt(w http.ResponseWriter, r *http.Request, tip Point) bool {
	setTipHeaders(w, tip)
	tipHash := w.Header().Get("ETag")
	requestETag := r.Header.Get("If-None-Match")
	if tipHash != "" && requestETag != "" && requestETag == tipHash {
		w.WriteHeader(http.StatusNotModified)
		return true
	}
	return false
}

func setTipHeaders(w http.ResponseWriter, tip Point) {
	w.Header().
		Set("X-Most-Recent-Checkpoint", strconv.FormatUint(tip.SlotNo, 10))
	if tip.HeaderHash != "" {
		w.Header().Set("ETag", tip.HeaderHash)
	}
}

func (s *Server) handleNotFound(w http.ResponseWriter, r *http.Request) {
	if knownKupoPath(r.URL.Path) {
		writeError(w, http.StatusNotAcceptable, ErrInvalidRequest)
		return
	}
	writeError(w, http.StatusNotFound, ErrNotFound)
}

func knownKupoPath(path string) bool {
	if strings.HasPrefix(path, "/v1/") {
		path = strings.TrimPrefix(path, "/v1")
	}
	if path == "/matches" || path == "/patterns" ||
		path == "/checkpoints" || path == "/health" ||
		path == "/metrics" {
		return true
	}
	for _, prefix := range []string{
		"/matches/", "/datums/", "/scripts/", "/patterns/",
		"/checkpoints/", "/metadata/",
	} {
		if !strings.HasPrefix(path, prefix) {
			continue
		}
		remainder := strings.TrimPrefix(path, prefix)
		maxSegments := 1
		if prefix == "/matches/" || prefix == "/patterns/" {
			maxSegments = 2
		}
		if remainder != "" &&
			len(strings.Split(remainder, "/")) <= maxSegments {
			return true
		}
	}
	return false
}

func (s *Server) writeNodeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	if errors.Is(err, ErrInvalidRequest) {
		status = http.StatusBadRequest
	} else if errors.Is(err, ErrNotFound) {
		status = http.StatusNotFound
	}
	if status == http.StatusInternalServerError {
		s.logger.Error("Kupo request failed", "error", err)
	}
	writeError(w, status, err)
}

func validatePatternText(pattern string) error {
	_, err := parsePattern(pattern)
	return err
}

func requestPattern(r *http.Request) string {
	if payment := r.PathValue("payment"); payment != "" {
		return payment + "/" + r.PathValue("delegation")
	}
	return r.PathValue("pattern")
}

func decodeHexSize(value string, size int, name string) ([]byte, error) {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != size {
		return nil, fmt.Errorf("%w: invalid %s", ErrInvalidRequest, name)
	}
	return decoded, nil
}

func hasFlag(values url.Values, name string) bool {
	_, ok := values[name]
	return ok
}

func rejectUnknownQuery(values url.Values, allowed map[string]bool) error {
	for name := range values {
		if !allowed[name] {
			return fmt.Errorf(
				"%w: unknown query parameter %q",
				ErrInvalidRequest,
				name,
			)
		}
	}
	return nil
}

func validateQueryValues(
	values url.Values,
	flags map[string]bool,
	allowEmpty map[string]bool,
) error {
	for name, entries := range values {
		if len(entries) != 1 {
			return fmt.Errorf(
				"%w: query parameter %s must occur once",
				ErrInvalidRequest,
				name,
			)
		}
		if flags[name] {
			if entries[0] != "" {
				return fmt.Errorf(
					"%w: query flag %s does not take a value",
					ErrInvalidRequest,
					name,
				)
			}
			continue
		}
		if entries[0] == "" && !allowEmpty[name] {
			return fmt.Errorf(
				"%w: query parameter %s requires a value",
				ErrInvalidRequest,
				name,
			)
		}
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", jsonContentType)
	}
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		return
	}
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"hint": err.Error()})
}

func boolMetric(value bool) uint8 {
	if value {
		return 1
	}
	return 0
}
