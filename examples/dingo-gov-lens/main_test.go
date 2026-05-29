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

package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestActionTypeName(t *testing.T) {
	tests := map[int64]string{
		0: "Parameter Change",
		1: "Hard Fork Initiation",
		2: "Treasury Withdrawal",
		3: "No Confidence",
		4: "Update Committee",
		5: "New Constitution",
		6: "Info",
	}
	for input, expected := range tests {
		if got := actionTypeName(input); got != expected {
			t.Fatalf("actionTypeName(%d) = %q, want %q", input, got, expected)
		}
	}
}

func TestGovtoolActionURL(t *testing.T) {
	got := govtoolActionURL(
		"https://preview.gov.tools",
		"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		2,
	)
	want := "https://preview.gov.tools/governance_actions/0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef#2"
	if got != want {
		t.Fatalf("govtoolActionURL() = %q, want %q", got, want)
	}
}

func TestIsHex(t *testing.T) {
	if !isHex("abcdef012345", 12) {
		t.Fatal("expected valid lowercase hex")
	}
	if isHex("ABCDEF012345", 12) {
		t.Fatal("expected uppercase hex to be rejected")
	}
	if isHex("abcdef012345", 10) {
		t.Fatal("expected mismatched length to be rejected")
	}
}

func TestVoteBackfillPending(t *testing.T) {
	if !voteBackfillPending(0, 110_000_000, &backfillStatus{
		LastSlot:  2_000_000,
		Completed: false,
	}) {
		t.Fatal("expected vote backfill to be pending")
	}
	if voteBackfillPending(1, 110_000_000, &backfillStatus{
		LastSlot:  2_000_000,
		Completed: false,
	}) {
		t.Fatal("votes already present should not be pending")
	}
	if voteBackfillPending(0, 110_000_000, &backfillStatus{
		LastSlot:  110_000_000,
		Completed: false,
	}) {
		t.Fatal("backfill at proposal slot should not be pending")
	}
	if voteBackfillPending(0, 110_000_000, &backfillStatus{
		LastSlot:  2_000_000,
		Completed: true,
	}) {
		t.Fatal("completed backfill should not be pending")
	}
}

func TestServerErrorDoesNotExposeInternalError(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/proposals", nil)
	rec := httptest.NewRecorder()

	serverError(rec, req, "query proposals", errors.New("database password=secret failed"))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
	body := rec.Body.String()
	if strings.Contains(body, "password=secret") || strings.Contains(body, "query proposals") {
		t.Fatalf("serverError exposed internal details: %q", body)
	}
	if !strings.Contains(body, "internal server error") {
		t.Fatalf("serverError body = %q, want generic error", body)
	}
}
