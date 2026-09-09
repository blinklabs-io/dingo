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

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

type listPaginationNode struct {
	*mockNode
	calls  int
	params PaginationParams
}

func (n *listPaginationNode) PoolsExtended() ([]PoolExtendedInfo, error) {
	n.calls++
	return []PoolExtendedInfo{}, nil
}

func (n *listPaginationNode) AccountAssociatedAddresses(
	_ string,
	params PaginationParams,
) ([]AccountAssociatedAddressInfo, int, error) {
	n.calls++
	n.params = params
	return []AccountAssociatedAddressInfo{}, 0, nil
}

func (n *listPaginationNode) AccountDelegationHistory(
	_ string,
	params PaginationParams,
) ([]AccountDelegationHistoryInfo, int, error) {
	n.calls++
	n.params = params
	return []AccountDelegationHistoryInfo{}, 0, nil
}

func (n *listPaginationNode) AccountRegistrationHistory(
	_ string,
	params PaginationParams,
) ([]AccountRegistrationHistoryInfo, int, error) {
	n.calls++
	n.params = params
	return []AccountRegistrationHistoryInfo{}, 0, nil
}

func (n *listPaginationNode) AccountRewardHistory(
	_ string,
	params PaginationParams,
) ([]AccountRewardHistoryInfo, int, error) {
	n.calls++
	n.params = params
	return []AccountRewardHistoryInfo{}, 0, nil
}

func (n *listPaginationNode) AccountUTXOs(
	_ string,
	params PaginationParams,
) ([]AccountUTXOInfo, int, error) {
	n.calls++
	n.params = params
	return []AccountUTXOInfo{}, 0, nil
}

func (n *listPaginationNode) AccountWithdrawals(
	_ string,
	params PaginationParams,
) ([]AccountWithdrawalInfo, int, error) {
	n.calls++
	n.params = params
	return []AccountWithdrawalInfo{}, 0, nil
}

func (n *listPaginationNode) AccountTransactions(
	_ string,
	params AccountTransactionsParams,
) ([]AccountTransactionInfo, int, error) {
	n.calls++
	n.params = params.Pagination
	return []AccountTransactionInfo{}, 0, nil
}

func TestListRoutesRejectOutOfRangePagination(t *testing.T) {
	routes := []string{
		"/api/v0/pools/extended",
		"/api/v0/accounts/stake_test1/addresses",
		"/api/v0/accounts/stake_test1/delegations",
		"/api/v0/accounts/stake_test1/registrations",
		"/api/v0/accounts/stake_test1/rewards",
		"/api/v0/accounts/stake_test1/utxos",
		"/api/v0/accounts/stake_test1/withdrawals",
		"/api/v0/accounts/stake_test1/transactions",
	}
	for _, route := range routes {
		t.Run(route, func(t *testing.T) {
			for _, query := range []string{
				"count=0", "count=-1", "count=101", "count=abc",
				"page=0", "page=-1", "page=21474837", "page=abc",
				"order=sideways",
			} {
				t.Run(query, func(t *testing.T) {
					node := &listPaginationNode{mockNode: &mockNode{}}
					b := newTestBlockfrost(node)
					recorder := httptest.NewRecorder()
					request := httptest.NewRequest(
						http.MethodGet, route+"?"+query, nil,
					)
					b.handler().ServeHTTP(recorder, request)
					require.Zero(
						t,
						node.calls,
						"invalid pagination must not reach the adapter",
					)
					require.Equal(t, http.StatusBadRequest, recorder.Code)
					require.JSONEq(
						t,
						`{"status_code":400,"error":"Bad Request","message":"Invalid pagination parameters."}`,
						recorder.Body.String(),
					)
				})
			}
			for _, tc := range []struct {
				name  string
				query string
				want  PaginationParams
			}{
				{"defaults", "", PaginationParams{Count: 100, Page: 1, Order: "asc"}},
				{"minimum", "count=1&page=1&order=asc", PaginationParams{Count: 1, Page: 1, Order: "asc"}},
				{"maximum", "count=100&page=21474836&order=desc", PaginationParams{Count: 100, Page: 21474836, Order: "desc"}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					node := &listPaginationNode{mockNode: &mockNode{}}
					b := newTestBlockfrost(node)
					recorder := httptest.NewRecorder()
					request := httptest.NewRequest(
						http.MethodGet, route+"?"+tc.query, nil,
					)
					b.handler().ServeHTTP(recorder, request)
					require.Equal(t, http.StatusOK, recorder.Code)
					require.Equal(t, 1, node.calls)
					if route != "/api/v0/pools/extended" {
						require.Equal(t, tc.want, node.params)
					}
					require.JSONEq(t, `[]`, recorder.Body.String())
				})
			}
		})
	}
}
