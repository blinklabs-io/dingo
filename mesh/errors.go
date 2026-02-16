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

// Error represents a Mesh API error response.
type Error struct {
	Code        int32          `json:"code"`
	Message     string         `json:"message"`
	Description string         `json:"description,omitempty"`
	Retriable   bool           `json:"retriable"`
	Details     map[string]any `json:"details,omitempty"`
}

// Standard Mesh API error codes following the specification.
var (
	ErrNetworkNotSupported = &Error{
		Code:    1,
		Message: "network not supported",
		Description: "The requested network is not supported " +
			"by this server.",
		Retriable: false,
	}
	ErrBlockNotFound = &Error{
		Code:        2,
		Message:     "block not found",
		Description: "The requested block could not be found.",
		Retriable:   false,
	}
	ErrTransactionNotFound = &Error{
		Code:    3,
		Message: "transaction not found",
		Description: "The requested transaction could " +
			"not be found.",
		Retriable: false,
	}
	ErrAccountNotFound = &Error{
		Code:    4,
		Message: "account not found",
		Description: "The requested account could not " +
			"be found.",
		Retriable: false,
	}
	ErrInvalidRequest = &Error{
		Code:        5,
		Message:     "invalid request",
		Description: "The request was invalid or malformed.",
		Retriable:   false,
	}
	ErrInternal = &Error{
		Code:        6,
		Message:     "internal error",
		Description: "An internal server error occurred.",
		Retriable:   true,
	}
	ErrNotImplemented = &Error{
		Code:        7,
		Message:     "not implemented",
		Description: "This endpoint is not yet implemented.",
		Retriable:   false,
	}
	ErrInvalidPublicKey = &Error{
		Code:        8,
		Message:     "invalid public key",
		Description: "The provided public key is invalid.",
		Retriable:   false,
	}
	ErrInvalidTransaction = &Error{
		Code:    9,
		Message: "invalid transaction",
		Description: "The provided transaction is " +
			"invalid or malformed.",
		Retriable: false,
	}
	ErrSubmitFailed = &Error{
		Code:    10,
		Message: "transaction submit failed",
		Description: "The transaction could not be " +
			"submitted to the network.",
		Retriable: true,
	}
	ErrUnavailable = &Error{
		Code:    11,
		Message: "service unavailable",
		Description: "The service is temporarily " +
			"unavailable. Please retry.",
		Retriable: true,
	}
)

// AllErrors returns all defined error types for the
// /network/options response.
func AllErrors() []*Error {
	return []*Error{
		ErrNetworkNotSupported,
		ErrBlockNotFound,
		ErrTransactionNotFound,
		ErrAccountNotFound,
		ErrInvalidRequest,
		ErrInternal,
		ErrNotImplemented,
		ErrInvalidPublicKey,
		ErrInvalidTransaction,
		ErrSubmitFailed,
		ErrUnavailable,
	}
}

// wrapErr creates a new Error with additional details.
func wrapErr(base *Error, detail error) *Error {
	if detail == nil {
		return base
	}
	return &Error{
		Code:        base.Code,
		Message:     base.Message,
		Description: base.Description,
		Retriable:   base.Retriable,
		Details: map[string]any{
			"error": detail.Error(),
		},
	}
}
