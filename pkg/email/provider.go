// Copyright 2026 The Bucketeer Authors.
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

package email

import (
	"context"
	"errors"

	"github.com/bucketeer-io/bucketeer/v2/pkg/email/domain"
)

// errorRaw returns a short excerpt of the error message for receipt storage.
func errorRaw(err error, max int) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	if len(s) > max {
		return s[:max]
	}
	return s
}

// HeaderDeliveryID is the custom header carrying the stable delivery
// identity. Providers use it to de-duplicate redeliveries of the same
// logical email, and it stays identical across attempts and providers.
const HeaderDeliveryID = "X-Bucketeer-Delivery-ID"

// Machine-readable error codes recorded on deliveries and attempts.
const (
	// ErrorCodeRenderFailed means template rendering failed (repairable).
	ErrorCodeRenderFailed = "render_failed"
	// ErrorCodeRejected means the provider explicitly and permanently rejected
	// the message (failover allowed).
	ErrorCodeRejected = "rejected"
	// ErrorCodeTemporaryFailure means the provider returned an explicit
	// temporary failure (retried after backoff).
	ErrorCodeTemporaryFailure = "temporary_failure"
	// ErrorCodeUnknownResponse means the response is uncertain: the provider
	// may already have accepted the message.
	ErrorCodeUnknownResponse = "unknown_response"
)

// Outcome classifies the result of a provider attempt.
type Outcome int

const (
	// OutcomeAccepted means the provider explicitly accepted the message and
	// the receipt is available.
	OutcomeAccepted Outcome = iota
	// OutcomeRejected means an explicit permanent rejection. A backup provider
	// may be tried per the failover policy.
	OutcomeRejected
	// OutcomeRetryable means an explicit temporary failure. The same provider
	// is retried after backoff; failover is not triggered.
	OutcomeRetryable
	// OutcomeUnknown means the result is uncertain (timeout, connection error,
	// ambiguous response). The provider may have accepted the message, so the
	// original delivery identity must be queried or retried before any
	// failover.
	OutcomeUnknown
)

// Receipt is the provider-side receipt recorded when a message is accepted.
type Receipt struct {
	// MessageID is the provider's message identifier, when available.
	MessageID string
	// StatusCode is the provider response status code, when available.
	StatusCode int
	// Raw is a short excerpt of the raw provider response.
	Raw string
}

// SendRequest is a provider send using the frozen snapshot.
type SendRequest struct {
	// DeliveryID is the stable delivery identity.
	DeliveryID string
	// Snapshot is the frozen recipient/template snapshot.
	Snapshot *domain.Snapshot
}

// SendResult is the result of a provider send.
type SendResult struct {
	Outcome Outcome
	Receipt Receipt
	Err     error
}

// Provider is a low-level email provider.
type Provider interface {
	// Name returns the provider name (smtp, ses, sendgrid, mailersend).
	Name() string
	// Send sends the message described by the frozen snapshot.
	Send(ctx context.Context, req SendRequest) SendResult
}

// ReceiptQuerier is an optional capability of providers that can look up the
// state of a previously submitted message by its delivery identity. It is
// used to resolve uncertain outcomes. Providers that cannot query receipts
// return ErrReceiptQueryUnsupported and the delivery is retried with the
// original delivery identity instead.
type ReceiptQuerier interface {
	QueryReceipt(ctx context.Context, deliveryID string) (Outcome, Receipt, error)
}

// ErrReceiptQueryUnsupported is returned by providers unable to query a
// previously submitted message.
var ErrReceiptQueryUnsupported = errors.New("email: receipt query unsupported")
