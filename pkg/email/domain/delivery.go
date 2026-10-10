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

// Package domain contains the domain model for idempotent email delivery.
//
// A Delivery is the single logical email derived from an account creation
// domain event. Its identity is deterministic, so every redelivery of the
// same event (Pub/Sub, Redis Stream or a claimed message after a restart)
// converges to the same Delivery and at most one email is sent.
package domain

import (
	"errors"
	"strconv"
)

// DeliveryIDPrefix is the prefix for the stable delivery identity.
const DeliveryIDPrefix = "welcome-"

// DeriveDeliveryID returns the stable delivery identity derived from the
// account creation domain event ID. It never changes regardless of the
// transport that delivers the event or how many times it is redelivered.
func DeriveDeliveryID(eventID string) string {
	return DeliveryIDPrefix + eventID
}

// DeliveryStatus is the state of a Delivery.
type DeliveryStatus int

const (
	// DeliveryStatusPending means the delivery was created but rendering and
	// provider communication have not happened yet.
	DeliveryStatusPending DeliveryStatus = 0
	// DeliveryStatusRenderFailed means template rendering failed. The failure
	// is considered repairable: once the template is fixed the delivery can
	// be processed again. No provider attempt has started.
	DeliveryStatusRenderFailed DeliveryStatus = 1
	// DeliveryStatusBackoff means the provider returned an explicit temporary
	// failure. The delivery will be retried with the same provider after
	// NextRetryAt.
	DeliveryStatusBackoff DeliveryStatus = 2
	// DeliveryStatusPendingConfirmation means the result is uncertain (e.g.
	// timeout, connection error). The provider may already have accepted the
	// message, so it must be queried or retried with the original delivery
	// identity before sending through another provider.
	DeliveryStatusPendingConfirmation DeliveryStatus = 3
	// DeliveryStatusAccepted is terminal: at least one provider accepted the
	// message and the receipt was recorded. No failover is allowed anymore.
	DeliveryStatusAccepted DeliveryStatus = 4
	// DeliveryStatusRejected is terminal: every available provider explicitly
	// rejected the message and there is no backup provider left.
	DeliveryStatusRejected DeliveryStatus = 5
	// DeliveryStatusCanceled is terminal: the account was deleted or the
	// recipient address changed before the first provider attempt started.
	DeliveryStatusCanceled DeliveryStatus = 6
)

// Terminal reports whether the status is a terminal state.
func (s DeliveryStatus) Terminal() bool {
	switch s {
	case DeliveryStatusAccepted, DeliveryStatusRejected, DeliveryStatusCanceled:
		return true
	default:
		return false
	}
}

// AttemptResult is the result of a single provider attempt.
type AttemptResult int

const (
	// AttemptResultInProgress means the attempt row was recorded but the
	// provider response has not been recorded yet (e.g. crash in between).
	AttemptResultInProgress AttemptResult = 0
	// AttemptResultAccepted means the provider explicitly accepted the message.
	AttemptResultAccepted AttemptResult = 1
	// AttemptResultRejected means the provider explicitly and permanently
	// rejected the message; failover to a backup provider is allowed.
	AttemptResultRejected AttemptResult = 2
	// AttemptResultRetryable means the provider returned an explicit temporary
	// failure; the same provider is retried after backoff.
	AttemptResultRetryable AttemptResult = 3
	// AttemptResultUnknown means the response is uncertain; the provider may
	// have accepted the message, so the original identity must be resolved
	// first (query or retry) before any failover.
	AttemptResultUnknown AttemptResult = 4
)

// Snapshot is the frozen delivery snapshot. Once the first provider attempt
// has started, the delivery converges with this snapshot even if the account
// is deleted or the address changes afterwards.
type Snapshot struct {
	// To is the frozen recipient address.
	To string `json:"to"`
	// RequestedLanguage is the language requested by the event.
	RequestedLanguage string `json:"requestedLanguage"`
	// Language is the resolved language actually used for the template
	// (after the unknown-language fallback).
	Language string `json:"language"`
	// TemplateVersion is the frozen template version.
	TemplateVersion string `json:"templateVersion"`
	// Subject is the rendered subject, frozen at render time.
	Subject string `json:"subject"`
	// Body is the rendered HTML body, frozen at render time.
	Body string `json:"body"`
}

// Cancel reasons.
const (
	// CancelReasonAccountNotFound is used when the account no longer exists
	// at the frozen address: it was deleted or the address was changed.
	CancelReasonAccountNotFound = "account_not_found"
)

// Sentinel errors for illegal state transitions.
var (
	ErrIllegalTransition = errors.New("email domain: illegal state transition")
)

// Delivery is a single logical email derived from one account creation event.
type Delivery struct {
	// ID is the stable delivery identity, derived from the event ID.
	ID string
	// EventID is the source account creation domain event ID.
	EventID string
	// OrganizationID is the organization the account belongs to.
	OrganizationID string
	// EventEmail is the recipient address requested by the event. It is kept
	// even if rendering fails, so a repaired render can be retried by the
	// scheduler without a redelivered message.
	EventEmail string
	// EventLanguage is the language requested by the event.
	EventLanguage string
	// Status is the current delivery status.
	Status DeliveryStatus
	// Snapshot is the frozen recipient/template snapshot. Nil until rendered.
	Snapshot *Snapshot
	// AttemptCount is the number of provider attempts ever created.
	AttemptCount int
	// NextRetryAt is the earliest epoch second at which processing may resume.
	// Zero means it can be processed immediately.
	NextRetryAt int64
	// LastProvider is the provider used by the most recent attempt.
	LastProvider string
	// LastOutcome is the result of the most recent attempt.
	LastOutcome AttemptResult
	// LastErrorCode is a short machine-readable error code.
	LastErrorCode string
	// LastErrorMessage is the human-readable error message.
	LastErrorMessage string
	// AcceptedProvider is the provider that accepted the message.
	AcceptedProvider string
	// ReceiptID is the provider-side receipt ID of the accepted message.
	ReceiptID string
	// CancelReason explains why the delivery was canceled.
	CancelReason string
	// LeaseOwner identifies the processor currently converging the delivery.
	LeaseOwner string
	// LeaseUntil is the epoch second until which the lease is valid.
	LeaseUntil int64
	// CreatedAt is the creation epoch second.
	CreatedAt int64
	// UpdatedAt is the last update epoch second.
	UpdatedAt int64
}

// NewDelivery creates a new pending delivery.
func NewDelivery(eventID, organizationID, eventEmail, eventLanguage string, now int64) *Delivery {
	return &Delivery{
		ID:             DeriveDeliveryID(eventID),
		EventID:        eventID,
		OrganizationID: organizationID,
		EventEmail:     eventEmail,
		EventLanguage:  eventLanguage,
		Status:         DeliveryStatusPending,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

// IsTerminal reports whether the delivery is in a terminal state.
func (d *Delivery) IsTerminal() bool {
	return d.Status.Terminal()
}

// HasStarted reports whether a provider attempt has ever been created. Once
// started, the delivery must converge with the frozen snapshot.
func (d *Delivery) HasStarted() bool {
	return d.AttemptCount > 0
}

// touch updates UpdatedAt.
func (d *Delivery) touch(now int64) {
	d.UpdatedAt = now
}

// Freeze stores the rendered snapshot. It is only allowed before the delivery
// is accepted or canceled.
func (d *Delivery) Freeze(s *Snapshot, now int64) error {
	if d.IsTerminal() {
		return ErrIllegalTransition
	}
	d.Snapshot = s
	if d.Status == DeliveryStatusPending || d.Status == DeliveryStatusRenderFailed {
		d.Status = DeliveryStatusPending
	}
	d.LastErrorCode = ""
	d.LastErrorMessage = ""
	d.touch(now)
	return nil
}

// MarkRenderFailed records a repairable template rendering failure.
func (d *Delivery) MarkRenderFailed(code, message string, retryAt, now int64) error {
	if d.IsTerminal() || d.HasStarted() {
		return ErrIllegalTransition
	}
	d.Status = DeliveryStatusRenderFailed
	d.NextRetryAt = retryAt
	d.LastProvider = ""
	d.LastOutcome = AttemptResultInProgress
	d.LastErrorCode = code
	d.LastErrorMessage = message
	d.touch(now)
	return nil
}

// MarkBackoff records an explicit temporary provider failure.
func (d *Delivery) MarkBackoff(provider, code, message string, retryAt, now int64) error {
	if d.IsTerminal() {
		return ErrIllegalTransition
	}
	d.Status = DeliveryStatusBackoff
	d.NextRetryAt = retryAt
	d.LastProvider = provider
	d.LastOutcome = AttemptResultRetryable
	d.LastErrorCode = code
	d.LastErrorMessage = message
	d.touch(now)
	return nil
}

// MarkPendingConfirmation records an uncertain provider response.
func (d *Delivery) MarkPendingConfirmation(provider, code, message string, retryAt, now int64) error {
	if d.IsTerminal() {
		return ErrIllegalTransition
	}
	d.Status = DeliveryStatusPendingConfirmation
	d.NextRetryAt = retryAt
	d.LastProvider = provider
	d.LastOutcome = AttemptResultUnknown
	d.LastErrorCode = code
	d.LastErrorMessage = message
	d.touch(now)
	return nil
}

// MarkRejected records a transient explicit rejection from a provider. The
// delivery remains processable so a backup provider can be tried.
func (d *Delivery) MarkRejected(provider, code, message string, now int64) error {
	if d.IsTerminal() {
		return ErrIllegalTransition
	}
	d.Status = DeliveryStatusPending
	d.NextRetryAt = 0
	d.LastProvider = provider
	d.LastOutcome = AttemptResultRejected
	d.LastErrorCode = code
	d.LastErrorMessage = message
	d.touch(now)
	return nil
}

// MarkAccepted records that a provider accepted the message. It is terminal,
// so no failover is allowed afterwards.
func (d *Delivery) MarkAccepted(provider, receiptID string, now int64) error {
	if d.Status == DeliveryStatusAccepted {
		return nil
	}
	if d.Status == DeliveryStatusRejected || d.Status == DeliveryStatusCanceled {
		return ErrIllegalTransition
	}
	d.Status = DeliveryStatusAccepted
	d.NextRetryAt = 0
	d.LastProvider = provider
	d.LastOutcome = AttemptResultAccepted
	d.LastErrorCode = ""
	d.LastErrorMessage = ""
	d.AcceptedProvider = provider
	d.ReceiptID = receiptID
	d.touch(now)
	return nil
}

// MarkTerminalRejected records that no provider could deliver the message.
// It is terminal.
func (d *Delivery) MarkTerminalRejected(message string, now int64) error {
	if d.IsTerminal() {
		return ErrIllegalTransition
	}
	d.Status = DeliveryStatusRejected
	d.NextRetryAt = 0
	d.LastOutcome = AttemptResultRejected
	d.LastErrorMessage = message
	d.touch(now)
	return nil
}

// Cancel marks the delivery as canceled. It is only allowed before a provider
// attempt has started.
func (d *Delivery) Cancel(reason string, now int64) error {
	if d.HasStarted() || d.Status == DeliveryStatusAccepted {
		return ErrIllegalTransition
	}
	d.Status = DeliveryStatusCanceled
	d.NextRetryAt = 0
	d.CancelReason = reason
	d.touch(now)
	return nil
}

// Attempt is a single provider communication recorded for a delivery.
type Attempt struct {
	// ID is the attempt identity, derived from the delivery ID and sequence.
	ID string
	// DeliveryID is the delivery the attempt belongs to.
	DeliveryID string
	// Seq is the 1-based attempt sequence within the delivery.
	Seq int
	// Provider is the provider used by the attempt.
	Provider string
	// Result is the attempt result.
	Result AttemptResult
	// ReceiptID is the provider-side receipt ID when accepted.
	ReceiptID string
	// StatusCode is the provider response status code, when available.
	StatusCode int
	// ErrorCode is a short machine-readable error code.
	ErrorCode string
	// ErrorMessage is the human-readable error message.
	ErrorMessage string
	// StartedAt is the epoch second the attempt started.
	StartedAt int64
	// FinishedAt is the epoch second the attempt finished. Zero while in progress.
	FinishedAt int64
	// CreatedAt is the creation epoch second.
	CreatedAt int64
}

// NewAttempt records a new in-progress attempt. The row must exist before the
// provider is contacted, so a crash always leaves a detectable in-flight
// attempt that is resolved with the original delivery identity.
func NewAttempt(deliveryID string, seq int, provider string, now int64) *Attempt {
	return &Attempt{
		ID:         attemptID(deliveryID, seq),
		DeliveryID: deliveryID,
		Seq:        seq,
		Provider:   provider,
		Result:     AttemptResultInProgress,
		StartedAt:  now,
		CreatedAt:  now,
	}
}

// attemptID derives a stable attempt identity.
func attemptID(deliveryID string, seq int) string {
	return deliveryID + "-a" + strconv.Itoa(seq)
}

// Accept records an accepted attempt.
func (a *Attempt) Accept(receiptID string, statusCode int, finishedAt int64) {
	a.Result = AttemptResultAccepted
	a.ReceiptID = receiptID
	a.StatusCode = statusCode
	a.FinishedAt = finishedAt
}

// Reject records an explicitly rejected attempt.
func (a *Attempt) Reject(code, message string, statusCode int, finishedAt int64) {
	a.Result = AttemptResultRejected
	a.ErrorCode = code
	a.ErrorMessage = message
	a.StatusCode = statusCode
	a.FinishedAt = finishedAt
}

// MarkRetryable records an explicit temporary failure.
func (a *Attempt) MarkRetryable(code, message string, statusCode int, finishedAt int64) {
	a.Result = AttemptResultRetryable
	a.ErrorCode = code
	a.ErrorMessage = message
	a.StatusCode = statusCode
	a.FinishedAt = finishedAt
}

// MarkUnknown records an uncertain result.
func (a *Attempt) MarkUnknown(code, message string, statusCode int, finishedAt int64) {
	a.Result = AttemptResultUnknown
	a.ErrorCode = code
	a.ErrorMessage = message
	a.StatusCode = statusCode
	a.FinishedAt = finishedAt
}


