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

//go:generate mockgen -source=$GOFILE -package=mock -destination=./mock/$GOFILE
package v2

import (
	"context"

	"github.com/bucketeer-io/bucketeer/v2/pkg/email/domain"
	pkgErr "github.com/bucketeer-io/bucketeer/v2/pkg/error"
)

// Package name for domain errors.
const emailPackageName = pkgErr.EmailPackageName

// nolint:lll
var (
	ErrDeliveryAlreadyExists = pkgErr.NewErrorAlreadyExists(
		emailPackageName, "email delivery already exists")
	ErrDeliveryNotFound = pkgErr.NewErrorNotFound(
		emailPackageName, "email delivery not found", "email_delivery")
	ErrAttemptAlreadyExists = pkgErr.NewErrorAlreadyExists(
		emailPackageName, "email delivery attempt already exists")
	ErrDeliveryUnexpectedAffectedRows = pkgErr.NewErrorUnexpectedAffectedRows(
		emailPackageName, "email delivery unexpected affected rows")
)

// DeliveryStorage persists email deliveries and their provider attempts.
//
// Every write required to converge a delivery (the delivery row, the attempt
// row and the provider receipt) is recorded through this interface before the
// source message is acknowledged.
type DeliveryStorage interface {
	// CreateDelivery inserts a new delivery.
	CreateDelivery(ctx context.Context, d *domain.Delivery) error
	// UpdateDelivery persists the current delivery state.
	UpdateDelivery(ctx context.Context, d *domain.Delivery) error
	// GetDelivery returns the delivery with the given stable identity.
	GetDelivery(ctx context.Context, id string) (*domain.Delivery, error)
	// CreateAttempt records a provider attempt. The row is created before the
	// provider is contacted.
	CreateAttempt(ctx context.Context, a *domain.Attempt) error
	// UpdateAttempt persists the attempt result and the provider receipt.
	UpdateAttempt(ctx context.Context, a *domain.Attempt) error
	// ListAttempts returns every attempt of a delivery ordered by sequence.
	ListAttempts(ctx context.Context, deliveryID string) ([]*domain.Attempt, error)
	// AcquireLease grants an exclusive lease to converge a delivery. It
	// succeeds when the delivery has no active lease or the caller already
	// owns it, and fails for every other caller, so concurrent messages or
	// pods can never create two provider attempts for the same delivery.
	AcquireLease(ctx context.Context, id, owner string, ttlSec, now int64) (bool, error)
	// ReleaseLease releases a lease owned by the caller.
	ReleaseLease(ctx context.Context, id, owner string) error
	// ListDueDeliveries returns non-terminal deliveries whose NextRetryAt is
	// due and that are not actively leased, so the internal scheduler can
	// converge them independently of message redelivery.
	ListDueDeliveries(ctx context.Context, now int64, limit int) ([]*domain.Delivery, error)
}
