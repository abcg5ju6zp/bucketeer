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

package mysql

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"time"

	"github.com/bucketeer-io/bucketeer/v2/pkg/email/domain"
	v2 "github.com/bucketeer-io/bucketeer/v2/pkg/email/storage/v2"
	mysqlstorage "github.com/bucketeer-io/bucketeer/v2/pkg/storage/v2/mysql"
)

const (
	// Column length limits, must match the migration.
	errorCodeMaxLen    = 64
	errorMessageMaxLen = 511
	receiptIDMaxLen    = 255
)

var (
	//go:embed sql/email_delivery/insert_delivery.sql
	insertDeliverySQL string
	//go:embed sql/email_delivery/update_delivery.sql
	updateDeliverySQL string
	//go:embed sql/email_delivery/select_delivery.sql
	selectDeliverySQL string
	//go:embed sql/email_delivery/insert_attempt.sql
	insertAttemptSQL string
	//go:embed sql/email_delivery/update_attempt.sql
	updateAttemptSQL string
	//go:embed sql/email_delivery/list_attempts.sql
	listAttemptsSQL string
	//go:embed sql/email_delivery/acquire_lease.sql
	acquireLeaseSQL string
	//go:embed sql/email_delivery/release_lease.sql
	releaseLeaseSQL string
	//go:embed sql/email_delivery/list_due_deliveries.sql
	listDueDeliveriesSQL string
)

type deliveryStorage struct {
	qe mysqlstorage.QueryExecer
}

// NewDeliveryStorage creates the MySQL implementation of DeliveryStorage.
func NewDeliveryStorage(qe mysqlstorage.QueryExecer) v2.DeliveryStorage {
	return &deliveryStorage{qe: qe}
}

func (s *deliveryStorage) CreateDelivery(ctx context.Context, d *domain.Delivery) error {
	_, err := s.qe.ExecContext(
		ctx,
		insertDeliverySQL,
		d.ID,
		d.EventID,
		d.OrganizationID,
		d.EventEmail,
		d.EventLanguage,
		int32(d.Status),
		snapshotArg(d.Snapshot),
		d.AttemptCount,
		d.NextRetryAt,
		d.LastProvider,
		int32(d.LastOutcome),
		truncate(d.LastErrorCode, errorCodeMaxLen),
		truncate(d.LastErrorMessage, errorMessageMaxLen),
		d.AcceptedProvider,
		truncate(d.ReceiptID, receiptIDMaxLen),
		d.CancelReason,
		d.LeaseOwner,
		d.LeaseUntil,
		d.CreatedAt,
		d.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, mysqlstorage.ErrDuplicateEntry) {
			return v2.ErrDeliveryAlreadyExists
		}
		return err
	}
	return nil
}

func (s *deliveryStorage) UpdateDelivery(ctx context.Context, d *domain.Delivery) error {
	result, err := s.qe.ExecContext(
		ctx,
		updateDeliverySQL,
		int32(d.Status),
		snapshotArg(d.Snapshot),
		d.AttemptCount,
		d.NextRetryAt,
		d.LastProvider,
		int32(d.LastOutcome),
		truncate(d.LastErrorCode, errorCodeMaxLen),
		truncate(d.LastErrorMessage, errorMessageMaxLen),
		d.AcceptedProvider,
		truncate(d.ReceiptID, receiptIDMaxLen),
		d.CancelReason,
		d.LeaseOwner,
		d.LeaseUntil,
		d.UpdatedAt,
		d.ID,
	)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return v2.ErrDeliveryUnexpectedAffectedRows
	}
	return nil
}

func (s *deliveryStorage) GetDelivery(ctx context.Context, id string) (*domain.Delivery, error) {
	return scanDelivery(s.qe.QueryRowContext(ctx, selectDeliverySQL, id))
}

func (s *deliveryStorage) CreateAttempt(ctx context.Context, a *domain.Attempt) error {
	_, err := s.qe.ExecContext(
		ctx,
		insertAttemptSQL,
		a.ID,
		a.DeliveryID,
		a.Seq,
		a.Provider,
		int32(a.Result),
		truncate(a.ReceiptID, receiptIDMaxLen),
		a.StatusCode,
		truncate(a.ErrorCode, errorCodeMaxLen),
		truncate(a.ErrorMessage, errorMessageMaxLen),
		a.StartedAt,
		a.FinishedAt,
		a.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, mysqlstorage.ErrDuplicateEntry) {
			return v2.ErrAttemptAlreadyExists
		}
		return err
	}
	return nil
}

func (s *deliveryStorage) UpdateAttempt(ctx context.Context, a *domain.Attempt) error {
	result, err := s.qe.ExecContext(
		ctx,
		updateAttemptSQL,
		int32(a.Result),
		truncate(a.ReceiptID, receiptIDMaxLen),
		a.StatusCode,
		truncate(a.ErrorCode, errorCodeMaxLen),
		truncate(a.ErrorMessage, errorMessageMaxLen),
		a.FinishedAt,
		a.ID,
	)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return v2.ErrDeliveryUnexpectedAffectedRows
	}
	return nil
}

func (s *deliveryStorage) ListAttempts(
	ctx context.Context,
	deliveryID string,
) ([]*domain.Attempt, error) {
	rows, err := s.qe.QueryContext(ctx, listAttemptsSQL, deliveryID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	attempts := []*domain.Attempt{}
	for rows.Next() {
		a := &domain.Attempt{}
		if err := rows.Scan(
			&a.ID,
			&a.DeliveryID,
			&a.Seq,
			&a.Provider,
			&a.Result,
			&a.ReceiptID,
			&a.StatusCode,
			&a.ErrorCode,
			&a.ErrorMessage,
			&a.StartedAt,
			&a.FinishedAt,
			&a.CreatedAt,
		); err != nil {
			return nil, err
		}
		attempts = append(attempts, a)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return attempts, nil
}

func (s *deliveryStorage) AcquireLease(
	ctx context.Context,
	id, owner string,
	ttlSec, now int64,
) (bool, error) {
	result, err := s.qe.ExecContext(
		ctx,
		acquireLeaseSQL,
		owner,
		now+ttlSec,
		now,
		id,
		int32(domain.DeliveryStatusAccepted),
		int32(domain.DeliveryStatusRejected),
		int32(domain.DeliveryStatusCanceled),
		now,
		owner,
	)
	if err != nil {
		return false, err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rowsAffected == 1, nil
}

func (s *deliveryStorage) ReleaseLease(
	ctx context.Context,
	id, owner string,
) error {
	_, err := s.qe.ExecContext(ctx, releaseLeaseSQL, timeNow(), id, owner)
	return err
}

func (s *deliveryStorage) ListDueDeliveries(
	ctx context.Context,
	now int64,
	limit int,
) ([]*domain.Delivery, error) {
	rows, err := s.qe.QueryContext(
		ctx,
		listDueDeliveriesSQL,
		int32(domain.DeliveryStatusRenderFailed),
		int32(domain.DeliveryStatusBackoff),
		int32(domain.DeliveryStatusPendingConfirmation),
		now,
		now,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	deliveries := []*domain.Delivery{}
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, err
		}
		deliveries = append(deliveries, d)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return deliveries, nil
}

// rowScanner is satisfied by both a single Row and Rows.
type rowScanner interface {
	Scan(dest ...interface{}) error
}

func scanDelivery(row rowScanner) (*domain.Delivery, error) {
	d := &domain.Delivery{}
	var snapshot sql.NullString
	err := row.Scan(
		&d.ID,
		&d.EventID,
		&d.OrganizationID,
		&d.EventEmail,
		&d.EventLanguage,
		&d.Status,
		&snapshot,
		&d.AttemptCount,
		&d.NextRetryAt,
		&d.LastProvider,
		&d.LastOutcome,
		&d.LastErrorCode,
		&d.LastErrorMessage,
		&d.AcceptedProvider,
		&d.ReceiptID,
		&d.CancelReason,
		&d.LeaseOwner,
		&d.LeaseUntil,
		&d.CreatedAt,
		&d.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, mysqlstorage.ErrNoRows) {
			return nil, v2.ErrDeliveryNotFound
		}
		return nil, err
	}
	if snapshot.Valid && snapshot.String != "" {
		s := &domain.Snapshot{}
		if err := json.Unmarshal([]byte(snapshot.String), s); err != nil {
			return nil, err
		}
		d.Snapshot = s
	}
	return d, nil
}

// snapshotArg returns the JSON parameter for the snapshot column.
func snapshotArg(s *domain.Snapshot) interface{} {
	if s == nil {
		return nil
	}
	return mysqlstorage.JSONObject{Val: s}
}

// truncate limits a string to n characters, as counted in bytes.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// timeNow returns the current epoch second.
func timeNow() int64 {
	return time.Now().Unix()
}
