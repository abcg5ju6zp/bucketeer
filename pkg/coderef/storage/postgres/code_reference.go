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

package postgres

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/bucketeer-io/bucketeer/v2/pkg/coderef/domain"
	"github.com/bucketeer-io/bucketeer/v2/pkg/coderef/storage"
	pgstorage "github.com/bucketeer-io/bucketeer/v2/pkg/storage/v2/postgres"
	coderefproto "github.com/bucketeer-io/bucketeer/v2/proto/coderef"
	"google.golang.org/protobuf/proto"
)

var (
	// code reference statements
	//go:embed sql/code_reference/insert_code_reference.sql
	insertCodeReferenceSQL string
	//go:embed sql/code_reference/update_code_reference.sql
	updateCodeReferenceSQL string
	//go:embed sql/code_reference/select_code_reference.sql
	selectCodeReferenceSQL string
	//go:embed sql/code_reference/select_code_references.sql
	selectCodeReferencesSQL string
	//go:embed sql/code_reference/delete_code_reference.sql
	deleteCodeReferenceSQL string

	// reference change statements
	//go:embed sql/reference_change/insert_reference_change.sql
	insertReferenceChangeSQL string
	//go:embed sql/reference_change/select_reference_change.sql
	selectReferenceChangeSQL string
	//go:embed sql/reference_change/select_reference_change_by_request_hash.sql
	selectReferenceChangeByRequestHashSQL string
	//go:embed sql/reference_change/update_reference_change_status.sql
	updateReferenceChangeStatusSQL string
	//go:embed sql/reference_change/select_pending_reference_changes.sql
	selectPendingReferenceChangesSQL string
	//go:embed sql/reference_change/update_reference_change_event_data.sql
	updateReferenceChangeEventDataSQL string
	//go:embed sql/reference_change/select_confirmed_watermark.sql
	selectConfirmedWatermarkSQL string
	//go:embed sql/reference_change/count_live_references_at_watermark.sql
	countLiveReferencesAtWatermarkSQL string
	//go:embed sql/reference_change/lock_feature_for_archive.sql
	lockFeatureForArchiveSQL string
	//go:embed sql/reference_change/select_has_changes_after.sql
	selectHasChangesAfterSQL string
)

type codeReferenceStorage struct {
	qe pgstorage.QueryExecer
}

func NewCodeReferenceStorage(qe pgstorage.QueryExecer) storage.CodeReferenceStorage {
	return &codeReferenceStorage{qe}
}

func scanCodeReference(scanner interface {
	Scan(dest ...interface{}) error
}) (*domain.CodeReference, error) {
	codeRef := &domain.CodeReference{}
	err := scanner.Scan(
		&codeRef.Id,
		&codeRef.FeatureId,
		&codeRef.FilePath,
		&codeRef.FileExtension,
		&codeRef.LineNumber,
		&codeRef.CodeSnippet,
		&codeRef.ContentHash,
		&pgstorage.JSONObject{Val: &codeRef.Aliases},
		&codeRef.RepositoryName,
		&codeRef.RepositoryOwner,
		&codeRef.RepositoryType,
		&codeRef.RepositoryBranch,
		&codeRef.CommitHash,
		&codeRef.EnvironmentId,
		&codeRef.CreatedAt,
		&codeRef.UpdatedAt,
		&codeRef.Version,
	)
	if err != nil {
		return nil, err
	}
	return codeRef, nil
}

func scanReferenceChange(scanner interface {
	Scan(dest ...interface{}) error
}) (*domain.ReferenceChange, error) {
	ch := &domain.ReferenceChange{}
	err := scanner.Scan(
		&ch.ID,
		&ch.Seq,
		&ch.CodeReferenceID,
		&ch.EnvironmentID,
		&ch.FeatureID,
		&ch.ChangeType,
		&ch.BeforeVersion,
		&ch.AfterVersion,
		&ch.RequestHash,
		&ch.EventData,
		&ch.Status,
		&ch.CreatedAt,
		&ch.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return ch, nil
}

func (s *codeReferenceStorage) GetCodeReference(
	ctx context.Context,
	id string,
) (*domain.CodeReference, error) {
	codeRef, err := scanCodeReference(s.qe.QueryRowContext(ctx, selectCodeReferenceSQL, id))
	if err != nil {
		if errors.Is(err, pgstorage.ErrNoRows) {
			return nil, storage.ErrCodeReferenceNotFound
		}
		return nil, err
	}
	return codeRef, nil
}

func (s *codeReferenceStorage) ListCodeReferences(
	ctx context.Context,
	params storage.ListCodeReferencesParams,
) ([]*domain.CodeReference, int, int64, error) {
	listOpts, err := listCodeReferencesOptionsFromParams(params)
	if err != nil {
		return nil, 0, 0, err
	}
	whereSQL, whereArgs := pgstorage.ConstructWhereSQLString(listOpts.CreateWhereParts())
	orderBySQL := pgstorage.ConstructOrderBySQLString(listOpts.Orders)
	limitOffsetSQL := pgstorage.ConstructLimitOffsetSQLString(listOpts.Limit, listOpts.Offset)
	query := fmt.Sprintf("%s %s %s %s", selectCodeReferencesSQL, whereSQL, orderBySQL, limitOffsetSQL)
	rows, err := s.qe.QueryContext(ctx, query, whereArgs...)
	if err != nil {
		return nil, 0, 0, err
	}
	defer rows.Close()
	codeRefs := make([]*domain.CodeReference, 0, listOpts.Limit)
	for rows.Next() {
		codeRef, err := scanCodeReference(rows)
		if err != nil {
			return nil, 0, 0, err
		}
		codeRefs = append(codeRefs, codeRef)
	}
	if rows.Err() != nil {
		return nil, 0, 0, rows.Err()
	}
	nextOffset := listOpts.Offset + len(codeRefs)
	var total int64
	countQuery := fmt.Sprintf(
		"SELECT COUNT(1) FROM code_reference %s",
		whereSQL,
	)
	if err := s.qe.QueryRowContext(ctx, countQuery, whereArgs...).Scan(&total); err != nil {
		return nil, 0, 0, err
	}
	return codeRefs, nextOffset, total, nil
}

func (s *codeReferenceStorage) GetChangeByRequestHash(
	ctx context.Context,
	environmentID, requestHash string,
) (*domain.ReferenceChange, error) {
	ch, err := scanReferenceChange(s.qe.QueryRowContext(
		ctx, selectReferenceChangeByRequestHashSQL, environmentID, requestHash,
	))
	if err != nil {
		if errors.Is(err, pgstorage.ErrNoRows) {
			return nil, storage.ErrReferenceChangeNotFound
		}
		return nil, err
	}
	return ch, nil
}

func (s *codeReferenceStorage) GetChange(ctx context.Context, id string) (*domain.ReferenceChange, error) {
	ch, err := scanReferenceChange(s.qe.QueryRowContext(ctx, selectReferenceChangeSQL, id))
	if err != nil {
		if errors.Is(err, pgstorage.ErrNoRows) {
			return nil, storage.ErrReferenceChangeNotFound
		}
		return nil, err
	}
	return ch, nil
}

func (s *codeReferenceStorage) CreateReferenceAndChange(
	ctx context.Context,
	codeRef *domain.CodeReference,
	change *domain.ReferenceChange,
	buildEvent storage.EventBuilder,
) error {
	if err := s.insertCodeReference(ctx, codeRef); err != nil {
		return err
	}
	return s.insertChangeAndEvent(ctx, change, buildEvent)
}

func (s *codeReferenceStorage) UpdateReferenceAndChange(
	ctx context.Context,
	codeRef *domain.CodeReference,
	change *domain.ReferenceChange,
	buildEvent storage.EventBuilder,
) error {
	result, err := s.qe.ExecContext(
		ctx,
		updateCodeReferenceSQL,
		codeRef.FilePath,
		codeRef.FileExtension,
		codeRef.LineNumber,
		codeRef.CodeSnippet,
		codeRef.ContentHash,
		pgstorage.JSONObject{Val: codeRef.Aliases},
		codeRef.RepositoryName,
		codeRef.RepositoryOwner,
		codeRef.RepositoryType,
		codeRef.RepositoryBranch,
		codeRef.CommitHash,
		codeRef.UpdatedAt,
		codeRef.Version,
		codeRef.Id,
		codeRef.EnvironmentId,
		codeRef.Version-1,
	)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return storage.ErrCodeReferenceUnexpectedAffectedRows
	}
	return s.insertChangeAndEvent(ctx, change, buildEvent)
}

func (s *codeReferenceStorage) DeleteReferenceAndChange(
	ctx context.Context,
	change *domain.ReferenceChange,
	buildEvent storage.EventBuilder,
) error {
	result, err := s.qe.ExecContext(
		ctx,
		deleteCodeReferenceSQL,
		change.CodeReferenceID,
		change.EnvironmentID,
		change.BeforeVersion,
	)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return storage.ErrCodeReferenceUnexpectedAffectedRows
	}
	return s.insertChangeAndEvent(ctx, change, buildEvent)
}

// insertChangeAndEvent inserts the change row with NULL event_data, reads the
// allocated sequence from RETURNING, builds the domain event and stores the
// serialized event in the change row. All statements participate in the
// caller's transaction, so the change row is never visible without its
// recoverable event.
func (s *codeReferenceStorage) insertChangeAndEvent(
	ctx context.Context,
	ch *domain.ReferenceChange,
	buildEvent storage.EventBuilder,
) error {
	var seq int64
	err := s.qe.QueryRowContext(
		ctx,
		insertReferenceChangeSQL,
		ch.ID,
		ch.CodeReferenceID,
		ch.EnvironmentID,
		ch.FeatureID,
		ch.ChangeType,
		ch.BeforeVersion,
		ch.AfterVersion,
		ch.RequestHash,
		nil, // event_data is written after the sequence is allocated
		ch.Status,
		ch.CreatedAt,
		ch.UpdatedAt,
	).Scan(&seq)
	if err != nil {
		return err
	}
	event, err := buildEvent(seq)
	if err != nil {
		return err
	}
	eventData, err := proto.Marshal(event)
	if err != nil {
		return err
	}
	now := storage.NowUnix()
	_, err = s.qe.ExecContext(
		ctx,
		updateReferenceChangeEventDataSQL,
		eventData,
		now,
		ch.ID,
	)
	if err != nil {
		return err
	}
	ch.Seq = seq
	ch.EventData = eventData
	ch.UpdatedAt = now
	return nil
}

func (s *codeReferenceStorage) insertCodeReference(
	ctx context.Context,
	cr *domain.CodeReference,
) error {
	_, err := s.qe.ExecContext(
		ctx,
		insertCodeReferenceSQL,
		cr.Id,
		cr.FeatureId,
		cr.FilePath,
		cr.FileExtension,
		cr.LineNumber,
		cr.CodeSnippet,
		cr.ContentHash,
		pgstorage.JSONObject{Val: cr.Aliases},
		cr.RepositoryName,
		cr.RepositoryOwner,
		cr.RepositoryType,
		cr.RepositoryBranch,
		cr.CommitHash,
		cr.EnvironmentId,
		cr.CreatedAt,
		cr.UpdatedAt,
		cr.Version,
	)
	return err
}

func (s *codeReferenceStorage) MarkChangePublished(ctx context.Context, id string) error {
	now := storage.NowUnix()
	_, err := s.qe.ExecContext(
		ctx,
		updateReferenceChangeStatusSQL,
		domain.ChangeStatusPublished,
		now,
		id,
		domain.ChangeStatusPending,
	)
	// Zero affected rows means the change was already published, which is a
	// successful outcome.
	return err
}

func (s *codeReferenceStorage) ListPendingChanges(
	ctx context.Context,
	limit int,
) ([]*domain.ReferenceChange, error) {
	rows, err := s.qe.QueryContext(
		ctx,
		selectPendingReferenceChangesSQL,
		domain.ChangeStatusPending,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectReferenceChanges(rows)
}

func (s *codeReferenceStorage) GetConfirmedWatermark(
	ctx context.Context,
	environmentID string,
) (int64, error) {
	var watermark int64
	err := s.qe.QueryRowContext(
		ctx,
		selectConfirmedWatermarkSQL,
		environmentID,
		domain.ChangeStatusPending,
		environmentID,
	).Scan(&watermark)
	return watermark, err
}

func (s *codeReferenceStorage) CountLiveReferencesAtWatermark(
	ctx context.Context,
	environmentID string,
	featureIDs []string,
	watermark int64,
) (map[string]int64, error) {
	result := make(map[string]int64, len(featureIDs))
	if len(featureIDs) == 0 {
		return result, nil
	}
	// Placeholders are bound by number, not by textual order, so the NOT
	// EXISTS clause can reference $4/$5 while the IN list starts at $6.
	placeholders := make([]string, len(featureIDs))
	args := make([]interface{}, 0, len(featureIDs)+5)
	args = append(args,
		environmentID,           // $1
		watermark,               // $2
		domain.ChangeTypeCreate, // $3
		watermark,               // $4
		domain.ChangeTypeDelete, // $5
	)
	for i, id := range featureIDs {
		placeholders[i] = fmt.Sprintf("$%d", 6+i)
		args = append(args, id)
	}
	query := fmt.Sprintf(countLiveReferencesAtWatermarkSQL, strings.Join(placeholders, ", "))
	rows, err := s.qe.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var featureID string
		var count int64
		if err := rows.Scan(&featureID, &count); err != nil {
			return nil, err
		}
		result[featureID] = count
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return result, nil
}

func (s *codeReferenceStorage) HasReferenceChangesAfter(
	ctx context.Context,
	environmentID, featureID string,
	watermark int64,
) (bool, error) {
	// Lock the parent feature row first.
	var locked int
	err := s.qe.QueryRowContext(
		ctx,
		lockFeatureForArchiveSQL,
		environmentID,
		featureID,
	).Scan(&locked)
	if err != nil {
		if errors.Is(err, pgstorage.ErrNoRows) {
			return false, storage.ErrCodeReferenceNotFound
		}
		return false, err
	}
	// Check for changes after the watermark.
	var found int
	err = s.qe.QueryRowContext(
		ctx,
		selectHasChangesAfterSQL,
		environmentID,
		featureID,
		watermark,
	).Scan(&found)
	if err != nil {
		if errors.Is(err, pgstorage.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func collectReferenceChanges(rows interface {
	Next() bool
	Close() error
	Err() error
	Scan(dest ...interface{}) error
}) ([]*domain.ReferenceChange, error) {
	changes := make([]*domain.ReferenceChange, 0)
	for rows.Next() {
		ch, err := scanReferenceChange(rows)
		if err != nil {
			return nil, err
		}
		changes = append(changes, ch)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return changes, nil
}

func listCodeReferencesOptionsFromParams(
	p storage.ListCodeReferencesParams,
) (*pgstorage.ListOptions, error) {
	filters := []*pgstorage.Filter{
		{Column: "environment_id", Operator: pgstorage.OperatorEqual, Value: p.EnvironmentID},
		{Column: "feature_id", Operator: pgstorage.OperatorEqual, Value: p.FeatureID},
	}
	if p.RepositoryName != "" {
		filters = append(filters, &pgstorage.Filter{
			Column: "repository_name", Operator: pgstorage.OperatorEqual, Value: p.RepositoryName,
		})
	}
	if p.RepositoryOwner != "" {
		filters = append(filters, &pgstorage.Filter{
			Column: "repository_owner", Operator: pgstorage.OperatorEqual, Value: p.RepositoryOwner,
		})
	}
	if p.RepositoryType != coderefproto.CodeReference_REPOSITORY_TYPE_UNSPECIFIED {
		filters = append(filters, &pgstorage.Filter{
			Column: "repository_type", Operator: pgstorage.OperatorEqual, Value: p.RepositoryType,
		})
	}
	if p.RepositoryBranch != "" {
		filters = append(filters, &pgstorage.Filter{
			Column: "repository_branch", Operator: pgstorage.OperatorEqual, Value: p.RepositoryBranch,
		})
	}
	if p.FileExtension != "" {
		filters = append(filters, &pgstorage.Filter{
			Column: "file_extension", Operator: pgstorage.OperatorEqual, Value: p.FileExtension,
		})
	}

	var column string
	switch p.OrderBy {
	case coderefproto.ListCodeReferencesRequest_DEFAULT:
		column = "id"
	case coderefproto.ListCodeReferencesRequest_CREATED_AT:
		column = "created_at"
	case coderefproto.ListCodeReferencesRequest_UPDATED_AT:
		column = "updated_at"
	default:
		return nil, storage.ErrInvalidOrderBy
	}
	direction := pgstorage.OrderDirectionAsc
	if p.OrderDirection == coderefproto.ListCodeReferencesRequest_DESC {
		direction = pgstorage.OrderDirectionDesc
	}

	cursor := p.Cursor
	if cursor == "" {
		cursor = "0"
	}
	offset, err := strconv.Atoi(cursor)
	if err != nil || offset < 0 {
		return nil, storage.ErrInvalidCursor
	}
	limit := p.PageSize
	if limit < 0 {
		limit = 0
	}
	return &pgstorage.ListOptions{
		Limit:   limit,
		Offset:  offset,
		Filters: filters,
		Orders:  []*pgstorage.Order{pgstorage.NewOrder(column, direction)},
	}, nil
}
