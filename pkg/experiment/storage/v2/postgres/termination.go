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
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"strings"

	"github.com/bucketeer-io/bucketeer/v2/pkg/experiment/domain"
	v2es "github.com/bucketeer-io/bucketeer/v2/pkg/experiment/storage/v2"
	pgstorage "github.com/bucketeer-io/bucketeer/v2/pkg/storage/v2/postgres"
)

var (
	//go:embed sql/termination/insert_termination.sql
	insertTerminationSQL string
	//go:embed sql/termination/select_termination.sql
	selectTerminationSQL string
	//go:embed sql/termination/select_terminations.sql
	selectTerminationsSQL string
	//go:embed sql/termination/update_termination.sql
	updateTerminationSQL string
)

type terminationStorage struct {
	qe pgstorage.QueryExecer
}

func NewTerminationStorage(qe pgstorage.QueryExecer) v2es.TerminationStorage {
	return &terminationStorage{qe: qe}
}

func (s *terminationStorage) CreateTermination(
	ctx context.Context,
	t *domain.Termination,
) error {
	summary := t.InputSummary
	if summary == nil {
		// The column is NOT NULL; '{}' marks "no input summary yet".
		summary = &domain.InputSummary{}
	}
	_, err := s.qe.ExecContext(
		ctx,
		insertTerminationSQL,
		t.ID,
		t.EnvironmentID,
		t.ExperimentID,
		t.Reason,
		t.Phase,
		pgstorage.JSONObject{Val: t.Snapshot},
		t.EvaluationWatermark,
		t.GoalWatermark,
		pgstorage.JSONObject{Val: summary},
		t.FinalResultID,
		t.OwnerID,
		t.OwnerExpiresAt,
		t.Attempts,
		sql.NullString{String: t.LastError, Valid: t.LastError != ""},
		t.CreatedAt,
		t.UpdatedAt,
		t.FinalizedAt,
	)
	if err != nil {
		if errors.Is(err, pgstorage.ErrDuplicateEntry) {
			return v2es.ErrTerminationAlreadyExists
		}
		return err
	}
	return nil
}

func (s *terminationStorage) GetTerminationByExperimentID(
	ctx context.Context,
	experimentID string,
	environmentID string,
) (*domain.Termination, error) {
	row := s.qe.QueryRowContext(ctx, selectTerminationSQL, experimentID, environmentID)
	t, err := scanTermination(row)
	if err != nil {
		if errors.Is(err, pgstorage.ErrNoRows) {
			return nil, v2es.ErrTerminationNotFound
		}
		return nil, err
	}
	return t, nil
}

func (s *terminationStorage) ListTerminationsByPhases(
	ctx context.Context,
	environmentID string,
	phases []int32,
) ([]*domain.Termination, error) {
	if len(phases) == 0 {
		return []*domain.Termination{}, nil
	}
	// environment_id is $1 and the phases are placeholders starting at $2.
	phasePlaceholders := make([]string, 0, len(phases))
	args := make([]interface{}, 0, len(phases)+1)
	args = append(args, environmentID)
	for i, p := range phases {
		phasePlaceholders = append(phasePlaceholders, fmt.Sprintf("$%d", i+2))
		args = append(args, p)
	}
	query := selectTerminationsSQL + strings.Join(phasePlaceholders, ", ") + ")"
	rows, err := s.qe.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	terminations := make([]*domain.Termination, 0)
	for rows.Next() {
		t, err := scanTermination(rows)
		if err != nil {
			return nil, err
		}
		terminations = append(terminations, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return terminations, nil
}

func (s *terminationStorage) UpdateTerminationCAS(
	ctx context.Context,
	t *domain.Termination,
	expectedPhase int32,
	expectedOwner string,
) error {
	summary := t.InputSummary
	if summary == nil {
		summary = &domain.InputSummary{}
	}
	query := updateTerminationSQL
	args := []interface{}{
		t.Phase,
		pgstorage.JSONObject{Val: summary},
		t.FinalResultID,
		t.OwnerID,
		t.OwnerExpiresAt,
		t.Attempts,
		sql.NullString{String: t.LastError, Valid: t.LastError != ""},
		t.UpdatedAt,
		t.FinalizedAt,
		t.ID,
		t.EnvironmentID,
		expectedPhase,
	}
	if expectedOwner != "" {
		query += " AND owner_id = $13"
		args = append(args, expectedOwner)
	}
	result, err := s.qe.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return v2es.ErrTerminationPhaseConflict
	}
	return nil
}

// rowScanner abstracts QueryRow and Rows for scanTermination.
type rowScanner interface {
	Scan(dest ...interface{}) error
}

func scanTermination(row rowScanner) (*domain.Termination, error) {
	t := &domain.Termination{Snapshot: &domain.ConfigSnapshot{}}
	summary := &domain.InputSummary{}
	var lastError sql.NullString
	err := row.Scan(
		&t.ID,
		&t.EnvironmentID,
		&t.ExperimentID,
		&t.Reason,
		&t.Phase,
		&pgstorage.JSONObject{Val: t.Snapshot},
		&t.EvaluationWatermark,
		&t.GoalWatermark,
		&pgstorage.JSONObject{Val: summary},
		&t.FinalResultID,
		&t.OwnerID,
		&t.OwnerExpiresAt,
		&t.Attempts,
		&lastError,
		&t.CreatedAt,
		&t.UpdatedAt,
		&t.FinalizedAt,
	)
	if err != nil {
		return nil, err
	}
	// A '{}' document means the summary has not been computed yet; keep the
	// pointer nil so callers can distinguish the two states.
	if summary.EndAt != 0 || len(summary.Goals) > 0 {
		t.InputSummary = summary
	}
	if lastError.Valid {
		t.LastError = lastError.String
	}
	return t, nil
}
