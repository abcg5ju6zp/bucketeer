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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	coderefdomain "github.com/bucketeer-io/bucketeer/v2/pkg/coderef/domain"
	"github.com/bucketeer-io/bucketeer/v2/pkg/coderef/storage"
	pgstorage "github.com/bucketeer-io/bucketeer/v2/pkg/storage/v2/postgres"
	postgresmock "github.com/bucketeer-io/bucketeer/v2/pkg/storage/v2/postgres/mock"
)

func TestNewCodeReferenceStorage(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	s := NewCodeReferenceStorage(postgresmock.NewMockQueryExecer(ctrl))
	assert.NotNil(t, s)
	assert.Implements(t, (*storage.CodeReferenceStorage)(nil), s)
}

// TestEmbeddedSQLStatements guards against broken embed paths and ensures
// every statement references the expected tables/columns and uses numbered
// placeholders.
func TestEmbeddedSQLStatements(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		sql  string
		want []string
	}{
		{"insert reference", insertCodeReferenceSQL, []string{"INSERT INTO code_reference", "$17"}},
		{"update reference", updateCodeReferenceSQL, []string{"UPDATE code_reference", "AND version = $16"}},
		{"select reference", selectCodeReferenceSQL, []string{"FROM", "code_reference"}},
		{"list references", selectCodeReferencesSQL, []string{"FROM", "code_reference"}},
		{"delete reference", deleteCodeReferenceSQL, []string{"DELETE FROM code_reference", "AND version = $3"}},
		{"insert change", insertReferenceChangeSQL, []string{"INSERT INTO code_reference_change", "RETURNING seq"}},
		{"select change", selectReferenceChangeSQL, []string{"FROM", "code_reference_change"}},
		{"select change by hash", selectReferenceChangeByRequestHashSQL, []string{"request_hash = $2"}},
		{"update change status", updateReferenceChangeStatusSQL, []string{"UPDATE code_reference_change", "status = $1"}},
		{"update change event", updateReferenceChangeEventDataSQL, []string{"event_data = $1"}},
		{"pending changes", selectPendingReferenceChangesSQL, []string{"status = $1", "ORDER BY", "seq"}},
		{"watermark", selectConfirmedWatermarkSQL, []string{"MIN(seq)", "MAX(seq)", "$3"}},
		{"count at watermark", countLiveReferencesAtWatermarkSQL, []string{"NOT EXISTS", "GROUP BY c.feature_id", "$4", "$5"}},
		{"lock feature", lockFeatureForArchiveSQL, []string{"FROM feature", "FOR UPDATE"}},
		{"changes after", selectHasChangesAfterSQL, []string{"seq > $3", "FOR UPDATE"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.NotEmpty(t, tc.sql)
			for _, w := range tc.want {
				assert.Contains(t, tc.sql, w)
			}
		})
	}
}

func TestCountLiveReferencePlaceholders(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	qe := postgresmock.NewMockQueryExecer(ctrl)
	s := NewCodeReferenceStorage(qe).(*codeReferenceStorage)

	t.Run("numbered placeholders start at $6 and args keep order", func(t *testing.T) {
		t.Parallel()
		var captured string
		var capturedArgs []interface{}
		rows := postgresmock.NewMockRows(ctrl)
		qe.EXPECT().QueryContext(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(
			ctx context.Context, query string, args ...interface{},
		) (interface{}, error) {
			captured = query
			capturedArgs = args
			return rows, nil
		})
		rows.EXPECT().Close().Return(nil)
		rows.EXPECT().Next().Return(false)
		rows.EXPECT().Err().Return(nil)

		_, err := s.CountLiveReferencesAtWatermark(
			context.Background(), "env-1",
			[]string{"f1", "f2", "f3"}, 10,
		)
		assert.NoError(t, err)
		assert.Contains(t, captured, "IN ($6, $7, $8)")
		// $1 env, $2/$4 watermark, $3 create type, $5 delete type, features.
		require.Len(t, capturedArgs, 8)
		assert.Equal(t, "env-1", capturedArgs[0])      // $1
		assert.Equal(t, int64(10), capturedArgs[1])    // $2 watermark
		assert.Equal(t, coderefdomain.ChangeTypeCreate, capturedArgs[2]) // $3
		assert.Equal(t, int64(10), capturedArgs[3])                       // $4 watermark
		assert.Equal(t, coderefdomain.ChangeTypeDelete, capturedArgs[4]) // $5
		assert.Equal(t, "f1", capturedArgs[5])         // $6
		assert.Equal(t, "f2", capturedArgs[6])         // $7
		assert.Equal(t, "f3", capturedArgs[7])         // $8
	})

	t.Run("empty features does not hit the database", func(t *testing.T) {
		t.Parallel()
		counts, err := s.CountLiveReferencesAtWatermark(
			context.Background(), "env-1", []string{}, 10,
		)
		assert.NoError(t, err)
		assert.Empty(t, counts)
	})
}

func TestGetNotFoundErrorMapping(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	qe := postgresmock.NewMockQueryExecer(ctrl)
	s := NewCodeReferenceStorage(qe)

	t.Run("reference maps no rows to not found", func(t *testing.T) {
		t.Parallel()
		row := postgresmock.NewMockRow(ctrl)
		qe.EXPECT().QueryRowContext(gomock.Any(), gomock.Any(), gomock.Any()).Return(row)
		row.EXPECT().Scan(gomock.Any()).Return(pgstorage.ErrNoRows)
		_, err := s.GetCodeReference(context.Background(), "id-1")
		assert.ErrorIs(t, err, storage.ErrCodeReferenceNotFound)
	})

	t.Run("change maps no rows to change not found", func(t *testing.T) {
		t.Parallel()
		row := postgresmock.NewMockRow(ctrl)
		qe.EXPECT().QueryRowContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(row)
		row.EXPECT().Scan(gomock.Any()).Return(pgstorage.ErrNoRows)
		_, err := s.GetChangeByRequestHash(context.Background(), "env-1", "hash")
		assert.ErrorIs(t, err, storage.ErrReferenceChangeNotFound)
	})
}
