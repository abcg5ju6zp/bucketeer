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
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/bucketeer-io/bucketeer/v2/pkg/coderef/storage"
	mysqlstorage "github.com/bucketeer-io/bucketeer/v2/pkg/storage/v2/mysql"
	mysqlmock "github.com/bucketeer-io/bucketeer/v2/pkg/storage/v2/mysql/mock"
)

func TestNewCodeReferenceStorage(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	s := NewCodeReferenceStorage(mysqlmock.NewMockQueryExecer(ctrl))
	assert.NotNil(t, s)
	assert.Implements(t, (*storage.CodeReferenceStorage)(nil), s)
}

// TestEmbeddedSQLStatements guards against broken embed paths and ensures
// every statement references the expected tables/columns.
func TestEmbeddedSQLStatements(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		sql  string
		want []string
	}{
		{"insert reference", insertCodeReferenceSQL, []string{"INSERT INTO code_reference", "version"}},
		{"update reference", updateCodeReferenceSQL, []string{"UPDATE code_reference", "AND version = ?"}},
		{"select reference", selectCodeReferenceSQL, []string{"FROM", "code_reference"}},
		{"list references", selectCodeReferencesSQL, []string{"FROM", "code_reference"}},
		{"delete reference", deleteCodeReferenceSQL, []string{"DELETE FROM code_reference", "AND version = ?"}},
		{"insert change", insertReferenceChangeSQL, []string{"INSERT INTO code_reference_change"}},
		{"select change", selectReferenceChangeSQL, []string{"FROM", "code_reference_change"}},
		{"select change by hash", selectReferenceChangeByRequestHashSQL, []string{"request_hash = ?"}},
		{"update change status", updateReferenceChangeStatusSQL, []string{"UPDATE code_reference_change", "status = ?"}},
		{"update change event", updateReferenceChangeEventDataSQL, []string{"event_data = ?"}},
		{"pending changes", selectPendingReferenceChangesSQL, []string{"status = ?", "ORDER BY", "seq"}},
		{"watermark", selectConfirmedWatermarkSQL, []string{"MIN(seq)", "MAX(seq)"}},
		{"count at watermark", countLiveReferencesAtWatermarkSQL, []string{"NOT EXISTS", "GROUP BY c.feature_id"}},
		{"lock feature", lockFeatureForArchiveSQL, []string{"FROM feature", "FOR UPDATE"}},
		{"changes after", selectHasChangesAfterSQL, []string{"seq > ?", "FOR UPDATE"}},
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

func TestCountLiveReferencePlaceholderArgs(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	qe := mysqlmock.NewMockQueryExecer(ctrl)
	s := NewCodeReferenceStorage(qe).(*codeReferenceStorage)

	t.Run("builds one placeholder per feature in order", func(t *testing.T) {
		t.Parallel()
		var captured string
		rows := mysqlmock.NewMockRows(ctrl)
		qe.EXPECT().QueryContext(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(
			ctx context.Context, query string, args ...interface{},
		) (interface{}, error) {
			captured = query
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
		assert.Contains(t, captured, "IN (?, ?, ?)")
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
	qe := mysqlmock.NewMockQueryExecer(ctrl)
	s := NewCodeReferenceStorage(qe)

	t.Run("reference maps no rows to not found", func(t *testing.T) {
		t.Parallel()
		row := mysqlmock.NewMockRow(ctrl)
		qe.EXPECT().QueryRowContext(gomock.Any(), gomock.Any(), gomock.Any()).Return(row)
		row.EXPECT().Scan(gomock.Any()).Return(mysqlstorage.ErrNoRows)
		_, err := s.GetCodeReference(context.Background(), "id-1")
		assert.ErrorIs(t, err, storage.ErrCodeReferenceNotFound)
	})

	t.Run("change maps no rows to change not found", func(t *testing.T) {
		t.Parallel()
		row := mysqlmock.NewMockRow(ctrl)
		qe.EXPECT().QueryRowContext(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(row)
		row.EXPECT().Scan(gomock.Any()).Return(mysqlstorage.ErrNoRows)
		_, err := s.GetChangeByRequestHash(context.Background(), "env-1", "hash")
		assert.ErrorIs(t, err, storage.ErrReferenceChangeNotFound)
	})
}
