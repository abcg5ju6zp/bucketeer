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
	_ "embed"
	"errors"

	operationaldatabase "github.com/bucketeer-io/bucketeer/v2/pkg/eventcounter/storage/v2/operational_database"
	"github.com/bucketeer-io/bucketeer/v2/pkg/storage/v2/mysql"
)

var (
	//go:embed sql/upsert_event_watermark.sql
	upsertEventWatermarkSQL string
	//go:embed sql/select_event_watermark.sql
	selectEventWatermarkSQL string
)

type eventWatermarkStorage struct {
	qe mysql.QueryExecer
}

func NewEventWatermarkStorage(qe mysql.QueryExecer) operationaldatabase.EventWatermarkStorage {
	return &eventWatermarkStorage{qe: qe}
}

func (s *eventWatermarkStorage) UpsertEventWatermark(
	ctx context.Context,
	environmentID string,
	eventType int32,
	highWaterAt int64,
	updatedAt int64,
) error {
	_, err := s.qe.ExecContext(
		ctx,
		upsertEventWatermarkSQL,
		environmentID,
		eventType,
		highWaterAt,
		updatedAt,
	)
	return err
}

func (s *eventWatermarkStorage) GetEventWatermark(
	ctx context.Context,
	environmentID string,
	eventType int32,
) (int64, int64, bool, error) {
	var highWaterAt, rowUpdatedAt int64
	err := s.qe.QueryRowContext(
		ctx,
		selectEventWatermarkSQL,
		environmentID,
		eventType,
	).Scan(&highWaterAt, &rowUpdatedAt)
	if err != nil {
		if errors.Is(err, mysql.ErrNoRows) {
			return 0, 0, false, nil
		}
		return 0, 0, false, err
	}
	return highWaterAt, rowUpdatedAt, true, nil
}
