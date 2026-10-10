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

package processor

import (
	"context"
	"time"

	"go.uber.org/zap"
)

const (
	watermarkEventTypeEvaluation = int32(1)
	watermarkEventTypeGoal       = int32(2)
)

// EventWatermarkRecorder persists the high-water mark of event timestamps
// durably written to the data warehouse, per environment and event type.
// The experiment finalizer waits until both watermarks reach the fixed
// termination watermarks before calculating the final statistics.
type EventWatermarkRecorder interface {
	UpsertEventWatermark(
		ctx context.Context,
		environmentID string,
		eventType int32,
		highWaterAt int64,
		updatedAt int64,
	) error
}

// persistedEvent is the subset of a data-warehouse event the watermark
// calculation needs (all generated events satisfy it).
type persistedEvent interface {
	GetId() string
	GetEnvironmentId() string
	GetTimestamp() int64
}

// highWaterByEnvironment returns the highest event timestamp that was
// durably persisted per environment: failed rows are excluded.
func highWaterByEnvironment[E persistedEvent](
	events []E,
	fails map[string]bool,
) map[string]int64 {
	highWaterByEnv := make(map[string]int64)
	for _, e := range events {
		if fails[e.GetId()] {
			continue
		}
		envID := e.GetEnvironmentId()
		if ts := e.GetTimestamp(); ts > highWaterByEnv[envID] {
			highWaterByEnv[envID] = ts
		}
	}
	return highWaterByEnv
}

// recordWatermark advances the environment watermark to the given high-water
// mark. It is best-effort: a recorder failure must not nack the
// already-persisted events, it is only logged (the finalizer keeps waiting in
// that case).
func recordWatermark(
	ctx context.Context,
	logger *zap.Logger,
	recorder EventWatermarkRecorder,
	environmentID string,
	eventType int32,
	highWaterAt int64,
) {
	if recorder == nil || highWaterAt == 0 {
		return
	}
	if err := recorder.UpsertEventWatermark(
		ctx,
		environmentID,
		eventType,
		highWaterAt,
		time.Now().UnixMicro(),
	); err != nil {
		logger.Error("Failed to record event watermark",
			zap.Error(err),
			zap.String("environmentId", environmentID),
			zap.Int32("eventType", eventType),
			zap.Int64("highWaterAt", highWaterAt),
		)
	}
}
