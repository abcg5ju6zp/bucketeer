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
package operationaldatabase

import "context"

const (
	// WatermarkEventTypeEvaluation is the evaluation event watermark.
	WatermarkEventTypeEvaluation int32 = 1
	// WatermarkEventTypeGoal is the goal event watermark.
	WatermarkEventTypeGoal int32 = 2
)

// EventWatermarkStorage persists the per-environment, per-event-type
// high-water mark of event timestamps (epoch microseconds) that have been
// durably persisted by the subscriber. The experiment finalizer waits until
// both watermarks reach the fixed termination watermarks before calculating
// the final statistics.
type EventWatermarkStorage interface {
	// UpsertEventWatermark advances the high-water mark to highWaterAt (it
	// never moves backwards) and always advances updatedAt, so the
	// finalizer can distinguish a quiet environment from a subscriber that
	// is behind.
	UpsertEventWatermark(
		ctx context.Context,
		environmentID string,
		eventType int32,
		highWaterAt int64,
		updatedAt int64,
	) error
	// GetEventWatermark returns the stored high-water mark. When no row
	// exists yet, found is false (no error).
	GetEventWatermark(
		ctx context.Context,
		environmentID string,
		eventType int32,
	) (highWaterAt int64, updatedAt int64, found bool, err error)
}
