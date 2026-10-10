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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	epproto "github.com/bucketeer-io/bucketeer/v2/proto/eventpersisterdwh"
)

type recordedWatermark struct {
	environmentID string
	eventType     int32
	highWaterAt   int64
}

type fakeWatermarkRecorder struct {
	calls []recordedWatermark
	err   error
}

func (f *fakeWatermarkRecorder) UpsertEventWatermark(
	_ context.Context,
	environmentID string,
	eventType int32,
	highWaterAt int64,
	_ int64,
) error {
	f.calls = append(f.calls, recordedWatermark{environmentID, eventType, highWaterAt})
	return f.err
}

func TestHighWaterByEnvironment(t *testing.T) {
	t.Parallel()
	events := []*epproto.EvaluationEvent{
		{Id: "e1", EnvironmentId: "env-a", Timestamp: 100},
		{Id: "e2", EnvironmentId: "env-a", Timestamp: 300},
		{Id: "e3", EnvironmentId: "env-a", Timestamp: 200},
		{Id: "e4", EnvironmentId: "env-b", Timestamp: 50},
	}
	patterns := []struct {
		desc     string
		fails    map[string]bool
		expected map[string]int64
	}{
		{
			desc:  "all persisted takes max per environment",
			fails: map[string]bool{},
			expected: map[string]int64{
				"env-a": 300,
				"env-b": 50,
			},
		},
		{
			desc:  "failed rows are excluded",
			fails: map[string]bool{"e2": true},
			expected: map[string]int64{
				"env-a": 200,
				"env-b": 50,
			},
		},
		{
			desc:     "environment without any persisted row is absent",
			fails:    map[string]bool{"e1": true, "e2": true, "e3": true},
			expected: map[string]int64{"env-b": 50},
		},
	}
	for _, p := range patterns {
		t.Run(p.desc, func(t *testing.T) {
			assert.Equal(t, p.expected, highWaterByEnvironment(events, p.fails))
		})
	}
}

func TestRecordWatermarkBestEffort(t *testing.T) {
	t.Parallel()
	t.Run("records the watermark", func(t *testing.T) {
		rec := &fakeWatermarkRecorder{}
		recordWatermark(context.Background(), zap.NewNop(), rec, "env-a",
			watermarkEventTypeEvaluation, 123456)
		assert.Len(t, rec.calls, 1)
		assert.Equal(t, recordedWatermark{
			environmentID: "env-a",
			eventType:     watermarkEventTypeEvaluation,
			highWaterAt:   123456,
		}, rec.calls[0])
	})
	t.Run("recorder error is swallowed", func(t *testing.T) {
		rec := &fakeWatermarkRecorder{err: errors.New("db down")}
		// must not panic / return (best effort)
		recordWatermark(context.Background(), zap.NewNop(), rec, "env-a",
			watermarkEventTypeGoal, 1)
		assert.Len(t, rec.calls, 1)
	})
	t.Run("nil recorder and zero watermark are no-ops", func(t *testing.T) {
		recordWatermark(context.Background(), zap.NewNop(), nil, "env-a", 1, 1)
	})
}
