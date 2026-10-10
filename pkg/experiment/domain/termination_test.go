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

package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	experimentproto "github.com/bucketeer-io/bucketeer/v2/proto/experiment"
	featureproto "github.com/bucketeer-io/bucketeer/v2/proto/feature"
)

func newTerminationExperiment() *experimentproto.Experiment {
	return &experimentproto.Experiment{
		Id:             "experiment-id",
		FeatureId:      "feature-id",
		FeatureVersion: 3,
		Variations: []*featureproto.Variation{
			{Id: "var-a", Value: "A"},
			{Id: "var-b", Value: "B"},
		},
		GoalIds:         []string{"goal-1", "goal-2"},
		StartAt:         100,
		StopAt:          200,
		BaseVariationId: "var-a",
		Status:          experimentproto.Experiment_RUNNING,
	}
}

func TestNewTermination(t *testing.T) {
	t.Parallel()
	patterns := []struct {
		desc        string
		reason      int32
		expectedErr error
	}{
		{desc: "scheduled", reason: TerminationReasonScheduled, expectedErr: nil},
		{desc: "forced", reason: TerminationReasonForced, expectedErr: nil},
		{desc: "invalid reason", reason: 99, expectedErr: ErrTerminationInvalid},
	}
	for _, p := range patterns {
		t.Run(p.desc, func(t *testing.T) {
			tn, err := NewTermination("env-id", newTerminationExperiment(), p.reason, 300)
			if p.expectedErr != nil {
				assert.Equal(t, p.expectedErr, err)
				assert.Nil(t, tn)
				return
			}
			require.NoError(t, err)
			assert.NotEmpty(t, tn.ID)
			assert.Equal(t, "env-id", tn.EnvironmentID)
			assert.Equal(t, "experiment-id", tn.ExperimentID)
			assert.Equal(t, p.reason, tn.Reason)
			assert.Equal(t, TerminationPhaseWaitingWatermark, tn.Phase)
			assert.Equal(t, int64(300)*microsecondsPerSecond, tn.EvaluationWatermark)
			assert.Equal(t, int64(300)*microsecondsPerSecond, tn.GoalWatermark)
		})
	}
}

func TestNewTerminationMissingFields(t *testing.T) {
	t.Parallel()
	_, err := NewTermination("", newTerminationExperiment(), TerminationReasonScheduled, 1)
	assert.Equal(t, ErrTerminationInvalid, err)
	_, err = NewTermination("env", nil, TerminationReasonScheduled, 1)
	assert.Equal(t, ErrTerminationInvalid, err)
}

func TestConfigSnapshotStopAt(t *testing.T) {
	t.Parallel()
	patterns := []struct {
		desc       string
		reason     int32
		now        int64
		expected   int64
	}{
		{desc: "scheduled keeps scheduled stop", reason: TerminationReasonScheduled, now: 201, expected: 200},
		{desc: "forced before scheduled stop uses initiation time", reason: TerminationReasonForced, now: 150, expected: 150},
		{desc: "forced after scheduled stop keeps stop", reason: TerminationReasonForced, now: 220, expected: 200},
	}
	for _, p := range patterns {
		t.Run(p.desc, func(t *testing.T) {
			snap := NewConfigSnapshot(newTerminationExperiment(), p.reason, p.now)
			assert.Equal(t, int64(200), snap.OriginalStopAt)
			assert.Equal(t, p.expected, snap.StopAt)
		})
	}
}

func TestSnapshotExperiment(t *testing.T) {
	t.Parallel()
	tn, err := NewTermination("env-id", newTerminationExperiment(), TerminationReasonForced, 150)
	require.NoError(t, err)
	snapExp := tn.SnapshotExperiment()
	assert.Equal(t, "experiment-id", snapExp.Id)
	assert.Equal(t, int64(100), snapExp.StartAt)
	assert.Equal(t, int64(150), snapExp.StopAt)
	assert.Equal(t, experimentproto.Experiment_FINALIZING, snapExp.Status)
	assert.Equal(t, tn.ID, snapExp.TerminationId)
	assert.Len(t, snapExp.Variations, 2)
	assert.Equal(t, []string{"goal-1", "goal-2"}, snapExp.GoalIds)
}

func TestWatermarksReady(t *testing.T) {
	t.Parallel()
	tn, err := NewTermination("env-id", newTerminationExperiment(), TerminationReasonScheduled, 100)
	require.NoError(t, err)
	target := int64(100) * microsecondsPerSecond
	const (
		settle    = int64(60) * microsecondsPerSecond
		heartbeat = int64(2) * microsecondsPerSecond
	)
	patterns := []struct {
		desc       string
		eval       WatermarkPoint
		goal       WatermarkPoint
		now        int64

		expected   bool
	}{
		{
			desc:     "both high waters reached",
			eval:     WatermarkPoint{HighWaterAt: target, UpdatedAt: target},
			goal:     WatermarkPoint{HighWaterAt: target + 1, UpdatedAt: target},
			now:      target + 1,
			expected: true,
		},
		{
			desc:     "evaluation watermark behind, no settle elapsed yet",
			eval:     WatermarkPoint{HighWaterAt: target - 10, UpdatedAt: target},
			goal:     WatermarkPoint{HighWaterAt: target, UpdatedAt: target},
			now:      target + 10,
			expected: false,
		},
		{
			desc:     "behind but settle elapsed with fresh heartbeats (no events)",
			eval:     WatermarkPoint{HighWaterAt: 0, UpdatedAt: target + settle},
			goal:     WatermarkPoint{HighWaterAt: 0, UpdatedAt: target + settle},
			now:      target + settle + 1,
			expected: true,
		},
		{
			desc:     "behind and settle elapsed but subscriber heartbeat stale",
			eval:     WatermarkPoint{HighWaterAt: 0, UpdatedAt: target},
			goal:     WatermarkPoint{HighWaterAt: 0, UpdatedAt: target},
			now:      target + settle + 1,
			expected: false,
		},
		{
			desc:     "missing watermark rows after settle (old subscriber / no events)",
			eval:     WatermarkPoint{Missing: true},
			goal:     WatermarkPoint{Missing: true},
			now:      target + settle + 1,
			expected: true,
		},
		{
			desc:     "one fresh one stale stays finalizing",
			eval:     WatermarkPoint{HighWaterAt: target, UpdatedAt: target},
			goal:     WatermarkPoint{HighWaterAt: 0, UpdatedAt: target},
			now:      target + settle + 1,
			expected: false,
		},
	}
	for _, p := range patterns {
		t.Run(p.desc, func(t *testing.T) {
			got := tn.WatermarksReady(p.eval, p.goal, p.now, settle, heartbeat)
			assert.Equal(t, p.expected, got)
		})
	}
}

func TestTerminationPhases(t *testing.T) {
	t.Parallel()
	const ttl = int64(600)
	newWaiting := func(t *testing.T) *Termination {
		t.Helper()
		tn, err := NewTermination("env-id", newTerminationExperiment(), TerminationReasonScheduled, 100)
		require.NoError(t, err)
		return tn
	}
	t.Run("advance to computing binds owner", func(t *testing.T) {
		tn := newWaiting(t)
		require.NoError(t, tn.AdvanceToComputing("owner-1", ttl, 101))
		assert.Equal(t, TerminationPhaseComputing, tn.Phase)
		assert.Equal(t, "owner-1", tn.OwnerID)
		assert.Equal(t, int64(101+ttl), tn.OwnerExpiresAt)
		assert.Equal(t, int32(1), tn.Attempts)
		// advancing twice from the waiting phase is rejected
		assert.Equal(t, ErrTerminationPhaseInvalid, tn.AdvanceToComputing("owner-2", ttl, 102))
	})
	t.Run("other owner rejected while lease valid", func(t *testing.T) {
		tn := newWaiting(t)
		require.NoError(t, tn.AdvanceToComputing("owner-1", ttl, 101))
		err := tn.TakeOverOrRenewCompute("owner-2", ttl, 102)
		assert.Equal(t, ErrComputeOwnerHeld, err)
		assert.Equal(t, "owner-1", tn.OwnerID)
	})
	t.Run("owner renews its lease", func(t *testing.T) {
		tn := newWaiting(t)
		require.NoError(t, tn.AdvanceToComputing("owner-1", ttl, 101))
		require.NoError(t, tn.TakeOverOrRenewCompute("owner-1", ttl, 200))
		assert.Equal(t, int64(200+ttl), tn.OwnerExpiresAt)
		assert.Equal(t, int32(1), tn.Attempts)
	})
	t.Run("new owner takes over after lease loss and recomputes same input", func(t *testing.T) {
		tn := newWaiting(t)
		require.NoError(t, tn.AdvanceToComputing("owner-1", ttl, 101))
		require.NoError(t, tn.TakeOverOrRenewCompute("owner-2", ttl, 101+ttl+1))
		assert.Equal(t, "owner-2", tn.OwnerID)
		assert.Equal(t, int32(2), tn.Attempts)
		// snapshot input is unchanged
		assert.Equal(t, int64(100), tn.Snapshot.StartAt)
		assert.Equal(t, int64(200), tn.Snapshot.StopAt)
	})
	t.Run("result saved, confirmed, cache published", func(t *testing.T) {
		tn := newWaiting(t)
		require.NoError(t, tn.AdvanceToComputing("owner-1", ttl, 101))
		summary := &InputSummary{EndAt: 200}
		require.NoError(t, tn.MarkResultSaved("owner-1", summary, "result-id", 102))
		assert.Equal(t, TerminationPhaseResultSaved, tn.Phase)
		assert.Equal(t, "", tn.OwnerID)
		assert.Equal(t, int64(0), tn.OwnerExpiresAt)
		assert.Equal(t, "result-id", tn.FinalResultID)
		// a stale owner cannot save again
		assert.Equal(t, ErrTerminationPhaseInvalid,
			tn.MarkResultSaved("owner-1", summary, "result-id", 103))
		require.NoError(t, tn.MarkConfirmed(103))
		assert.Equal(t, TerminationPhaseConfirmed, tn.Phase)
		assert.Equal(t, int64(103), tn.FinalizedAt)
		require.NoError(t, tn.MarkCachePublished(104))
		assert.True(t, tn.IsDone())
	})
	t.Run("result saved rejects wrong owner and empty result", func(t *testing.T) {
		tn := newWaiting(t)
		require.NoError(t, tn.AdvanceToComputing("owner-1", ttl, 101))
		assert.Equal(t, ErrComputeOwnerHeld,
			tn.MarkResultSaved("owner-2", &InputSummary{}, "rid", 102))
		assert.Equal(t, ErrFinalResultIDRequired,
			tn.MarkResultSaved("owner-1", &InputSummary{}, "", 102))
	})
	t.Run("phase guards", func(t *testing.T) {
		tn := newWaiting(t)
		assert.Equal(t, ErrTerminationPhaseInvalid, tn.MarkResultSaved("o", nil, "r", 1))
		assert.Equal(t, ErrTerminationPhaseInvalid, tn.MarkConfirmed(1))
		assert.Equal(t, ErrTerminationPhaseInvalid, tn.MarkCachePublished(1))
		assert.False(t, tn.IsDone())
	})
	t.Run("terminal status by reason", func(t *testing.T) {
		scheduled := newWaiting(t)
		assert.Equal(t, experimentproto.Experiment_STOPPED, scheduled.TerminalStatusProto())
		forced, err := NewTermination("env-id", newTerminationExperiment(), TerminationReasonForced, 100)
		require.NoError(t, err)
		assert.Equal(t, experimentproto.Experiment_FORCE_STOPPED, forced.TerminalStatusProto())
	})
}
