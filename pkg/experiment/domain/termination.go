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
	"time"

	pkgErr "github.com/bucketeer-io/bucketeer/v2/pkg/error"
	"github.com/bucketeer-io/bucketeer/v2/pkg/uuid"
	experimentproto "github.com/bucketeer-io/bucketeer/v2/proto/experiment"
	featureproto "github.com/bucketeer-io/bucketeer/v2/proto/feature"
)

const (
	// TerminationReasonScheduled is the stop triggered by the scheduled stop
	// time. The experiment ends in Experiment_STOPPED.
	TerminationReasonScheduled int32 = 1
	// TerminationReasonForced is the early (manual) stop triggered by a user.
	// The experiment ends in Experiment_FORCE_STOPPED.
	TerminationReasonForced int32 = 2

	// Event type watermarks tracked in the event_watermark table.
	WatermarkEventTypeEvaluation int32 = 1
	WatermarkEventTypeGoal       int32 = 2

	// Termination phases. A termination always moves forward; every transition
	// is persisted, so a crashed or restarted owner resumes from the stored
	// phase instead of restarting the whole finalization.
	TerminationPhaseWaitingWatermark int32 = 1
	TerminationPhaseComputing        int32 = 2
	TerminationPhaseResultSaved      int32 = 3
	TerminationPhaseConfirmed        int32 = 4
	TerminationPhaseCachePublished   int32 = 5

	// microsecondsPerSecond is used when translating the fixed termination
	// boundary (epoch seconds) to event watermarks (epoch microseconds).
	microsecondsPerSecond int64 = 1_000_000
)

var (
	ErrTerminationInvalid = pkgErr.NewErrorInvalidArgNotMatchFormat(
		pkgErr.ExperimentPackageName,
		"termination is invalid",
		"termination",
	)
	ErrTerminationPhaseInvalid = pkgErr.NewErrorInvalidArgNotMatchFormat(
		pkgErr.ExperimentPackageName,
		"termination phase is invalid",
		"phase",
	)
	ErrComputeOwnerHeld = pkgErr.NewErrorAlreadyExists(
		pkgErr.ExperimentPackageName,
		"computation is owned by another owner",
	)
)

// ConfigSnapshot is the experiment configuration fixed at termination
// initiation. The final calculation only ever reads this snapshot, so any
// number of compute owners (including an owner taking over after a lock loss)
// deterministically compute over the exact same input.
type ConfigSnapshot struct {
	ExperimentID     string                        `json:"experiment_id"`
	FeatureID        string                        `json:"feature_id"`
	FeatureVersion   int32                         `json:"feature_version"`
	Variations       []*featureproto.Variation     `json:"variations"`
	GoalIDs          []string                      `json:"goal_ids"`
	StartAt          int64                         `json:"start_at"`
	StopAt           int64                         `json:"stop_at"`
	OriginalStopAt   int64                         `json:"original_stop_at"`
	BaseVariationID  string                        `json:"base_variation_id"`
}

// VariationInputSummary is the sufficient-statistic input of the final
// calculation for one variation of one goal, captured at the fixed watermark.
type VariationInputSummary struct {
	VariationID         string  `json:"variation_id"`
	EvaluationUserCount int64   `json:"evaluation_user_count"`
	EvaluationEventCount int64  `json:"evaluation_event_count"`
	GoalUserCount       int64   `json:"goal_user_count"`
	GoalEventCount      int64   `json:"goal_event_count"`
	GoalValueSum        float64 `json:"goal_value_sum"`
}

// GoalInputSummary is the per-goal slice of the final calculation input.
type GoalInputSummary struct {
	GoalID     string                   `json:"goal_id"`
	Variations []*VariationInputSummary `json:"variations"`
}

// InputSummary is attached to the final statistics. Together with the snapshot
// and the watermarks it proves which input produced the confirmed result.
type InputSummary struct {
	EndAt                     int64                `json:"end_at"`
	EvaluationWatermark       int64                `json:"evaluation_watermark"`
	GoalWatermark             int64                `json:"goal_watermark"`
	Goals                     []*GoalInputSummary  `json:"goals"`
	TotalEvaluationUserCount  int64                `json:"total_evaluation_user_count"`
	CalculatedAt              int64                `json:"calculated_at"`
}

// Termination is the persistent state machine of one experiment finalization.
type Termination struct {
	ID                  string
	EnvironmentID       string
	ExperimentID        string
	Reason              int32
	Phase               int32
	Snapshot            *ConfigSnapshot
	EvaluationWatermark int64
	GoalWatermark       int64
	InputSummary        *InputSummary
	FinalResultID       string
	OwnerID             string
	OwnerExpiresAt      int64
	Attempts            int32
	LastError           string
	CreatedAt           int64
	UpdatedAt           int64
	FinalizedAt         int64
}

// WatermarkPoint is a subscriber high-water mark at the time of reading.
// Timestamps are epoch microseconds.
type WatermarkPoint struct {
	// HighWaterAt is the highest event timestamp known to be durably
	// persisted. It is zero when no event has been persisted yet.
	HighWaterAt int64
	// UpdatedAt is when the subscriber last advanced/heartbeat-ed the
	// watermark row.
	UpdatedAt int64
	// Missing is true when no watermark row exists yet (subscriber not
	// producing watermarks yet, or an environment with no events).
	Missing bool
}

// NewTermination creates the stable termination identity for an experiment
// stop. Both the scheduled stop and an early stop go through this constructor;
// the (environment_id, experiment_id) unique constraint in the storage makes
// repeated triggers resume the same termination.
func NewTermination(
	environmentID string,
	experiment *experimentproto.Experiment,
	reason int32,
	now int64,
) (*Termination, error) {
	if experiment == nil || environmentID == "" {
		return nil, ErrTerminationInvalid
	}
	if reason != TerminationReasonScheduled && reason != TerminationReasonForced {
		return nil, ErrTerminationInvalid
	}
	id, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}
	snapshot := NewConfigSnapshot(experiment, reason, now)
	return &Termination{
		ID:                  id.String(),
		EnvironmentID:       environmentID,
		ExperimentID:        experiment.Id,
		Reason:              reason,
		Phase:               TerminationPhaseWaitingWatermark,
		Snapshot:            snapshot,
		// The watermark target is the initiation wall time: everything the
		// subscriber persists up to that point must be included in the final
		// statistics.
		EvaluationWatermark: now * microsecondsPerSecond,
		GoalWatermark:       now * microsecondsPerSecond,
		CreatedAt:           now,
		UpdatedAt:           now,
	}, nil
}

// NewConfigSnapshot fixes the experiment configuration at termination
// initiation. For an early stop, the counting boundary (StopAt) is the
// initiation time; the scheduled stop keeps the experiment's scheduled stop
// time because later evaluation events are dropped by the subscriber.
func NewConfigSnapshot(
	experiment *experimentproto.Experiment,
	reason int32,
	now int64,
) *ConfigSnapshot {
	stopAt := experiment.StopAt
	if reason == TerminationReasonForced && now < stopAt {
		stopAt = now
	}
	return &ConfigSnapshot{
		ExperimentID:    experiment.Id,
		FeatureID:       experiment.FeatureId,
		FeatureVersion:  experiment.FeatureVersion,
		Variations:      experiment.Variations,
		GoalIDs:         experiment.GoalIds,
		StartAt:         experiment.StartAt,
		StopAt:          stopAt,
		OriginalStopAt:  experiment.StopAt,
		BaseVariationID: experiment.BaseVariationId,
	}
}

// SnapshotExperiment rebuilds the experiment proto that the calculator must
// consume: the fixed variations/goals with the counting boundary StopAt.
func (t *Termination) SnapshotExperiment() *experimentproto.Experiment {
	return &experimentproto.Experiment{
		Id:              t.Snapshot.ExperimentID,
		FeatureId:       t.Snapshot.FeatureID,
		FeatureVersion:  t.Snapshot.FeatureVersion,
		Variations:      t.Snapshot.Variations,
		GoalIds:         t.Snapshot.GoalIDs,
		StartAt:         t.Snapshot.StartAt,
		StopAt:          t.Snapshot.StopAt,
		BaseVariationId: t.Snapshot.BaseVariationID,
		Status:          experimentproto.Experiment_FINALIZING,
		TerminationId:   t.ID,
	}
}

// WatermarksReady reports whether the subscriber has persisted every event up
// to the fixed watermarks.
//
// The primary condition is both high-water marks reaching the fixed targets.
// Environments with no events (or a subscriber that has not produced a
// watermark row yet) never reach a high-water target; for those, readiness is
// granted through a settle window, but only while the known watermark rows are
// fresh: a stale heartbeat means the subscriber is behind and the termination
// must keep waiting (it stays FINALIZING).
func (t *Termination) WatermarksReady(
	evaluation, goal WatermarkPoint,
	nowMicro int64,
	settleWindowMicro int64,
	heartbeatMaxAgeMicro int64,
) bool {
	if evaluation.HighWaterAt >= t.EvaluationWatermark &&
		goal.HighWaterAt >= t.GoalWatermark {
		return true
	}
	if nowMicro-t.EvaluationWatermark < settleWindowMicro {
		return false
	}
	fresh := func(p WatermarkPoint) bool {
		return p.Missing || nowMicro-p.UpdatedAt <= heartbeatMaxAgeMicro
	}
	return fresh(evaluation) && fresh(goal)
}

// AdvanceToComputing moves the termination from the watermark waiting phase
// into the compute phase and binds the compute owner lease.
func (t *Termination) AdvanceToComputing(
	ownerID string,
	ownerTTLSeconds int64,
	now int64,
) error {
	if ownerID == "" {
		return ErrTerminationInvalid
	}
	if t.Phase != TerminationPhaseWaitingWatermark {
		return ErrTerminationPhaseInvalid
	}
	t.Phase = TerminationPhaseComputing
	t.OwnerID = ownerID
	t.OwnerExpiresAt = now + ownerTTLSeconds
	t.Attempts++
	t.UpdatedAt = now
	return nil
}

// TakeOverOrRenewCompute handles compute ownership during the computing phase.
// The same owner renews its lease; a new owner may take over only after the
// lease expired (owner crash / loss), in which case the final result is
// recomputed from the same snapshot input.
func (t *Termination) TakeOverOrRenewCompute(
	ownerID string,
	ownerTTLSeconds int64,
	now int64,
) error {
	if t.Phase != TerminationPhaseComputing {
		return ErrTerminationPhaseInvalid
	}
	switch {
	case t.OwnerID == ownerID:
		t.OwnerExpiresAt = now + ownerTTLSeconds
		t.UpdatedAt = now
		return nil
	case now > t.OwnerExpiresAt:
		t.OwnerID = ownerID
		t.OwnerExpiresAt = now + ownerTTLSeconds
		t.Attempts++
		t.UpdatedAt = now
		return nil
	default:
		return ErrComputeOwnerHeld
	}
}

// MarkResultSaved records the final statistics input summary and the saved
// result, then releases the compute owner. Only the current owner may do it.
func (t *Termination) MarkResultSaved(
	ownerID string,
	summary *InputSummary,
	finalResultID string,
	now int64,
) error {
	if t.Phase != TerminationPhaseComputing {
		return ErrTerminationPhaseInvalid
	}
	if t.OwnerID != ownerID {
		return ErrComputeOwnerHeld
	}
	if finalResultID == "" {
		return ErrFinalResultIDRequired
	}
	t.Phase = TerminationPhaseResultSaved
	t.InputSummary = summary
	t.FinalResultID = finalResultID
	t.OwnerID = ""
	t.OwnerExpiresAt = 0
	t.UpdatedAt = now
	return nil
}

// MarkConfirmed records that the terminal experiment status, the domain event
// and the confirmed final result are committed transactionally.
func (t *Termination) MarkConfirmed(now int64) error {
	if t.Phase != TerminationPhaseResultSaved {
		return ErrTerminationPhaseInvalid
	}
	t.Phase = TerminationPhaseConfirmed
	t.FinalizedAt = now
	t.UpdatedAt = now
	return nil
}

// MarkCachePublished is the terminal phase: the experiments cache no longer
// attributes events to this experiment and holds state consistent with the
// snapshot.
func (t *Termination) MarkCachePublished(now int64) error {
	if t.Phase != TerminationPhaseConfirmed {
		return ErrTerminationPhaseInvalid
	}
	t.Phase = TerminationPhaseCachePublished
	t.UpdatedAt = now
	return nil
}

// IsDone reports whether the whole finalization completed.
func (t *Termination) IsDone() bool {
	return t.Phase == TerminationPhaseCachePublished
}

// RecordError stores the last phase error for observability without changing
// the phase; the next run retries from the stored phase.
func (t *Termination) RecordError(message string, now int64) {
	t.LastError = message
	t.UpdatedAt = now
}

// TerminalStatusProto returns the experiment status the confirmed termination
// ends in.
func (t *Termination) TerminalStatusProto() experimentproto.Experiment_Status {
	if t.Reason == TerminationReasonForced {
		return experimentproto.Experiment_FORCE_STOPPED
	}
	return experimentproto.Experiment_STOPPED
}

// TerminationOwnerTTL returns the compute lease duration. The lease must be
// longer than one final calculation round so another owner only takes over
// after an actual crash.
func TerminationOwnerTTL() time.Duration {
	return 10 * time.Minute
}
