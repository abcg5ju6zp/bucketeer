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
	"math"
	"time"

	"github.com/jinzhu/copier"
	"google.golang.org/protobuf/types/known/wrapperspb"

	pkgErr "github.com/bucketeer-io/bucketeer/v2/pkg/error"
	"github.com/bucketeer-io/bucketeer/v2/pkg/uuid"
	experimentproto "github.com/bucketeer-io/bucketeer/v2/proto/experiment"
	featureproto "github.com/bucketeer-io/bucketeer/v2/proto/feature"
)

var (
	ErrExperimentBeforeStart = pkgErr.NewErrorInvalidArgNotMatchFormat(
		pkgErr.ExperimentPackageName,
		"start timestamp is greater than now",
		"start_at",
	)
	ErrExperimentBeforeStop = pkgErr.NewErrorInvalidArgNotMatchFormat(
		pkgErr.ExperimentPackageName,
		"stop timestamp is greater than now",
		"stop_at",
	)
	ErrExperimentStatusInvalid = pkgErr.NewErrorInvalidArgNotMatchFormat(
		pkgErr.ExperimentPackageName,
		"experiment status is invalid",
		"status",
	)
	ErrExperimentAlreadyStopped = pkgErr.NewErrorInvalidArgNotMatchFormat(
		pkgErr.ExperimentPackageName,
		"experiment is already stopped",
		"status",
	)
	ErrExperimentFinalizing = pkgErr.NewErrorInvalidArgNotMatchFormat(
		pkgErr.ExperimentPackageName,
		"experiment is finalizing, the configuration is fixed",
		"status",
	)
	ErrExperimentNotFinalizing = pkgErr.NewErrorInvalidArgNotMatchFormat(
		pkgErr.ExperimentPackageName,
		"experiment is not finalizing",
		"status",
	)
	ErrTerminationIdentityMismatch = pkgErr.NewErrorInvalidArgNotMatchFormat(
		pkgErr.ExperimentPackageName,
		"termination identity does not match",
		"termination_id",
	)
	ErrFinalResultIDRequired = pkgErr.NewErrorInvalidArgNotMatchFormat(
		pkgErr.ExperimentPackageName,
		"final result id is required",
		"final_result_id",
	)
	ErrExperimentStartIsAfterStop = pkgErr.NewErrorInvalidArgNotMatchFormat(
		pkgErr.ExperimentPackageName,
		"start is after stop timestamp",
		"start_at",
	)
	ErrExperimentStopIsBeforeStart = pkgErr.NewErrorInvalidArgNotMatchFormat(
		pkgErr.ExperimentPackageName,
		"stop is before start timestamp",
		"stop_at",
	)
	ErrExperimentStopIsBeforeNow = pkgErr.NewErrorInvalidArgNotMatchFormat(
		pkgErr.ExperimentPackageName,
		"stop is same or older than now timestamp",
		"stop_at",
	)
	ErrBaseVariationNotFound = pkgErr.NewErrorNotFound(
		pkgErr.ExperimentPackageName,
		"base variation not found",
		"base_variation_id",
	)
)

type Experiment struct {
	*experimentproto.Experiment
}

func NewExperiment(
	featureID string,
	featureVersion int32,
	variations []*featureproto.Variation,
	goalIDs []string,
	startAt int64,
	stopAt int64,
	name string,
	description string,
	baseVariationID string,
	maintainer string) (*Experiment, error) {

	id, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}
	if !validateBaseVariation(baseVariationID, variations) {
		return nil, ErrBaseVariationNotFound
	}
	goalIDs = removeDuplicated(goalIDs)
	now := time.Now().Unix()
	return &Experiment{
		&experimentproto.Experiment{
			Id:              id.String(),
			FeatureId:       featureID,
			FeatureVersion:  featureVersion,
			Variations:      variations,
			GoalIds:         goalIDs,
			StartAt:         startAt,
			StopAt:          stopAt,
			StoppedAt:       math.MaxInt64,
			CreatedAt:       now,
			UpdatedAt:       now,
			Name:            name,
			Description:     description,
			BaseVariationId: baseVariationID,
			Status:          experimentproto.Experiment_WAITING,
			Maintainer:      maintainer,
		},
	}, nil
}

func validateBaseVariation(v string, vs []*featureproto.Variation) bool {
	for i := range vs {
		if vs[i].Id == v {
			return true
		}
	}
	return false
}

func removeDuplicated(args []string) []string {
	results := make([]string, 0, len(args))
	encountered := map[string]bool{}
	for _, v := range args {
		if _, duplicated := encountered[v]; !duplicated {
			results = append(results, v)
			encountered[v] = true
		}
	}
	return results
}

func (e *Experiment) Update(
	name *wrapperspb.StringValue,
	description *wrapperspb.StringValue,
	startAt *wrapperspb.Int64Value,
	stopAt *wrapperspb.Int64Value,
	status *experimentproto.UpdateExperimentRequest_UpdatedStatus,
	archived *wrapperspb.BoolValue,
) (*Experiment, error) {
	// While the finalization is in progress the configuration is fixed by the
	// termination snapshot. Draft updates, archiving and status changes are
	// rejected; the experiment only leaves FINALIZING through
	// ConfirmFinalized.
	if e.Status == experimentproto.Experiment_FINALIZING {
		return nil, ErrExperimentFinalizing
	}
	updated := &Experiment{}
	err := copier.Copy(&updated, e)
	if err != nil {
		return nil, err
	}

	now := time.Now().Unix()

	if name != nil {
		updated.Name = name.Value
	}

	if description != nil {
		updated.Description = description.Value
	}

	if startAt != nil && stopAt != nil {
		err = e.validatePeriod(startAt.Value, stopAt.Value)
		if err != nil {
			return nil, err
		}
		updated.StartAt = startAt.Value
		updated.StopAt = stopAt.Value
	}

	if archived != nil {
		updated.Archived = archived.Value
	}

	if status != nil {
		err = updated.updateStatus(status.Status, now)
		if err != nil {
			return nil, err
		}
	}

	updated.UpdatedAt = now

	return updated, nil
}

func (e *Experiment) updateStatus(status experimentproto.Experiment_Status, now int64) error {
	switch status {
	case experimentproto.Experiment_RUNNING:
		if e.Status != experimentproto.Experiment_WAITING {
			return ErrExperimentStatusInvalid
		}
		e.StartAt = now
		e.Status = experimentproto.Experiment_RUNNING
	case experimentproto.Experiment_STOPPED:
		if e.Status != experimentproto.Experiment_WAITING && e.Status != experimentproto.Experiment_RUNNING {
			return ErrExperimentStatusInvalid
		}
		if e.StopAt > now {
			return ErrExperimentBeforeStop
		}
		e.Status = experimentproto.Experiment_STOPPED
		e.StoppedAt = now
	case experimentproto.Experiment_FORCE_STOPPED:
		if e.Status != experimentproto.Experiment_WAITING && e.Status != experimentproto.Experiment_RUNNING {
			return ErrExperimentStatusInvalid
		}
		e.Status = experimentproto.Experiment_FORCE_STOPPED
		e.StoppedAt = now
	default:
		return ErrExperimentStatusInvalid
	}

	return nil
}

func (e *Experiment) Start() error {
	if e.Status != experimentproto.Experiment_WAITING {
		return ErrExperimentStatusInvalid
	}
	now := time.Now().Unix()
	if e.StartAt > now {
		return ErrExperimentBeforeStart
	}
	e.Status = experimentproto.Experiment_RUNNING
	e.UpdatedAt = now
	return nil
}

func (e *Experiment) Finish() error {
	if e.Status != experimentproto.Experiment_WAITING && e.Status != experimentproto.Experiment_RUNNING {
		return ErrExperimentStatusInvalid
	}
	now := time.Now().Unix()
	if e.StopAt > now {
		return ErrExperimentBeforeStop
	}
	e.Status = experimentproto.Experiment_STOPPED
	e.UpdatedAt = now
	return nil
}

func (e *Experiment) Stop() error {
	if e.Stopped {
		return ErrExperimentAlreadyStopped
	}
	now := time.Now().Unix()
	e.Stopped = true
	e.Status = experimentproto.Experiment_FORCE_STOPPED
	e.StoppedAt = now
	e.UpdatedAt = now
	return nil
}

// EnterFinalizing moves an active experiment (WAITING or RUNNING) into the
// FINALIZING state and binds it to the stable termination identity. Both the
// scheduled stop and an early (manual) stop enter the experiment through this
// same method, so a repeated stop trigger can never produce a second final
// result. The configuration snapshot and the event watermarks are fixed by the
// caller and persisted on the termination record; the experiment row itself is
// not changed beyond the status and the termination identity.
func (e *Experiment) EnterFinalizing(terminationID string) error {
	if terminationID == "" {
		return ErrTerminationIdentityMismatch
	}
	if e.Status != experimentproto.Experiment_WAITING &&
		e.Status != experimentproto.Experiment_RUNNING {
		return ErrExperimentStatusInvalid
	}
	now := time.Now().Unix()
	e.Status = experimentproto.Experiment_FINALIZING
	e.TerminationId = terminationID
	e.UpdatedAt = now
	return nil
}

// CanInitiateFinalization reports whether a stop request (scheduled or forced)
// is allowed to initiate the finalization right now. When checkStopAt is true
// the experiment's scheduled stop time must have been reached.
func (e *Experiment) CanInitiateFinalization(checkStopAt bool, now int64) error {
	if e.Status != experimentproto.Experiment_WAITING &&
		e.Status != experimentproto.Experiment_RUNNING {
		return ErrExperimentStatusInvalid
	}
	if checkStopAt && e.StopAt > now {
		return ErrExperimentBeforeStop
	}
	return nil
}

// ConfirmFinalized is the only transition out of FINALIZING: it binds the
// terminal status to the confirmed final result referenced by finalResultID.
// The termination identity must match the one set when finalization started.
// forced decides between FORCE_STOPPED (early stop) and STOPPED (scheduled
// stop); both terminal statuses can only ever reference the result that was
// computed from the fixed termination snapshot.
func (e *Experiment) ConfirmFinalized(
	terminationID string,
	finalResultID string,
	forced bool,
) error {
	if e.Status != experimentproto.Experiment_FINALIZING {
		return ErrExperimentNotFinalizing
	}
	if e.TerminationId == "" || e.TerminationId != terminationID {
		return ErrTerminationIdentityMismatch
	}
	if finalResultID == "" {
		return ErrFinalResultIDRequired
	}
	now := time.Now().Unix()
	if forced {
		e.Status = experimentproto.Experiment_FORCE_STOPPED
	} else {
		e.Status = experimentproto.Experiment_STOPPED
	}
	e.FinalResultId = finalResultID
	e.StoppedAt = now
	e.UpdatedAt = now
	return nil
}

func (e *Experiment) ChangePeriod(startAt, stopAt int64) error {
	if err := e.validatePeriod(startAt, stopAt); err != nil {
		return err
	}
	e.StartAt = startAt
	e.StopAt = stopAt
	e.UpdatedAt = time.Now().Unix()
	return nil
}

func (e *Experiment) validatePeriod(startAt, stopAt int64) error {
	if startAt >= stopAt {
		return ErrExperimentStartIsAfterStop
	}
	if stopAt <= time.Now().Unix() {
		return ErrExperimentStopIsBeforeNow
	}
	return nil
}

func (e *Experiment) ChangeStartAt(startAt int64) error {
	if err := e.validateStartAt(startAt); err != nil {
		return err
	}
	e.StartAt = startAt
	e.UpdatedAt = time.Now().Unix()
	return nil
}

func (e *Experiment) validateStartAt(startAt int64) error {
	if startAt >= e.StopAt {
		return ErrExperimentStartIsAfterStop
	}
	return nil
}

func (e *Experiment) ChangeStopAt(stopAt int64) error {
	if err := e.validateStopAt(stopAt); err != nil {
		return err
	}
	e.StopAt = stopAt
	e.UpdatedAt = time.Now().Unix()
	return nil
}

func (e *Experiment) ChangeName(name string) error {
	e.Name = name
	e.UpdatedAt = time.Now().Unix()
	return nil
}

func (e *Experiment) ChangeDescription(description string) error {
	e.Description = description
	e.UpdatedAt = time.Now().Unix()
	return nil
}

func (e *Experiment) validateStopAt(stopAt int64) error {
	if stopAt <= e.StartAt {
		return ErrExperimentStopIsBeforeStart
	}
	if stopAt <= time.Now().Unix() {
		return ErrExperimentStopIsBeforeNow
	}
	return nil
}

func (e *Experiment) SetArchived() error {
	e.Archived = true
	e.UpdatedAt = time.Now().Unix()
	return nil
}

func (e *Experiment) SetDeleted() error {
	e.Deleted = true
	e.UpdatedAt = time.Now().Unix()
	return nil
}

// SyncGoalIDs syncs goalID and goalIDs.
// FIXME: This function is needed until admin UI implements multiple goals.
func SyncGoalIDs(goalID string, goalIDs []string) (string, []string) {
	if goalID == "" && len(goalIDs) == 0 {
		return "", nil
	}
	if goalID == "" {
		return goalIDs[0], goalIDs
	}
	if len(goalIDs) == 0 {
		return goalID, []string{goalID}
	}
	return goalID, goalIDs
}

// IsNotFinished returns true while the experiment is still in progress:
// WAITING, RUNNING, or FINALIZING. FINALIZING is deliberately treated as in
// progress so that the linked feature flag (its variations) cannot be edited
// after the termination snapshot was taken; the association is only released
// once the experiment reaches STOPPED/FORCE_STOPPED.
func (e *Experiment) IsNotFinished(_ time.Time) bool {
	if e.Deleted {
		return false
	}
	switch e.Status {
	case experimentproto.Experiment_WAITING,
		experimentproto.Experiment_RUNNING,
		experimentproto.Experiment_FINALIZING:
		return true
	default:
		return false
	}
}
