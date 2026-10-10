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

// Package finalizer runs the experiment finalization pipeline.
//
// An experiment stops by entering FINALIZING with a stable termination
// identity. This job drives each termination through its persisted phases:
// wait for the evaluation/goal watermarks, compute the final statistics once
// from the fixed snapshot (owner lease, recompute on lock loss), confirm the
// result through the experiment service, and publish an experiments cache
// consistent with the snapshot. Every phase transition is compare-and-swap,
// so a crashed/restarted owner resumes the stored phase and a later phase
// failure only retries that phase.
package finalizer

import (
	"context"
	"os"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/bucketeer-io/bucketeer/v2/pkg/batch/jobs"
	cachev3 "github.com/bucketeer-io/bucketeer/v2/pkg/cache/v3"
	environmentclient "github.com/bucketeer-io/bucketeer/v2/pkg/environment/client"
	operationaldatabase "github.com/bucketeer-io/bucketeer/v2/pkg/eventcounter/storage/v2/operational_database"
	experimentclient "github.com/bucketeer-io/bucketeer/v2/pkg/experiment/client"
	experimentdomain "github.com/bucketeer-io/bucketeer/v2/pkg/experiment/domain"
	v2es "github.com/bucketeer-io/bucketeer/v2/pkg/experiment/storage/v2"
	calculatordomain "github.com/bucketeer-io/bucketeer/v2/pkg/experimentcalculator/domain"
	v2ecs "github.com/bucketeer-io/bucketeer/v2/pkg/experimentcalculator/storage/v2"
	"github.com/bucketeer-io/bucketeer/v2/pkg/log"
	environmentproto "github.com/bucketeer-io/bucketeer/v2/proto/environment"
	experimentproto "github.com/bucketeer-io/bucketeer/v2/proto/experiment"
)

const (
	listRequestSize = 500
	day             = 24 * time.Hour

	// Settle window before a no-longer-advancing (or still missing) watermark
	// is considered caught up. This bounds the wait for environments with no
	// events while delayed clients keep the experiment in FINALIZING.
	defaultWatermarkSettleWindow = 2 * time.Minute
	// A watermark heartbeat older than this means the subscriber is behind;
	// the finalization keeps waiting instead of using partial data.
	defaultWatermarkHeartbeatMaxAge = 2 * time.Minute
)

// finalCalculator computes the final statistics from a fixed snapshot.
type finalCalculator interface {
	RunFinal(
		ctx context.Context,
		environmentID string,
		experiment *experimentproto.Experiment,
	) (*calculatordomain.ExperimentResult, *experimentdomain.InputSummary, error)
}

type experimentFinalizer struct {
	environmentClient  environmentclient.Client
	experimentClient   experimentclient.Client
	terminationStorage v2es.TerminationStorage
	watermarkStorage   operationaldatabase.EventWatermarkStorage
	calculator         finalCalculator
	resultStorage      v2ecs.ExperimentResultStorage
	experimentCaches   []cachev3.ExperimentsCache

	settleWindow    time.Duration
	heartbeatMaxAge time.Duration
	ownerID         string
	location        *time.Location
	opts            *jobs.Options
	logger          *zap.Logger
}

// NewExperimentFinalizer creates the experiment finalization batch job.
func NewExperimentFinalizer(
	environmentClient environmentclient.Client,
	experimentClient experimentclient.Client,
	terminationStorage v2es.TerminationStorage,
	watermarkStorage operationaldatabase.EventWatermarkStorage,
	calculator finalCalculator,
	resultStorage v2ecs.ExperimentResultStorage,
	experimentCaches []cachev3.ExperimentsCache,
	location *time.Location,
	opts ...jobs.Option,
) jobs.Job {
	dopts := &jobs.Options{
		Timeout: 1 * time.Minute,
		Logger:  zap.NewNop(),
	}
	for _, opt := range opts {
		opt(dopts)
	}
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	return &experimentFinalizer{
		environmentClient:  environmentClient,
		experimentClient:   experimentClient,
		terminationStorage: terminationStorage,
		watermarkStorage:   watermarkStorage,
		calculator:         calculator,
		resultStorage:      resultStorage,
		experimentCaches:   experimentCaches,
		settleWindow:       defaultWatermarkSettleWindow,
		heartbeatMaxAge:    defaultWatermarkHeartbeatMaxAge,
		ownerID:            hostname,
		location:           location,
		opts:               dopts,
		logger:             dopts.Logger.Named("experiment-finalizer"),
	}
}

func (f *experimentFinalizer) Run(ctx context.Context) error {
	startTime := time.Now()
	var lastErr error
	environments, err := f.listEnvironments(ctx)
	if err != nil {
		jobs.RecordJob(jobs.JobExperimentFinalizer, err, time.Since(startTime))
		return err
	}
	for _, env := range environments {
		if err := f.runEnvironment(ctx, env.Id); err != nil {
			f.logger.Error("Failed to finalize experiments in environment",
				log.FieldsFromIncomingContext(ctx).AddFields(
					zap.String("environmentId", env.Id),
					zap.Error(err),
				)...,
			)
			lastErr = err
		}
	}
	jobs.RecordJob(jobs.JobExperimentFinalizer, lastErr, time.Since(startTime))
	return lastErr
}

func (f *experimentFinalizer) runEnvironment(ctx context.Context, environmentID string) error {
	terminations, err := f.terminationStorage.ListTerminationsByPhases(ctx, environmentID,
		[]int32{
			experimentdomain.TerminationPhaseWaitingWatermark,
			experimentdomain.TerminationPhaseComputing,
			experimentdomain.TerminationPhaseResultSaved,
			experimentdomain.TerminationPhaseConfirmed,
		})
	if err != nil {
		return err
	}
	var lastErr error
	for _, tn := range terminations {
		if err := f.runTermination(ctx, environmentID, tn); err != nil {
			f.logger.Error("Failed to advance experiment termination",
				log.FieldsFromIncomingContext(ctx).AddFields(
					zap.String("environmentId", environmentID),
					zap.String("experimentId", tn.ExperimentID),
					zap.Int32("phase", tn.Phase),
					zap.Error(err),
				)...,
			)
			lastErr = err
		}
	}
	return lastErr
}

// runTermination advances a single termination by at most one phase per tick.
func (f *experimentFinalizer) runTermination(
	ctx context.Context,
	environmentID string,
	tn *experimentdomain.Termination,
) error {
	switch tn.Phase {
	case experimentdomain.TerminationPhaseWaitingWatermark:
		return f.waitWatermarkAndStartCompute(ctx, tn)
	case experimentdomain.TerminationPhaseComputing:
		return f.computeAndSaveResult(ctx, tn)
	case experimentdomain.TerminationPhaseResultSaved:
		return f.confirmResult(ctx, environmentID, tn)
	case experimentdomain.TerminationPhaseConfirmed:
		return f.publishCache(ctx, environmentID, tn)
	default:
		return nil
	}
}

func (f *experimentFinalizer) waitWatermarkAndStartCompute(
	ctx context.Context,
	tn *experimentdomain.Termination,
) error {
	now := time.Now().In(f.location)
	evaluation, err := f.getWatermarkPoint(ctx, tn.EnvironmentID,
		experimentdomain.WatermarkEventTypeEvaluation)
	if err != nil {
		return err
	}
	goal, err := f.getWatermarkPoint(ctx, tn.EnvironmentID,
		experimentdomain.WatermarkEventTypeGoal)
	if err != nil {
		return err
	}
	if !tn.WatermarksReady(
		evaluation, goal,
		now.UnixMicro(),
		f.settleWindow.Microseconds(),
		f.heartbeatMaxAge.Microseconds(),
	) {
		// The wait timed out for this tick: the experiment stays FINALIZING
		// and the next run retries the exact same watermark check.
		f.logger.Info("Watermarks not ready, experiment remains finalizing",
			zap.String("environmentId", tn.EnvironmentID),
			zap.String("experimentId", tn.ExperimentID),
			zap.Int64("evaluationWatermark", evaluation.HighWaterAt),
			zap.Int64("goalWatermark", goal.HighWaterAt),
			zap.Int64("targetEvaluation", tn.EvaluationWatermark),
			zap.Int64("targetGoal", tn.GoalWatermark),
		)
		return nil
	}
	if err := tn.AdvanceToComputing(
		f.ownerID,
		int64(experimentdomain.TerminationOwnerTTL()/time.Second),
		now.Unix(),
	); err != nil {
		return err
	}
	return f.terminationStorage.UpdateTerminationCAS(
		ctx, tn, experimentdomain.TerminationPhaseWaitingWatermark, "")
}

func (f *experimentFinalizer) getWatermarkPoint(
	ctx context.Context,
	environmentID string,
	eventType int32,
) (experimentdomain.WatermarkPoint, error) {
	high, updated, found, err := f.watermarkStorage.GetEventWatermark(
		ctx, environmentID, eventType)
	if err != nil {
		return experimentdomain.WatermarkPoint{}, err
	}
	return experimentdomain.WatermarkPoint{
		HighWaterAt: high,
		UpdatedAt:   updated,
		Missing:     !found,
	}, nil
}

func (f *experimentFinalizer) computeAndSaveResult(
	ctx context.Context,
	tn *experimentdomain.Termination,
) error {
	now := time.Now().In(f.location).Unix()
	ttlSeconds := int64(experimentdomain.TerminationOwnerTTL() / time.Second)
	// Renew the lease when this owner holds it, take over when the lease
	// expired (crashed owner), or skip while another owner is active.
	if err := tn.TakeOverOrRenewCompute(f.ownerID, ttlSeconds, now); err != nil {
		if err == experimentdomain.ErrComputeOwnerHeld {
			return nil
		}
		return err
	}
	if err := f.terminationStorage.UpdateTerminationCAS(
		ctx, tn, experimentdomain.TerminationPhaseComputing, "",
	); err != nil {
		// Another owner already advanced or renewed: reload next tick.
		if err == v2es.ErrTerminationPhaseConflict {
			return nil
		}
		return err
	}

	// Compute from the fixed snapshot: a new owner after a lock loss uses
	// exactly the same input.
	result, summary, err := f.calculator.RunFinal(ctx, tn.EnvironmentID, tn.SnapshotExperiment())
	if err != nil {
		tn.RecordError(err.Error(), now)
		_ = f.terminationStorage.UpdateTerminationCAS(
			ctx, tn, experimentdomain.TerminationPhaseComputing, "")
		return err
	}
	// Persist the final result (idempotent upsert; a duplicate compute
	// overwrites with the deterministic same statistics).
	if err := f.resultStorage.UpdateExperimentResult(ctx, tn.EnvironmentID, result); err != nil {
		tn.RecordError(err.Error(), now)
		_ = f.terminationStorage.UpdateTerminationCAS(
			ctx, tn, experimentdomain.TerminationPhaseComputing, "")
		return err
	}
	summary.EvaluationWatermark = tn.EvaluationWatermark
	summary.GoalWatermark = tn.GoalWatermark
	summary.CalculatedAt = now
	if err := tn.MarkResultSaved(f.ownerID, summary, tn.ExperimentID, now); err != nil {
		return err
	}
	if err := f.terminationStorage.UpdateTerminationCAS(
		ctx, tn, experimentdomain.TerminationPhaseComputing, f.ownerID,
	); err != nil {
		// A competing owner won the phase: leave it, retry next tick.
		if err == v2es.ErrTerminationPhaseConflict {
			return nil
		}
		return err
	}
	f.logger.Info("Final result saved",
		zap.String("environmentId", tn.EnvironmentID),
		zap.String("experimentId", tn.ExperimentID),
	)
	return nil
}

func (f *experimentFinalizer) confirmResult(
	ctx context.Context,
	environmentID string,
	tn *experimentdomain.Termination,
) error {
	// Confirmation is transactional and idempotent in the experiment service:
	// the experiment moves to its terminal status bound to the final result
	// and the EXPERIMENT_FINISHED event is published in the same transaction.
	resp, err := f.experimentClient.UpdateExperiment(ctx,
		&experimentproto.UpdateExperimentRequest{
			Id:            tn.ExperimentID,
			EnvironmentId: environmentID,
			ConfirmFinalization: &experimentproto.UpdateExperimentRequest_ConfirmFinalization{
				TerminationId: tn.ID,
				FinalResultId: tn.ExperimentID,
			},
		})
	if err != nil {
		return err
	}
	if resp.GetExperiment().GetStatus() != experimentproto.Experiment_STOPPED &&
		resp.GetExperiment().GetStatus() != experimentproto.Experiment_FORCE_STOPPED {
		// Not confirmed yet (e.g. another owner still computing): retry later.
		return nil
	}
	now := time.Now().In(f.location).Unix()
	if err := tn.MarkConfirmed(now); err != nil {
		return err
	}
	if err := f.terminationStorage.UpdateTerminationCAS(
		ctx, tn, experimentdomain.TerminationPhaseResultSaved, "",
	); err != nil {
		if err == v2es.ErrTerminationPhaseConflict {
			return nil
		}
		return err
	}
	return nil
}

func (f *experimentFinalizer) publishCache(
	ctx context.Context,
	environmentID string,
	tn *experimentdomain.Termination,
) error {
	// Rebuild the environment experiments cache so it no longer contains
	// the now-confirmed experiment: its events stop being attributed and the
	// cached state matches the final snapshot. This also retries on its own:
	// a failed cache publish keeps the termination in CONFIRMED.
	resp, err := f.experimentClient.ListExperiments(ctx, &experimentproto.ListExperimentsRequest{
		StopAt:        time.Now().In(f.location).Add(-2 * day).Unix(),
		PageSize:      0,
		EnvironmentId: environmentID,
		Statuses: []experimentproto.Experiment_Status{
			experimentproto.Experiment_RUNNING,
			experimentproto.Experiment_FINALIZING,
			experimentproto.Experiment_STOPPED,
		},
		ExcludeFinalized: true,
		Archived:         wrapperspb.Bool(false),
	})
	if err != nil {
		return err
	}
	experiments := &experimentproto.Experiments{Experiments: resp.Experiments}
	for _, c := range f.experimentCaches {
		if err := c.Put(experiments, environmentID); err != nil {
			return err
		}
	}
	now := time.Now().In(f.location).Unix()
	if err := tn.MarkCachePublished(now); err != nil {
		return err
	}
	if err := f.terminationStorage.UpdateTerminationCAS(
		ctx, tn, experimentdomain.TerminationPhaseConfirmed, "",
	); err != nil {
		if err == v2es.ErrTerminationPhaseConflict {
			return nil
		}
		return err
	}
	f.logger.Info("Experiment finalization completed",
		zap.String("environmentId", environmentID),
		zap.String("experimentId", tn.ExperimentID),
	)
	return nil
}

func (f *experimentFinalizer) listEnvironments(ctx context.Context) ([]*environmentproto.EnvironmentV2, error) {
	var envs []*environmentproto.EnvironmentV2
	cursor := ""
	for {
		resp, err := f.environmentClient.ListEnvironmentsV2(ctx,
			&environmentproto.ListEnvironmentsV2Request{
				PageSize: listRequestSize,
				Cursor:   cursor,
				Archived: wrapperspb.Bool(false),
			})
		if err != nil {
			return nil, err
		}
		envs = append(envs, resp.Environments...)
		if len(resp.Environments) < listRequestSize {
			return envs, nil
		}
		cursor = resp.Cursor
	}
}
