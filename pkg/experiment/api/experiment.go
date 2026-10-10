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

package api

import (
	"context"
	"errors"
	"strconv"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "google.golang.org/protobuf/proto"

	"github.com/bucketeer-io/bucketeer/v2/pkg/api/api"
	domainevent "github.com/bucketeer-io/bucketeer/v2/pkg/domainevent/domain"
	"github.com/bucketeer-io/bucketeer/v2/pkg/experiment/domain"
	v2es "github.com/bucketeer-io/bucketeer/v2/pkg/experiment/storage/v2"
	"github.com/bucketeer-io/bucketeer/v2/pkg/log"
	accountproto "github.com/bucketeer-io/bucketeer/v2/proto/account"
	eventproto "github.com/bucketeer-io/bucketeer/v2/proto/event/domain"
	proto "github.com/bucketeer-io/bucketeer/v2/proto/experiment"
	featureproto "github.com/bucketeer-io/bucketeer/v2/proto/feature"
)

const (
	maxExperimentPeriodDays = 30
	maxExperimentPeriod     = maxExperimentPeriodDays * 24 * 60 * 60
)

func (s *experimentService) GetExperiment(
	ctx context.Context,
	req *proto.GetExperimentRequest,
) (*proto.GetExperimentResponse, error) {
	_, err := s.checkEnvironmentRole(
		ctx, accountproto.AccountV2_Role_Environment_VIEWER,
		req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	if err := validateGetExperimentRequest(req); err != nil {
		return nil, err
	}
	experiment, err := s.experimentStorage.GetExperiment(ctx, req.Id, req.EnvironmentId)
	if err != nil {
		if errors.Is(err, v2es.ErrExperimentNotFound) {
			return nil, statusExperimentNotFound.Err()
		}
		return nil, api.NewGRPCStatus(err).Err()
	}
	return &proto.GetExperimentResponse{
		Experiment: experiment.Experiment,
	}, nil
}

func validateGetExperimentRequest(req *proto.GetExperimentRequest) error {
	if req.Id == "" {
		return statusExperimentIDRequired.Err()
	}
	return nil
}

func (s *experimentService) ListExperiments(
	ctx context.Context,
	req *proto.ListExperimentsRequest,
) (*proto.ListExperimentsResponse, error) {
	_, err := s.checkEnvironmentRole(
		ctx, accountproto.AccountV2_Role_Environment_VIEWER,
		req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	params := v2es.ListExperimentsParams{
		EnvironmentID:    req.EnvironmentId,
		FeatureID:        req.FeatureId,
		StartAt:          req.StartAt,
		StopAt:           req.StopAt,
		Maintainer:       req.Maintainer,
		Statuses:         req.Statuses,
		SearchKeyword:    req.SearchKeyword,
		OrderBy:          req.OrderBy,
		OrderDirection:  req.OrderDirection,
		PageSize:         int(req.PageSize),
		Cursor:           req.Cursor,
		ExcludeFinalized: req.ExcludeFinalized,
	}
	if req.Archived != nil {
		params.Archived = &req.Archived.Value
	}
	if req.FeatureVersion != nil {
		params.FeatureVersion = &req.FeatureVersion.Value
	}
	experiments, nextCursor, totalCount, err := s.experimentStorage.ListExperiments(
		ctx,
		params,
	)
	if err != nil {
		if errors.Is(err, v2es.ErrInvalidCursor) {
			return nil, statusInvalidCursor.Err()
		}
		if errors.Is(err, v2es.ErrInvalidOrderBy) {
			return nil, statusInvalidOrderBy.Err()
		}
		s.logger.Error(
			"Failed to list experiments",
			log.FieldsFromIncomingContext(ctx).AddFields(
				zap.Error(err),
				zap.String("environmentId", req.EnvironmentId),
			)...,
		)
		return nil, api.NewGRPCStatus(err).Err()
	}

	summary, err := s.experimentStorage.GetExperimentSummary(ctx, req.EnvironmentId)
	if err != nil {
		s.logger.Error(
			"Failed to get experiment summary",
			log.FieldsFromIncomingContext(ctx).AddFields(
				zap.Error(err),
				zap.String("environmentId",
					req.EnvironmentId),
			)...,
		)
		return nil, api.NewGRPCStatus(err).Err()
	}
	return &proto.ListExperimentsResponse{
		Experiments: experiments,
		Cursor:      strconv.Itoa(nextCursor),
		TotalCount:  totalCount,
		Summary: &proto.ListExperimentsResponse_Summary{
			TotalWaitingCount: summary.TotalWaitingCount,
			TotalRunningCount: summary.TotalRunningCount,
			TotalStoppedCount: summary.TotalStoppedCount,
		},
	}, nil
}

func (s *experimentService) CreateExperiment(
	ctx context.Context,
	req *proto.CreateExperimentRequest,
) (*proto.CreateExperimentResponse, error) {
	editor, err := s.checkEnvironmentRole(
		ctx, accountproto.AccountV2_Role_Environment_EDITOR,
		req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	err = validateCreateExperimentRequest(req)
	if err != nil {
		return nil, err
	}
	getFeatureResp, err := s.featureClient.GetFeature(ctx, &featureproto.GetFeatureRequest{
		Id:            req.FeatureId,
		EnvironmentId: req.EnvironmentId,
	})
	if err != nil {
		if code := status.Code(err); code == codes.NotFound {
			return nil, statusFeatureNotFound.Err()
		}
		s.logger.Error(
			"Failed to get feature",
			log.FieldsFromIncomingContext(ctx).AddFields(
				zap.Error(err),
				zap.String("environmentId", req.EnvironmentId),
			)...,
		)
		return nil, api.NewGRPCStatus(err).Err()
	}
	experiment, err := domain.NewExperiment(
		req.FeatureId,
		getFeatureResp.Feature.Version,
		getFeatureResp.Feature.Variations,
		req.GoalIds,
		req.StartAt,
		req.StopAt,
		req.Name,
		req.Description,
		req.BaseVariationId,
		editor.Email,
	)
	if err != nil {
		s.logger.Error(
			"Failed to create experiment",
			log.FieldsFromIncomingContext(ctx).AddFields(
				zap.Error(err),
				zap.String("environmentId", req.EnvironmentId),
				zap.String("featureId", getFeatureResp.Feature.Id),
				zap.String("baseVariationId", req.BaseVariationId),
				zap.Any("featureVariations", getFeatureResp.Feature.Variations),
			)...,
		)
		return nil, api.NewGRPCStatus(err).Err()
	}
	err = s.dbClient.RunInTransactionV2(ctx, func(ctxWithTx context.Context) error {
		for _, gid := range req.GoalIds {
			goal, err := s.getGoalMySQL(ctxWithTx, gid, req.EnvironmentId)
			if err != nil {
				return err
			}
			if goal.ConnectionType != proto.Goal_EXPERIMENT {
				return statusGoalTypeMismatch.Err()
			}
		}
		e, err := domainevent.NewEvent(
			editor,
			eventproto.Event_EXPERIMENT,
			experiment.Id,
			eventproto.Event_EXPERIMENT_CREATED,
			&eventproto.ExperimentCreatedEvent{
				Id:              experiment.Id,
				FeatureId:       experiment.FeatureId,
				FeatureVersion:  experiment.FeatureVersion,
				Variations:      experiment.Variations,
				GoalIds:         experiment.GoalIds,
				StartAt:         experiment.StartAt,
				StopAt:          experiment.StopAt,
				StoppedAt:       experiment.StoppedAt,
				CreatedAt:       experiment.CreatedAt,
				UpdatedAt:       experiment.UpdatedAt,
				Name:            experiment.Name,
				Description:     experiment.Description,
				BaseVariationId: experiment.BaseVariationId,
			},
			req.EnvironmentId,
			experiment.Experiment,
			nil,
		)
		if err != nil {
			return err
		}
		err = s.publisher.Publish(ctx, e)
		if err != nil {
			return err
		}
		return s.experimentStorage.CreateExperiment(ctxWithTx, experiment, req.EnvironmentId)
	})
	if err != nil {
		if errors.Is(err, v2es.ErrGoalNotFound) {
			return nil, statusInvalidGoalID.Err()
		}
		if errors.Is(err, statusGoalTypeMismatch.Err()) {
			return nil, statusGoalTypeMismatch.Err()
		}
		if errors.Is(err, v2es.ErrExperimentAlreadyExists) {
			return nil, statusAlreadyExists.Err()
		}
		s.logger.Error(
			"Failed to create experiment",
			log.FieldsFromIncomingContext(ctx).AddFields(
				zap.Error(err),
				zap.String("environmentId", req.EnvironmentId),
			)...,
		)
		return nil, api.NewGRPCStatus(err).Err()
	}
	return &proto.CreateExperimentResponse{
		Experiment: experiment.Experiment,
	}, nil
}

func validateCreateExperimentRequest(
	req *proto.CreateExperimentRequest,
) error {
	if req.FeatureId == "" {
		return statusFeatureIDRequired.Err()
	}
	if len(req.GoalIds) == 0 {
		return statusGoalIDRequired.Err()
	}
	for _, gid := range req.GoalIds {
		if gid == "" {
			return statusGoalIDRequired.Err()
		}
	}
	if err := validateExperimentPeriod(req.StartAt, req.StopAt); err != nil {
		return err
	}
	if req.Name == "" {
		return statusExperimentNameRequired.Err()
	}
	return nil
}

func validateExperimentPeriod(startAt, stopAt int64) error {
	period := stopAt - startAt
	if period <= 0 || period > int64(maxExperimentPeriod) {
		return statusExperimentPeriodOutOfRange.Err()
	}
	return nil
}

func (s *experimentService) UpdateExperiment(
	ctx context.Context,
	req *proto.UpdateExperimentRequest,
) (*proto.UpdateExperimentResponse, error) {
	editor, err := s.checkEnvironmentRole(
		ctx, accountproto.AccountV2_Role_Environment_EDITOR,
		req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	err = validateUpdateExperimentRequest(req)
	if err != nil {
		s.logger.Error(
			"Failed validate update experiment request",
			log.FieldsFromIncomingContext(ctx).AddFields(
				zap.Error(err),
				zap.String("environmentId", req.EnvironmentId),
			)...,
		)
		return nil, err
	}

	// The internal finalization confirmation is idempotent and handled on a
	// dedicated path.
	if req.ConfirmFinalization != nil {
		return s.confirmFinalization(ctx, req, editor)
	}

	var experimentPb *proto.Experiment
	err = s.dbClient.RunInTransactionV2(ctx, func(ctxWithTx context.Context) error {
		experiment, err := s.experimentStorage.GetExperiment(ctxWithTx, req.Id, req.EnvironmentId)
		if err != nil {
			return err
		}

		// A stop request never moves the experiment to the terminal status
		// directly: it initiates the finalization (scheduled or early stop
		// share the same stable termination identity). While FINALIZING, a
		// repeated stop trigger is an idempotent no-op that returns the
		// current experiment.
		if isStopStatusRequest(req.Status) {
			switch experiment.Status {
			case proto.Experiment_WAITING, proto.Experiment_RUNNING:
				experimentPb, err = s.initiateTermination(
					ctxWithTx, req.EnvironmentId, editor, experiment, req.Status.Status)
				return err
			case proto.Experiment_FINALIZING:
				experimentPb = experiment.Experiment
				return nil
			}
		}

		updated, err := experiment.Update(
			req.Name,
			req.Description,
			req.StartAt,
			req.StopAt,
			req.Status,
			req.Archived,
		)
		if err != nil {
			return err
		}

		var eventMsg pb.Message
		if req.Archived != nil {
			if experiment.Status == proto.Experiment_RUNNING ||
				experiment.Status == proto.Experiment_FINALIZING {
				return v2es.ErrExperimentCannotBeArchived
			}
			eventMsg = &eventproto.ExperimentArchivedEvent{
				Id: req.Id,
			}
		} else {
			eventMsg = &eventproto.ExperimentUpdatedEvent{
				Id:          experiment.Id,
				Name:        updated.Name,
				Description: updated.Description,
				StartAt:     updated.StartAt,
				StopAt:      updated.StopAt,
				Status:      updated.Status,
			}
		}
		event, err := domainevent.NewEvent(
			editor,
			eventproto.Event_EXPERIMENT,
			experiment.Id,
			eventproto.Event_EXPERIMENT_UPDATED,
			eventMsg,
			req.EnvironmentId,
			updated,
			experiment,
		)
		if err != nil {
			return err
		}
		if err := s.publisher.Publish(ctxWithTx, event); err != nil {
			return err
		}
		experimentPb = updated.Experiment
		return s.experimentStorage.UpdateExperiment(ctxWithTx, updated, req.EnvironmentId)
	})
	if err != nil {
		if errors.Is(err, v2es.ErrExperimentNotFound) || errors.Is(err, v2es.ErrExperimentUnexpectedAffectedRows) {
			return nil, statusExperimentNotFound.Err()
		}
		s.logger.Error(
			"Failed to update experiment",
			log.FieldsFromIncomingContext(ctx).AddFields(
				zap.Error(err),
				zap.String("environmentId", req.EnvironmentId),
			)...,
		)
		return nil, api.NewGRPCStatus(err).Err()
	}
	return &proto.UpdateExperimentResponse{
		Experiment: experimentPb,
	}, nil
}

func isStopStatusRequest(status *proto.UpdateExperimentRequest_UpdatedStatus) bool {
	return status != nil &&
		(status.Status == proto.Experiment_STOPPED ||
			status.Status == proto.Experiment_FORCE_STOPPED)
}

// initiateTermination fixes the configuration snapshot and the two event
// watermarks on a termination record and moves the experiment into FINALIZING,
// all in the caller's transaction. Scheduled and early (manual) stops share
// this single path, so a repeated trigger resumes the existing termination
// instead of producing a second final result.
func (s *experimentService) initiateTermination(
	ctx context.Context,
	environmentID string,
	editor *eventproto.Editor,
	current *domain.Experiment,
	requestedStatus proto.Experiment_Status,
) (*proto.Experiment, error) {
	forced := requestedStatus == proto.Experiment_FORCE_STOPPED
	now := time.Now().Unix()
	if err := current.CanInitiateFinalization(!forced, now); err != nil {
		return nil, err
	}
	reason := domain.TerminationReasonScheduled
	if forced {
		reason = domain.TerminationReasonForced
	}
	termination, err := domain.NewTermination(environmentID, current.Experiment, reason, now)
	if err != nil {
		return nil, err
	}
	if err := s.terminationStorage.CreateTermination(ctx, termination); err != nil {
		if errors.Is(err, v2es.ErrTerminationAlreadyExists) {
			// A concurrent stop trigger won the race: the finalization is
			// already in progress. Reload and return it, never creating a
			// second termination.
			fresh, gerr := s.experimentStorage.GetExperiment(ctx, current.Id, environmentID)
			if gerr != nil {
				return nil, gerr
			}
			return fresh.Experiment, nil
		}
		return nil, err
	}
	updated := &domain.Experiment{
		Experiment: pb.Clone(current.Experiment).(*proto.Experiment),
	}
	if err := updated.EnterFinalizing(termination.ID); err != nil {
		return nil, err
	}
	event, err := domainevent.NewEvent(
		editor,
		eventproto.Event_EXPERIMENT,
		current.Id,
		eventproto.Event_EXPERIMENT_UPDATED,
		&eventproto.ExperimentUpdatedEvent{
			Id:          updated.Id,
			Name:        updated.Name,
			Description: updated.Description,
			StartAt:     updated.StartAt,
			StopAt:      updated.StopAt,
			Status:      proto.Experiment_FINALIZING,
		},
		environmentID,
		updated.Experiment,
		current.Experiment,
	)
	if err != nil {
		return nil, err
	}
	if err := s.publisher.Publish(ctx, event); err != nil {
		return nil, err
	}
	if err := s.experimentStorage.UpdateExperiment(ctx, updated, environmentID); err != nil {
		return nil, err
	}
	return updated.Experiment, nil
}

// confirmFinalization moves a FINALIZING experiment to its terminal status and
// binds it to the confirmed final result, in the same transaction in which the
// termination advances to CONFIRMED and the EXPERIMENT_FINISHED domain event is
// published. Repeating the confirmation with the same identity is a
// successful no-op so a crashed publisher/cache phase can be retried safely.
func (s *experimentService) confirmFinalization(
	ctx context.Context,
	req *proto.UpdateExperimentRequest,
	editor *eventproto.Editor,
) (*proto.UpdateExperimentResponse, error) {
	cf := req.ConfirmFinalization
	if cf.TerminationId == "" || cf.FinalResultId == "" {
		return nil, statusTerminationMismatch.Err()
	}
	var experimentPb *proto.Experiment
	err := s.dbClient.RunInTransactionV2(ctx, func(ctxWithTx context.Context) error {
		experiment, err := s.experimentStorage.GetExperiment(ctxWithTx, req.Id, req.EnvironmentId)
		if err != nil {
			return err
		}
		// Idempotent confirmation: already terminal and bound to exactly the
		// same confirmed result.
		if (experiment.Status == proto.Experiment_STOPPED ||
			experiment.Status == proto.Experiment_FORCE_STOPPED) &&
			experiment.TerminationId == cf.TerminationId &&
			experiment.FinalResultId == cf.FinalResultId {
			experimentPb = experiment.Experiment
			return nil
		}
		termination, err := s.terminationStorage.GetTerminationByExperimentID(
			ctxWithTx, req.Id, req.EnvironmentId)
		if err != nil {
			if errors.Is(err, v2es.ErrTerminationNotFound) {
				return statusTerminationMismatch.Err()
			}
			return err
		}
		if termination.ID != cf.TerminationId ||
			termination.FinalResultID != cf.FinalResultId {
			return statusTerminationMismatch.Err()
		}
		if termination.Phase != domain.TerminationPhaseResultSaved {
			// The final result has not been persisted yet; the terminal
			// status must not reference an unconfirmed result.
			return statusTerminationNotFound.Err()
		}
		previous := pb.Clone(experiment.Experiment).(*proto.Experiment)
		if err := experiment.ConfirmFinalized(
			cf.TerminationId,
			cf.FinalResultId,
			termination.Reason == domain.TerminationReasonForced,
		); err != nil {
			return api.NewGRPCStatus(err).Err()
		}
		if err := termination.MarkConfirmed(time.Now().Unix()); err != nil {
			return err
		}
		if err := s.terminationStorage.UpdateTerminationCAS(
			ctxWithTx, termination, domain.TerminationPhaseResultSaved, ""); err != nil {
			return err
		}
		event, err := domainevent.NewEvent(
			editor,
			eventproto.Event_EXPERIMENT,
			experiment.Id,
			eventproto.Event_EXPERIMENT_FINISHED,
			&eventproto.ExperimentFinishedEvent{},
			req.EnvironmentId,
			experiment.Experiment,
			previous,
		)
		if err != nil {
			return err
		}
		if err := s.publisher.Publish(ctxWithTx, event); err != nil {
			return err
		}
		if err := s.experimentStorage.UpdateExperiment(
			ctxWithTx, experiment, req.EnvironmentId); err != nil {
			return err
		}
		experimentPb = experiment.Experiment
		return nil
	})
	if err != nil {
		if errors.Is(err, v2es.ErrExperimentNotFound) ||
			errors.Is(err, v2es.ErrExperimentUnexpectedAffectedRows) {
			return nil, statusExperimentNotFound.Err()
		}
		if errors.Is(err, v2es.ErrTerminationPhaseConflict) {
			// Another concurrent confirmation already advanced the phase.
			return nil, statusTerminationMismatch.Err()
		}
		s.logger.Error(
			"Failed to confirm experiment finalization",
			log.FieldsFromIncomingContext(ctx).AddFields(
				zap.Error(err),
				zap.String("environmentId", req.EnvironmentId),
				zap.String("experimentId", req.Id),
			)...,
		)
		return nil, api.NewGRPCStatus(err).Err()
	}
	return &proto.UpdateExperimentResponse{
		Experiment: experimentPb,
	}, nil
}

func validateUpdateExperimentRequest(
	req *proto.UpdateExperimentRequest,
) error {
	if req.Id == "" {
		return statusExperimentIDRequired.Err()
	}
	if req.Name != nil && req.Name.Value == "" {
		return statusExperimentNameRequired.Err()
	}
	if (req.StartAt != nil && req.StopAt == nil) ||
		(req.StartAt == nil && req.StopAt != nil) {
		return statusExperimentPeriodInvalid.Err()
	}
	if req.StartAt != nil && req.StopAt != nil {
		if err := validateExperimentPeriod(
			req.StartAt.Value,
			req.StopAt.Value,
		); err != nil {
			return err
		}
	}
	return nil
}

func (s *experimentService) DeleteExperiment(
	ctx context.Context,
	req *proto.DeleteExperimentRequest,
) (*proto.DeleteExperimentResponse, error) {
	editor, err := s.checkEnvironmentRole(
		ctx, accountproto.AccountV2_Role_Environment_EDITOR,
		req.EnvironmentId)
	if err != nil {
		return nil, err
	}
	if err = validateDeleteExperimentRequest(req); err != nil {
		return nil, err
	}

	var experimentPb *domain.Experiment
	err = s.dbClient.RunInTransactionV2(ctx, func(ctxWithTx context.Context) error {
		experiment, err := s.experimentStorage.GetExperiment(ctxWithTx, req.Id, req.EnvironmentId)
		if err != nil {
			s.logger.Error(
				"Failed to get experiment",
				log.FieldsFromIncomingContext(ctx).AddFields(
					zap.Error(err),
					zap.String("environmentId", req.EnvironmentId),
					zap.String("experimentId", req.Id),
				)...,
			)
			return err
		}
		experimentPb = experiment

		err = experiment.SetDeleted()
		if err != nil {
			s.logger.Error(
				"Failed to set deleted",
				log.FieldsFromIncomingContext(ctx).AddFields(
					zap.Error(err),
					zap.String("environmentId", req.EnvironmentId),
					zap.String("experimentId", req.Id),
				)...,
			)
			return err
		}

		return s.experimentStorage.UpdateExperiment(ctxWithTx, experiment, req.EnvironmentId)
	})
	if err != nil {
		s.logger.Error(
			"Failed to delete experiment",
			log.FieldsFromIncomingContext(ctx).AddFields(
				zap.Error(err),
				zap.String("environmentId", req.EnvironmentId),
				zap.String("experimentId", req.Id),
			)...,
		)
		if errors.Is(err, v2es.ErrExperimentNotFound) ||
			errors.Is(err, v2es.ErrExperimentUnexpectedAffectedRows) {
			return nil, statusExperimentNotFound.Err()
		}
		return nil, api.NewGRPCStatus(err).Err()
	}

	event, err := domainevent.NewEvent(
		editor,
		eventproto.Event_EXPERIMENT,
		req.Id,
		eventproto.Event_EXPERIMENT_DELETED,
		&eventproto.ExperimentDeletedEvent{
			Id: req.Id,
		},
		req.EnvironmentId,
		nil,
		experimentPb,
	)
	if err != nil {
		s.logger.Error(
			"Failed to create event",
			log.FieldsFromIncomingContext(ctx).AddFields(
				zap.Error(err),
				zap.String("environmentId", req.EnvironmentId),
				zap.String("experimentId", req.Id),
			)...,
		)
		return nil, api.NewGRPCStatus(err).Err()
	}
	if err := s.publisher.Publish(ctx, event); err != nil {
		s.logger.Error(
			"Failed to publish event",
			log.FieldsFromIncomingContext(ctx).AddFields(
				zap.Error(err),
				zap.String("environmentId", req.EnvironmentId),
				zap.String("experimentId", req.Id),
			)...,
		)
		return nil, api.NewGRPCStatus(err).Err()
	}

	return &proto.DeleteExperimentResponse{}, nil
}

func validateDeleteExperimentRequest(req *proto.DeleteExperimentRequest) error {
	if req.Id == "" {
		return statusExperimentIDRequired.Err()
	}
	return nil
}
