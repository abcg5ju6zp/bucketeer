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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/bucketeer-io/bucketeer/v2/pkg/experiment/domain"
	v2es "github.com/bucketeer-io/bucketeer/v2/pkg/experiment/storage/v2"
	storagemock "github.com/bucketeer-io/bucketeer/v2/pkg/experiment/storage/v2/mock"
	dbmock "github.com/bucketeer-io/bucketeer/v2/pkg/storage/v2/database/mock"
	experimentproto "github.com/bucketeer-io/bucketeer/v2/proto/experiment"
)

func timeNowSec() int64 {
	return time.Now().Unix()
}

func wrapperspbString(v string) *wrapperspb.StringValue {
	return wrapperspb.String(v)
}

// runInTx mocks the transaction wrapper by executing the closure in-place and
// propagating its error exactly like the real transaction would.
func runInTx(s *experimentService) {
	s.dbClient.(*dbmock.MockClient).EXPECT().RunInTransactionV2(
		gomock.Any(), gomock.Any(),
	).DoAndReturn(func(ctx context.Context, fn func(ctx context.Context) error) error {
		return fn(ctx)
	})
}

func TestInitiateTerminationScheduled(t *testing.T) {
	t.Parallel()
	c := gomock.NewController(t)
	service := createExperimentService(c, nil, nil, nil)
	ctx := createContextWithToken()
	now := timeNowSec()
	exp := &domain.Experiment{Experiment: &experimentproto.Experiment{
		Id: "eid", Status: experimentproto.Experiment_RUNNING,
		StartAt: now - 10, StopAt: now - 1,
	}}
	service.experimentStorage.(*storagemock.MockExperimentStorage).EXPECT().
		GetExperiment(gomock.Any(), "eid", "ns0").Return(exp, nil)
	service.terminationStorage.(*storagemock.MockTerminationStorage).EXPECT().
		CreateTermination(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, tn *domain.Termination) error {
			assert.Equal(t, domain.TerminationReasonScheduled, tn.Reason)
			assert.Equal(t, domain.TerminationPhaseWaitingWatermark, tn.Phase)
			assert.Equal(t, "eid", tn.ExperimentID)
			return nil
		})
	service.experimentStorage.(*storagemock.MockExperimentStorage).EXPECT().
		UpdateExperiment(gomock.Any(), gomock.Any(), "ns0").
		DoAndReturn(func(_ context.Context, e *domain.Experiment, _ string) error {
			assert.Equal(t, experimentproto.Experiment_FINALIZING, e.Status)
			assert.NotEmpty(t, e.TerminationId)
			return nil
		})
	runInTx(service)
	resp, err := service.UpdateExperiment(ctx, &experimentproto.UpdateExperimentRequest{
		Id: "eid", EnvironmentId: "ns0",
		Status: &experimentproto.UpdateExperimentRequest_UpdatedStatus{
			Status: experimentproto.Experiment_STOPPED,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, experimentproto.Experiment_FINALIZING, resp.Experiment.Status)
	assert.NotEmpty(t, resp.Experiment.TerminationId)
}

func TestInitiateTerminationScheduledBeforeStopRejected(t *testing.T) {
	t.Parallel()
	c := gomock.NewController(t)
	service := createExperimentService(c, nil, nil, nil)
	ctx := createContextWithToken()
	now := timeNowSec()
	exp := &domain.Experiment{Experiment: &experimentproto.Experiment{
		Id: "eid", Status: experimentproto.Experiment_RUNNING,
		StartAt: now - 10, StopAt: now + 3600,
	}}
	service.experimentStorage.(*storagemock.MockExperimentStorage).EXPECT().
		GetExperiment(gomock.Any(), "eid", "ns0").Return(exp, nil)
	runInTx(service)
	_, err := service.UpdateExperiment(ctx, &experimentproto.UpdateExperimentRequest{
		Id: "eid", EnvironmentId: "ns0",
		Status: &experimentproto.UpdateExperimentRequest_UpdatedStatus{
			Status: experimentproto.Experiment_STOPPED,
		},
	})
	assert.Error(t, err)
}

func TestInitiateTerminationEarlyStop(t *testing.T) {
	t.Parallel()
	c := gomock.NewController(t)
	service := createExperimentService(c, nil, nil, nil)
	ctx := createContextWithToken()
	now := timeNowSec()
	exp := &domain.Experiment{Experiment: &experimentproto.Experiment{
		Id: "eid", Status: experimentproto.Experiment_RUNNING,
		StartAt: now - 10, StopAt: now + 3600,
	}}
	service.experimentStorage.(*storagemock.MockExperimentStorage).EXPECT().
		GetExperiment(gomock.Any(), "eid", "ns0").Return(exp, nil)
	service.terminationStorage.(*storagemock.MockTerminationStorage).EXPECT().
		CreateTermination(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, tn *domain.Termination) error {
			assert.Equal(t, domain.TerminationReasonForced, tn.Reason)
			return nil
		})
	service.experimentStorage.(*storagemock.MockExperimentStorage).EXPECT().
		UpdateExperiment(gomock.Any(), gomock.Any(), "ns0").Return(nil)
	runInTx(service)
	resp, err := service.UpdateExperiment(ctx, &experimentproto.UpdateExperimentRequest{
		Id: "eid", EnvironmentId: "ns0",
		Status: &experimentproto.UpdateExperimentRequest_UpdatedStatus{
			Status: experimentproto.Experiment_FORCE_STOPPED,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, experimentproto.Experiment_FINALIZING, resp.Experiment.Status)
}

func TestInitiateTerminationDuplicateIsIdempotent(t *testing.T) {
	t.Parallel()
	c := gomock.NewController(t)
	service := createExperimentService(c, nil, nil, nil)
	ctx := createContextWithToken()
	now := timeNowSec()
	exp := &domain.Experiment{Experiment: &experimentproto.Experiment{
		Id: "eid", Status: experimentproto.Experiment_RUNNING,
		StartAt: now - 10, StopAt: now - 1,
	}}
	inFlight := &domain.Experiment{Experiment: &experimentproto.Experiment{
		Id: "eid", Status: experimentproto.Experiment_FINALIZING,
		TerminationId: "tid",
	}}
	service.experimentStorage.(*storagemock.MockExperimentStorage).EXPECT().
		GetExperiment(gomock.Any(), "eid", "ns0").Return(exp, nil)
	service.terminationStorage.(*storagemock.MockTerminationStorage).EXPECT().
		CreateTermination(gomock.Any(), gomock.Any()).Return(v2es.ErrTerminationAlreadyExists)
	// Concurrent trigger lost: the existing finalization is reloaded.
	service.experimentStorage.(*storagemock.MockExperimentStorage).EXPECT().
		GetExperiment(gomock.Any(), "eid", "ns0").Return(inFlight, nil)
	runInTx(service)
	resp, err := service.UpdateExperiment(ctx, &experimentproto.UpdateExperimentRequest{
		Id: "eid", EnvironmentId: "ns0",
		Status: &experimentproto.UpdateExperimentRequest_UpdatedStatus{
			Status: experimentproto.Experiment_STOPPED,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, experimentproto.Experiment_FINALIZING, resp.Experiment.Status)
	assert.Equal(t, "tid", resp.Experiment.TerminationId)
}

func TestStopWhileFinalizingIsIdempotentNoOp(t *testing.T) {
	t.Parallel()
	c := gomock.NewController(t)
	service := createExperimentService(c, nil, nil, nil)
	ctx := createContextWithToken()
	inFlight := &domain.Experiment{Experiment: &experimentproto.Experiment{
		Id: "eid", Status: experimentproto.Experiment_FINALIZING, TerminationId: "tid",
	}}
	service.experimentStorage.(*storagemock.MockExperimentStorage).EXPECT().
		GetExperiment(gomock.Any(), "eid", "ns0").Return(inFlight, nil)
	runInTx(service)
	resp, err := service.UpdateExperiment(ctx, &experimentproto.UpdateExperimentRequest{
		Id: "eid", EnvironmentId: "ns0",
		Status: &experimentproto.UpdateExperimentRequest_UpdatedStatus{
			Status: experimentproto.Experiment_FORCE_STOPPED,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, experimentproto.Experiment_FINALIZING, resp.Experiment.Status)
	assert.Equal(t, "tid", resp.Experiment.TerminationId)
}

func TestDraftUpdateWhileFinalizingRejected(t *testing.T) {
	t.Parallel()
	c := gomock.NewController(t)
	service := createExperimentService(c, nil, nil, nil)
	ctx := createContextWithToken()
	inFlight := &domain.Experiment{Experiment: &experimentproto.Experiment{
		Id: "eid", Status: experimentproto.Experiment_FINALIZING, TerminationId: "tid",
	}}
	service.experimentStorage.(*storagemock.MockExperimentStorage).EXPECT().
		GetExperiment(gomock.Any(), "eid", "ns0").Return(inFlight, nil)
	runInTx(service)
	_, err := service.UpdateExperiment(ctx, &experimentproto.UpdateExperimentRequest{
		Id: "eid", EnvironmentId: "ns0",
		Name: wrapperspbString("new-name"),
	})
	assert.Error(t, err)
}

func confirmRequest() *experimentproto.UpdateExperimentRequest {
	return &experimentproto.UpdateExperimentRequest{
		Id: "eid", EnvironmentId: "ns0",
		ConfirmFinalization: &experimentproto.UpdateExperimentRequest_ConfirmFinalization{
			TerminationId: "tid",
			FinalResultId: "rid",
		},
	}
}

func TestConfirmFinalizationSuccess(t *testing.T) {
	t.Parallel()
	for _, forced := range []bool{false, true} {
		c := gomock.NewController(t)
		service := createExperimentService(c, nil, nil, nil)
		ctx := createContextWithToken()
		reason := domain.TerminationReasonScheduled
		expectedStatus := experimentproto.Experiment_STOPPED
		if forced {
			reason = domain.TerminationReasonForced
			expectedStatus = experimentproto.Experiment_FORCE_STOPPED
		}
		exp := &domain.Experiment{Experiment: &experimentproto.Experiment{
			Id: "eid", Status: experimentproto.Experiment_FINALIZING, TerminationId: "tid",
		}}
		tn := &domain.Termination{
			ID: "tid", EnvironmentID: "ns0", ExperimentID: "eid",
			Reason: reason, Phase: domain.TerminationPhaseResultSaved,
			FinalResultID: "rid",
		}
		service.experimentStorage.(*storagemock.MockExperimentStorage).EXPECT().
			GetExperiment(gomock.Any(), "eid", "ns0").Return(exp, nil)
		service.terminationStorage.(*storagemock.MockTerminationStorage).EXPECT().
			GetTerminationByExperimentID(gomock.Any(), "eid", "ns0").Return(tn, nil)
		service.terminationStorage.(*storagemock.MockTerminationStorage).EXPECT().
			UpdateTerminationCAS(gomock.Any(), gomock.Any(),
				domain.TerminationPhaseResultSaved, "").
			DoAndReturn(func(_ context.Context, updated *domain.Termination, _ int32, _ string) error {
				assert.Equal(t, domain.TerminationPhaseConfirmed, updated.Phase)
				return nil
			})
		service.experimentStorage.(*storagemock.MockExperimentStorage).EXPECT().
			UpdateExperiment(gomock.Any(), gomock.Any(), "ns0").
			DoAndReturn(func(_ context.Context, e *domain.Experiment, _ string) error {
				assert.Equal(t, expectedStatus, e.Status)
				assert.Equal(t, "rid", e.FinalResultId)
				assert.InDelta(t, timeNowSec(), e.StoppedAt, 5)
				return nil
			})
		runInTx(service)
		resp, err := service.UpdateExperiment(ctx, confirmRequest())
		assert.NoError(t, err)
		assert.Equal(t, expectedStatus, resp.Experiment.Status)
		assert.Equal(t, "rid", resp.Experiment.FinalResultId)
	}
}

func TestConfirmFinalizationIdempotent(t *testing.T) {
	t.Parallel()
	c := gomock.NewController(t)
	service := createExperimentService(c, nil, nil, nil)
	ctx := createContextWithToken()
	already := &domain.Experiment{Experiment: &experimentproto.Experiment{
		Id: "eid", Status: experimentproto.Experiment_STOPPED,
		TerminationId: "tid", FinalResultId: "rid",
	}}
	service.experimentStorage.(*storagemock.MockExperimentStorage).EXPECT().
		GetExperiment(gomock.Any(), "eid", "ns0").Return(already, nil)
	runInTx(service)
	resp, err := service.UpdateExperiment(ctx, confirmRequest())
	assert.NoError(t, err)
	assert.Equal(t, experimentproto.Experiment_STOPPED, resp.Experiment.Status)
}

func TestDeleteGoalReferencedByFinalizingRejected(t *testing.T) {
	t.Parallel()
	c := gomock.NewController(t)
	service := createExperimentService(c, nil, nil, nil)
	ctx := createContextWithToken()
	finalizing := []*experimentproto.Experiment{
		{Id: "eid", Status: experimentproto.Experiment_FINALIZING, GoalIds: []string{"goal-id"}},
	}
	service.experimentStorage.(*storagemock.MockExperimentStorage).EXPECT().
		ListExperiments(gomock.Any(), gomock.Any()).
		Return(finalizing, 1, int64(1), nil)
	_, err := service.DeleteGoal(ctx, &experimentproto.DeleteGoalRequest{
		Id: "goal-id", EnvironmentId: "ns0",
	})
	assert.Equal(t, statusGoalReferencedByFinalizing.Err(), err)
}

func TestConfirmFinalizationRejections(t *testing.T) {
	t.Parallel()
	patterns := []struct {
		desc        string
		exp         *domain.Experiment
		termination *domain.Termination
		getTermErr  error
		expectedErr error
	}{
		{
			desc: "termination not found",
			exp: &domain.Experiment{Experiment: &experimentproto.Experiment{
				Id: "eid", Status: experimentproto.Experiment_FINALIZING, TerminationId: "tid",
			}},
			getTermErr:  v2es.ErrTerminationNotFound,
			expectedErr: statusTerminationMismatch.Err(),
		},
		{
			desc: "termination identity mismatch",
			exp: &domain.Experiment{Experiment: &experimentproto.Experiment{
				Id: "eid", Status: experimentproto.Experiment_FINALIZING, TerminationId: "other",
			}},
			termination: &domain.Termination{
				ID: "other", Phase: domain.TerminationPhaseResultSaved, FinalResultID: "rid",
			},
			expectedErr: statusTerminationMismatch.Err(),
		},
		{
			desc: "result not saved yet",
			exp: &domain.Experiment{Experiment: &experimentproto.Experiment{
				Id: "eid", Status: experimentproto.Experiment_FINALIZING, TerminationId: "tid",
			}},
			termination: &domain.Termination{
				ID: "tid", Phase: domain.TerminationPhaseWaitingWatermark, FinalResultID: "rid",
			},
			expectedErr: statusTerminationNotFound.Err(),
		},
	}
	for _, p := range patterns {
		t.Run(p.desc, func(t *testing.T) {
			c := gomock.NewController(t)
			service := createExperimentService(c, nil, nil, nil)
			ctx := createContextWithToken()
			service.experimentStorage.(*storagemock.MockExperimentStorage).EXPECT().
				GetExperiment(gomock.Any(), "eid", "ns0").Return(p.exp, nil)
			if p.termination != nil || p.getTermErr != nil {
				service.terminationStorage.(*storagemock.MockTerminationStorage).EXPECT().
					GetTerminationByExperimentID(gomock.Any(), "eid", "ns0").
					Return(p.termination, p.getTermErr)
			}
			runInTx(service)
			_, err := service.UpdateExperiment(ctx, confirmRequest())
			assert.Equal(t, p.expectedErr, err)
		})
	}
}
