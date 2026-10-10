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

package finalizer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"

	"github.com/bucketeer-io/bucketeer/v2/pkg/batch/jobs"
	cachev3 "github.com/bucketeer-io/bucketeer/v2/pkg/cache/v3"
	cachemock "github.com/bucketeer-io/bucketeer/v2/pkg/cache/v3/mock"
	environmentclientmock "github.com/bucketeer-io/bucketeer/v2/pkg/environment/client/mock"
	watermock "github.com/bucketeer-io/bucketeer/v2/pkg/eventcounter/storage/v2/operational_database/mock"
	experimentclientmock "github.com/bucketeer-io/bucketeer/v2/pkg/experiment/client/mock"
	experimentdomain "github.com/bucketeer-io/bucketeer/v2/pkg/experiment/domain"
	storagemock "github.com/bucketeer-io/bucketeer/v2/pkg/experiment/storage/v2/mock"
	calculatordomain "github.com/bucketeer-io/bucketeer/v2/pkg/experimentcalculator/domain"
	resultmock "github.com/bucketeer-io/bucketeer/v2/pkg/experimentcalculator/storage/v2/mock"
	environmentproto "github.com/bucketeer-io/bucketeer/v2/proto/environment"
	experimentproto "github.com/bucketeer-io/bucketeer/v2/proto/experiment"
)

const microPerSec = int64(1_000_000)

type fakeCalculator struct {
	result *calculatordomain.ExperimentResult
	err    error
	calls  int
}

func (f *fakeCalculator) RunFinal(
	_ context.Context, _ string, _ *experimentproto.Experiment,
) (*calculatordomain.ExperimentResult, *experimentdomain.InputSummary, error) {
	f.calls++
	if f.err != nil {
		return nil, nil, f.err
	}
	return f.result, &experimentdomain.InputSummary{EndAt: 200}, nil
}

func newFinalizer(
	term *storagemock.MockTerminationStorage,
	wm *watermock.MockEventWatermarkStorage,
	res *resultmock.MockExperimentResultStorage,
	caches []cachev3.ExperimentsCache,
	calc finalCalculator,
	exClient *experimentclientmock.MockClient,
) *experimentFinalizer {
	return &experimentFinalizer{
		experimentClient:   exClient,
		terminationStorage: term,
		watermarkStorage:   wm,
		calculator:         calc,
		resultStorage:      res,
		experimentCaches:   caches,
		settleWindow:       time.Minute,
		heartbeatMaxAge:    time.Minute,
		ownerID:            "owner-self",
		location:           time.UTC,
		opts:               &jobs.Options{Logger: zap.NewNop()},
		logger:             zap.NewNop(),
	}
}

func newTermination(phase int32) *experimentdomain.Termination {
	nowMicro := time.Now().UnixMicro()
	return &experimentdomain.Termination{
		ID:                  "tid",
		EnvironmentID:       "env",
		ExperimentID:        "eid",
		Reason:              experimentdomain.TerminationReasonScheduled,
		Phase:               phase,
		Snapshot:            &experimentdomain.ConfigSnapshot{ExperimentID: "eid", StopAt: 200},
		EvaluationWatermark: nowMicro,
		GoalWatermark:       nowMicro,
	}
}

func TestWaitingWatermarkNotReadyStaysFinalizing(t *testing.T) {
	t.Parallel()
	c := gomock.NewController(t)
	tn := newTermination(experimentdomain.TerminationPhaseWaitingWatermark)
	term := storagemock.NewMockTerminationStorage(c)
	wm := watermock.NewMockEventWatermarkStorage(c)
	res := resultmock.NewMockExperimentResultStorage(c)
	ex := experimentclientmock.NewMockClient(c)
	now := time.Now().UnixMicro()
	wm.EXPECT().GetEventWatermark(gomock.Any(), "env", int32(1)).
		Return(now-30*1000000, now, true, nil)
	wm.EXPECT().GetEventWatermark(gomock.Any(), "env", int32(2)).
		Return(now-20*1000000, now, true, nil)
	f := newFinalizer(term, wm, res, nil, &fakeCalculator{}, ex)
	assert.NoError(t, f.runTermination(context.Background(), "env", tn))
	assert.Equal(t, experimentdomain.TerminationPhaseWaitingWatermark, tn.Phase)
}

func TestWaitingWatermarkReadyAdvancesToComputing(t *testing.T) {
	t.Parallel()
	c := gomock.NewController(t)
	tn := newTermination(experimentdomain.TerminationPhaseWaitingWatermark)
	term := storagemock.NewMockTerminationStorage(c)
	wm := watermock.NewMockEventWatermarkStorage(c)
	res := resultmock.NewMockExperimentResultStorage(c)
	ex := experimentclientmock.NewMockClient(c)
	now := time.Now().UnixMicro()
	wm.EXPECT().GetEventWatermark(gomock.Any(), "env", int32(1)).
		Return(now+30*1000000, now, true, nil)
	wm.EXPECT().GetEventWatermark(gomock.Any(), "env", int32(2)).
		Return(now+30*1000000, now, true, nil)
	term.EXPECT().UpdateTerminationCAS(gomock.Any(), gomock.Any(),
		experimentdomain.TerminationPhaseWaitingWatermark, "").
		DoAndReturn(func(_ context.Context, updated *experimentdomain.Termination, _ int32, _ string) error {
			assert.Equal(t, experimentdomain.TerminationPhaseComputing, updated.Phase)
			assert.Equal(t, "owner-self", updated.OwnerID)
			return nil
		})
	f := newFinalizer(term, wm, res, nil, &fakeCalculator{}, ex)
	assert.NoError(t, f.runTermination(context.Background(), "env", tn))
}

func TestComputingSkippedWhileAnotherOwnerActive(t *testing.T) {
	t.Parallel()
	c := gomock.NewController(t)
	tn := newTermination(experimentdomain.TerminationPhaseComputing)
	tn.OwnerID = "owner-other"
	tn.OwnerExpiresAt = time.Now().Add(time.Hour).Unix()
	term := storagemock.NewMockTerminationStorage(c)
	wm := watermock.NewMockEventWatermarkStorage(c)
	res := resultmock.NewMockExperimentResultStorage(c)
	ex := experimentclientmock.NewMockClient(c)
	calc := &fakeCalculator{}
	f := newFinalizer(term, wm, res, nil, calc, ex)
	assert.NoError(t, f.runTermination(context.Background(), "env", tn))
	assert.Equal(t, 0, calc.calls)
}

func TestExpiredOwnerRecomputesAndSavesResult(t *testing.T) {
	t.Parallel()
	c := gomock.NewController(t)
	tn := newTermination(experimentdomain.TerminationPhaseComputing)
	tn.OwnerID = "owner-dead"
	tn.OwnerExpiresAt = time.Now().Add(-time.Minute).Unix()
	term := storagemock.NewMockTerminationStorage(c)
	wm := watermock.NewMockEventWatermarkStorage(c)
	res := resultmock.NewMockExperimentResultStorage(c)
	ex := experimentclientmock.NewMockClient(c)
	calc := &fakeCalculator{result: &calculatordomain.ExperimentResult{}}
	// lease renewal/takeover persist
	term.EXPECT().UpdateTerminationCAS(gomock.Any(), gomock.Any(),
		experimentdomain.TerminationPhaseComputing, "").Return(nil)
	res.EXPECT().UpdateExperimentResult(gomock.Any(), "env", gomock.Any()).Return(nil)
	term.EXPECT().UpdateTerminationCAS(gomock.Any(), gomock.Any(),
		experimentdomain.TerminationPhaseComputing, "owner-self").
		DoAndReturn(func(_ context.Context, updated *experimentdomain.Termination, _ int32, _ string) error {
			assert.Equal(t, experimentdomain.TerminationPhaseResultSaved, updated.Phase)
			if assert.NotNil(t, updated.InputSummary) {
				assert.Equal(t, tn.EvaluationWatermark, updated.InputSummary.EvaluationWatermark)
				assert.Equal(t, tn.GoalWatermark, updated.InputSummary.GoalWatermark)
				assert.Positive(t, updated.InputSummary.CalculatedAt)
			}
			return nil
		})
	f := newFinalizer(term, wm, res, nil, calc, ex)
	assert.NoError(t, f.runTermination(context.Background(), "env", tn))
	assert.Equal(t, 1, calc.calls)
}

func TestResultSavedConfirms(t *testing.T) {
	t.Parallel()
	c := gomock.NewController(t)
	tn := newTermination(experimentdomain.TerminationPhaseResultSaved)
	tn.FinalResultID = "eid"
	term := storagemock.NewMockTerminationStorage(c)
	wm := watermock.NewMockEventWatermarkStorage(c)
	res := resultmock.NewMockExperimentResultStorage(c)
	ex := experimentclientmock.NewMockClient(c)
	ex.EXPECT().UpdateExperiment(gomock.Any(), confirmReqMatcher{t: t}).
		Return(&experimentproto.UpdateExperimentResponse{Experiment: &experimentproto.Experiment{
			Id: "eid", Status: experimentproto.Experiment_STOPPED,
		}}, nil)
	term.EXPECT().UpdateTerminationCAS(gomock.Any(), gomock.Any(),
		experimentdomain.TerminationPhaseResultSaved, "").Return(nil)
	f := newFinalizer(term, wm, res, nil, &fakeCalculator{}, ex)
	assert.NoError(t, f.runTermination(context.Background(), "env", tn))
}

func TestResultSavedConfirmFailurePropagates(t *testing.T) {
	t.Parallel()
	c := gomock.NewController(t)
	tn := newTermination(experimentdomain.TerminationPhaseResultSaved)
	term := storagemock.NewMockTerminationStorage(c)
	wm := watermock.NewMockEventWatermarkStorage(c)
	res := resultmock.NewMockExperimentResultStorage(c)
	ex := experimentclientmock.NewMockClient(c)
	ex.EXPECT().UpdateExperiment(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("rpc unavailable"))
	f := newFinalizer(term, wm, res, nil, &fakeCalculator{}, ex)
	assert.Error(t, f.runTermination(context.Background(), "env", tn))
}

// confirmReqMatcher verifies the idempotent finalization confirmation request.
type confirmReqMatcher struct {
	t *testing.T
}

func (m confirmReqMatcher) Matches(x any) bool {
	req, ok := x.(*experimentproto.UpdateExperimentRequest)
	if !ok {
		return false
	}
	if req.GetId() != "eid" || req.GetEnvironmentId() != "env" {
		return false
	}
	cf := req.GetConfirmFinalization()
	return cf != nil && cf.TerminationId == "tid" && cf.FinalResultId == "eid"
}

func (m confirmReqMatcher) String() string {
	return "matches the finalization confirmation request tid/eid"
}

func TestConfirmedPublishesCache(t *testing.T) {
	t.Parallel()
	c := gomock.NewController(t)
	tn := newTermination(experimentdomain.TerminationPhaseConfirmed)
	term := storagemock.NewMockTerminationStorage(c)
	wm := watermock.NewMockEventWatermarkStorage(c)
	res := resultmock.NewMockExperimentResultStorage(c)
	ex := experimentclientmock.NewMockClient(c)
	cache1 := cachemock.NewMockExperimentsCache(c)
	cache2 := cachemock.NewMockExperimentsCache(c)
	ex.EXPECT().ListExperiments(gomock.Any(), gomock.Any()).
		Return(&experimentproto.ListExperimentsResponse{Experiments: []*experimentproto.Experiment{
			{Id: "other"},
		}}, nil)
	cache1.EXPECT().Put(gomock.Any(), "env").Return(nil)
	cache2.EXPECT().Put(gomock.Any(), "env").Return(nil)
	term.EXPECT().UpdateTerminationCAS(gomock.Any(), gomock.Any(),
		experimentdomain.TerminationPhaseConfirmed, "").Return(nil)
	f := newFinalizer(term, wm, res,
		[]cachev3.ExperimentsCache{cache1, cache2}, &fakeCalculator{}, ex)
	assert.NoError(t, f.runTermination(context.Background(), "env", tn))
	assert.True(t, tn.IsDone())
}

func TestConfirmedCachePutFailureKeepsPhase(t *testing.T) {
	t.Parallel()
	c := gomock.NewController(t)
	tn := newTermination(experimentdomain.TerminationPhaseConfirmed)
	term := storagemock.NewMockTerminationStorage(c)
	wm := watermock.NewMockEventWatermarkStorage(c)
	res := resultmock.NewMockExperimentResultStorage(c)
	ex := experimentclientmock.NewMockClient(c)
	cache1 := cachemock.NewMockExperimentsCache(c)
	ex.EXPECT().ListExperiments(gomock.Any(), gomock.Any()).
		Return(&experimentproto.ListExperimentsResponse{}, nil)
	cache1.EXPECT().Put(gomock.Any(), "env").Return(errors.New("redis down"))
	f := newFinalizer(term, wm, res,
		[]cachev3.ExperimentsCache{cache1}, &fakeCalculator{}, ex)
	assert.Error(t, f.runTermination(context.Background(), "env", tn))
	assert.Equal(t, experimentdomain.TerminationPhaseConfirmed, tn.Phase)
}

func TestRunListsEnvironmentsAndTerminations(t *testing.T) {
	t.Parallel()
	c := gomock.NewController(t)
	term := storagemock.NewMockTerminationStorage(c)
	wm := watermock.NewMockEventWatermarkStorage(c)
	res := resultmock.NewMockExperimentResultStorage(c)
	ex := experimentclientmock.NewMockClient(c)
	envClient := environmentclientmock.NewMockClient(c)
	envClient.EXPECT().ListEnvironmentsV2(gomock.Any(), gomock.Any()).
		Return(&environmentproto.ListEnvironmentsV2Response{Environments: []*environmentproto.EnvironmentV2{
			{Id: "env-a"},
		}}, nil)
	term.EXPECT().ListTerminationsByPhases(gomock.Any(), "env-a", gomock.Any()).
		Return([]*experimentdomain.Termination{}, nil)
	f := &experimentFinalizer{
		environmentClient:  envClient,
		terminationStorage: term,
		watermarkStorage:   wm,
		calculator:         &fakeCalculator{},
		resultStorage:      res,
		experimentClient:   ex,
		location:           time.UTC,
		opts:               &jobs.Options{Logger: zap.NewNop()},
		logger:             zap.NewNop(),
	}
	assert.NoError(t, f.Run(context.Background()))
}
