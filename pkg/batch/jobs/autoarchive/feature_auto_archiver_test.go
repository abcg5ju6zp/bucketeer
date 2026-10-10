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

package autoarchive

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/bucketeer-io/bucketeer/v2/pkg/batch/jobs"
	coderefstoragemock "github.com/bucketeer-io/bucketeer/v2/pkg/coderef/storage/mock"
	environmentdomain "github.com/bucketeer-io/bucketeer/v2/pkg/environment/domain"
	environmentstoragemock "github.com/bucketeer-io/bucketeer/v2/pkg/environment/storage/v2/mock"
	featureclientmock "github.com/bucketeer-io/bucketeer/v2/pkg/feature/client/mock"
	"github.com/bucketeer-io/bucketeer/v2/pkg/feature/domain"
	featurestoragemock "github.com/bucketeer-io/bucketeer/v2/pkg/feature/storage/v2/mock"
	"github.com/bucketeer-io/bucketeer/v2/pkg/log"
	environmentproto "github.com/bucketeer-io/bucketeer/v2/proto/environment"
	featureproto "github.com/bucketeer-io/bucketeer/v2/proto/feature"
)

func TestFeatureAutoArchiver_Run(t *testing.T) {
	t.Parallel()

	patterns := []struct {
		desc          string
		setup         func(a *featureAutoArchiver)
		expectedError bool
	}{
		{
			desc: "success: no environments with auto-archive enabled",
			setup: func(a *featureAutoArchiver) {
				a.envStorage.(*environmentstoragemock.MockEnvironmentStorage).EXPECT().
					ListAutoArchiveEnabledEnvironments(gomock.Any()).
					Return([]*environmentdomain.EnvironmentV2{}, nil)
			},
			expectedError: false,
		},
		{
			desc: "error: failed to list auto-archive enabled environments",
			setup: func(a *featureAutoArchiver) {
				a.envStorage.(*environmentstoragemock.MockEnvironmentStorage).EXPECT().
					ListAutoArchiveEnabledEnvironments(gomock.Any()).
					Return(nil, errors.New("internal error"))
			},
			expectedError: true,
		},
		{
			desc: "success: no features in environment",
			setup: func(a *featureAutoArchiver) {
				a.envStorage.(*environmentstoragemock.MockEnvironmentStorage).EXPECT().
					ListAutoArchiveEnabledEnvironments(gomock.Any()).
					Return([]*environmentdomain.EnvironmentV2{
						createTestEnvironment("env-1", 60, true),
					}, nil)
				a.ftStorage.(*featurestoragemock.MockFeatureStorage).EXPECT().
					ListFeatures(gomock.Any(), gomock.Any()).
					Return([]*featureproto.Feature{}, 0, int64(0), nil)
			},
			expectedError: false,
		},
		{
			desc: "error: failed to list features",
			setup: func(a *featureAutoArchiver) {
				a.envStorage.(*environmentstoragemock.MockEnvironmentStorage).EXPECT().
					ListAutoArchiveEnabledEnvironments(gomock.Any()).
					Return([]*environmentdomain.EnvironmentV2{
						createTestEnvironment("env-1", 60, true),
					}, nil)
				a.ftStorage.(*featurestoragemock.MockFeatureStorage).EXPECT().
					ListFeatures(gomock.Any(), gomock.Any()).
					Return(nil, 0, int64(0), errors.New("internal error"))
			},
			expectedError: true,
		},
		{
			desc: "success: no archivable features - all recently used, no coderef calls",
			setup: func(a *featureAutoArchiver) {
				now := time.Now().Unix()
				a.envStorage.(*environmentstoragemock.MockEnvironmentStorage).EXPECT().
					ListAutoArchiveEnabledEnvironments(gomock.Any()).
					Return([]*environmentdomain.EnvironmentV2{
						createTestEnvironment("env-1", 60, false),
					}, nil)
				a.ftStorage.(*featurestoragemock.MockFeatureStorage).EXPECT().
					ListFeatures(gomock.Any(), gomock.Any()).
					Return([]*featureproto.Feature{
						createTestFeature("feature-1", now-86400), // used 1 day ago
					}, 1, int64(1), nil)
			},
			expectedError: false,
		},
		{
			desc: "success: archive multiple features without watermark",
			setup: func(a *featureAutoArchiver) {
				now := time.Now().Unix()
				oldTime := now - (100 * 24 * 60 * 60)
				a.envStorage.(*environmentstoragemock.MockEnvironmentStorage).EXPECT().
					ListAutoArchiveEnabledEnvironments(gomock.Any()).
					Return([]*environmentdomain.EnvironmentV2{
						createTestEnvironment("env-1", 60, false),
					}, nil)
				a.ftStorage.(*featurestoragemock.MockFeatureStorage).EXPECT().
					ListFeatures(gomock.Any(), gomock.Any()).
					Return([]*featureproto.Feature{
						createTestFeature("feature-1", oldTime),
						createTestFeature("feature-2", oldTime),
					}, 2, int64(2), nil)
				a.featureClient.(*featureclientmock.MockClient).EXPECT().
					UpdateFeature(gomock.Any(), gomock.Any()).
					DoAndReturn(func(
						ctx context.Context,
						req *featureproto.UpdateFeatureRequest,
						_ ...grpc.CallOption,
					) (*featureproto.UpdateFeatureResponse, error) {
						assert.Equal(t, int64(0), req.ReferenceWatermark)
						return &featureproto.UpdateFeatureResponse{}, nil
					}).
					Times(2)
			},
			expectedError: false,
		},
		{
			desc: "success: archive with fixed watermark and confirmed counts",
			setup: func(a *featureAutoArchiver) {
				now := time.Now().Unix()
				oldTime := now - (100 * 24 * 60 * 60)
				a.envStorage.(*environmentstoragemock.MockEnvironmentStorage).EXPECT().
					ListAutoArchiveEnabledEnvironments(gomock.Any()).
					Return([]*environmentdomain.EnvironmentV2{
						createTestEnvironment("env-1", 60, true),
					}, nil)
				a.ftStorage.(*featurestoragemock.MockFeatureStorage).EXPECT().
					ListFeatures(gomock.Any(), gomock.Any()).
					Return([]*featureproto.Feature{
						createTestFeature("feature-1", oldTime),
						createTestFeature("feature-2", oldTime),
					}, 2, int64(2), nil)
				codeRefMock := a.codeRefStorage.(*coderefstoragemock.MockCodeReferenceStorage)
				codeRefMock.EXPECT().
					GetConfirmedWatermark(gomock.Any(), "env-1").
					Return(int64(99), nil)
				codeRefMock.EXPECT().
					CountLiveReferencesAtWatermark(
						gomock.Any(), "env-1",
						[]string{"feature-1", "feature-2"}, int64(99),
					).
					Return(map[string]int64{
						"feature-1": 0,
						"feature-2": 1,
					}, nil)
				a.featureClient.(*featureclientmock.MockClient).EXPECT().
					UpdateFeature(gomock.Any(), gomock.Any()).
					DoAndReturn(func(
						ctx context.Context,
						req *featureproto.UpdateFeatureRequest,
						_ ...grpc.CallOption,
					) (*featureproto.UpdateFeatureResponse, error) {
						assert.Equal(t, int64(99), req.ReferenceWatermark)
						assert.Equal(t, "feature-1", req.Id)
						return &featureproto.UpdateFeatureResponse{}, nil
					})
			},
			expectedError: false,
		},
		{
			desc: "success: deferred archive (failed precondition) is not a job error",
			setup: func(a *featureAutoArchiver) {
				now := time.Now().Unix()
				oldTime := now - (100 * 24 * 60 * 60)
				a.envStorage.(*environmentstoragemock.MockEnvironmentStorage).EXPECT().
					ListAutoArchiveEnabledEnvironments(gomock.Any()).
					Return([]*environmentdomain.EnvironmentV2{
						createTestEnvironment("env-1", 60, true),
					}, nil)
				a.ftStorage.(*featurestoragemock.MockFeatureStorage).EXPECT().
					ListFeatures(gomock.Any(), gomock.Any()).
					Return([]*featureproto.Feature{
						createTestFeature("feature-1", oldTime),
					}, 1, int64(1), nil)
				codeRefMock := a.codeRefStorage.(*coderefstoragemock.MockCodeReferenceStorage)
				codeRefMock.EXPECT().
					GetConfirmedWatermark(gomock.Any(), "env-1").
					Return(int64(50), nil)
				codeRefMock.EXPECT().
					CountLiveReferencesAtWatermark(
						gomock.Any(), "env-1", []string{"feature-1"}, int64(50),
					).
					Return(map[string]int64{"feature-1": 0}, nil)
				a.featureClient.(*featureclientmock.MockClient).EXPECT().
					UpdateFeature(gomock.Any(), gomock.Any()).
					Return(nil, status.Error(codes.FailedPrecondition, "stale"))
			},
			expectedError: false,
		},
		{
			desc: "error: watermark lookup fails",
			setup: func(a *featureAutoArchiver) {
				now := time.Now().Unix()
				oldTime := now - (100 * 24 * 60 * 60)
				a.envStorage.(*environmentstoragemock.MockEnvironmentStorage).EXPECT().
					ListAutoArchiveEnabledEnvironments(gomock.Any()).
					Return([]*environmentdomain.EnvironmentV2{
						createTestEnvironment("env-1", 60, true),
					}, nil)
				a.ftStorage.(*featurestoragemock.MockFeatureStorage).EXPECT().
					ListFeatures(gomock.Any(), gomock.Any()).
					Return([]*featureproto.Feature{
						createTestFeature("feature-1", oldTime),
					}, 1, int64(1), nil)
				a.codeRefStorage.(*coderefstoragemock.MockCodeReferenceStorage).EXPECT().
					GetConfirmedWatermark(gomock.Any(), "env-1").
					Return(int64(0), errors.New("db error"))
			},
			expectedError: true,
		},
		{
			desc: "error: partial failure - one archive fails",
			setup: func(a *featureAutoArchiver) {
				now := time.Now().Unix()
				oldTime := now - (100 * 24 * 60 * 60)
				a.envStorage.(*environmentstoragemock.MockEnvironmentStorage).EXPECT().
					ListAutoArchiveEnabledEnvironments(gomock.Any()).
					Return([]*environmentdomain.EnvironmentV2{
						createTestEnvironment("env-1", 60, false),
					}, nil)
				a.ftStorage.(*featurestoragemock.MockFeatureStorage).EXPECT().
					ListFeatures(gomock.Any(), gomock.Any()).
					Return([]*featureproto.Feature{
						createTestFeature("feature-1", oldTime),
						createTestFeature("feature-2", oldTime),
					}, 2, int64(2), nil)
				gomock.InOrder(
					a.featureClient.(*featureclientmock.MockClient).EXPECT().
						UpdateFeature(gomock.Any(), gomock.Any()).
						Return(&featureproto.UpdateFeatureResponse{}, nil),
					a.featureClient.(*featureclientmock.MockClient).EXPECT().
						UpdateFeature(gomock.Any(), gomock.Any()).
						Return(nil, errors.New("archive failed")),
				)
			},
			expectedError: true,
		},
		{
			desc: "success: skip features with dependencies",
			setup: func(a *featureAutoArchiver) {
				now := time.Now().Unix()
				oldTime := now - (100 * 24 * 60 * 60)
				a.envStorage.(*environmentstoragemock.MockEnvironmentStorage).EXPECT().
					ListAutoArchiveEnabledEnvironments(gomock.Any()).
					Return([]*environmentdomain.EnvironmentV2{
						createTestEnvironment("env-1", 60, false),
					}, nil)
				a.ftStorage.(*featurestoragemock.MockFeatureStorage).EXPECT().
					ListFeatures(gomock.Any(), gomock.Any()).
					Return([]*featureproto.Feature{
						createTestFeature("feature-1", oldTime),
						createTestFeatureWithPrerequisite("feature-2", oldTime, "feature-1"),
					}, 2, int64(2), nil)
				a.featureClient.(*featureclientmock.MockClient).EXPECT().
					UpdateFeature(gomock.Any(), gomock.Any()).
					Return(&featureproto.UpdateFeatureResponse{}, nil).
					Times(1)
			},
			expectedError: false,
		},
	}

	for _, p := range patterns {
		t.Run(p.desc, func(t *testing.T) {
			controller := gomock.NewController(t)
			defer controller.Finish()

			archiver := newMockFeatureAutoArchiver(t, controller)
			p.setup(archiver)

			err := archiver.Run(context.Background())

			if p.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestFeatureAutoArchiver_ArchiveFeature(t *testing.T) {
	t.Parallel()

	patterns := []struct {
		desc           string
		setup          func(a *featureAutoArchiver)
		featureID      string
		environmentID  string
		watermark      int64
		expectedError  bool
		expectDeferred bool
	}{
		{
			desc: "success: archive feature with watermark forwarded",
			setup: func(a *featureAutoArchiver) {
				a.featureClient.(*featureclientmock.MockClient).EXPECT().
					UpdateFeature(gomock.Any(), &featureproto.UpdateFeatureRequest{
						Id:                 "feature-1",
						EnvironmentId:      "env-1",
						Archived:           wrapperspb.Bool(true),
						Comment:            "Archived automatically due to inactivity (environment setting)",
						ReferenceWatermark: int64(33),
					}).
					Return(&featureproto.UpdateFeatureResponse{}, nil)
			},
			featureID:     "feature-1",
			environmentID: "env-1",
			watermark:     33,
			expectedError: false,
		},
		{
			desc: "deferred: feature service rejects with failed precondition",
			setup: func(a *featureAutoArchiver) {
				a.featureClient.(*featureclientmock.MockClient).EXPECT().
					UpdateFeature(gomock.Any(), gomock.Any()).
					Return(nil, status.Error(codes.FailedPrecondition, "stale"))
			},
			featureID:      "feature-1",
			environmentID:  "env-1",
			watermark:      10,
			expectDeferred: true,
		},
		{
			desc: "error: archive feature fails with other code",
			setup: func(a *featureAutoArchiver) {
				a.featureClient.(*featureclientmock.MockClient).EXPECT().
					UpdateFeature(gomock.Any(), gomock.Any()).
					Return(nil, errors.New("internal error"))
			},
			featureID:     "feature-1",
			environmentID: "env-1",
			expectedError: true,
		},
	}

	for _, p := range patterns {
		t.Run(p.desc, func(t *testing.T) {
			controller := gomock.NewController(t)
			defer controller.Finish()

			archiver := newMockFeatureAutoArchiver(t, controller)
			p.setup(archiver)

			err := archiver.ArchiveFeature(
				context.Background(), p.featureID, p.environmentID, p.watermark,
			)

			switch {
			case p.expectDeferred:
				assert.ErrorIs(t, err, domain.ErrArchiveDeferred)
			case p.expectedError:
				assert.Error(t, err)
			default:
				assert.NoError(t, err)
			}
		})
	}
}

func newMockFeatureAutoArchiver(t *testing.T, c *gomock.Controller) *featureAutoArchiver {
	t.Helper()
	logger, err := log.NewLogger()
	require.NoError(t, err)
	codeRefStorage := coderefstoragemock.NewMockCodeReferenceStorage(c)
	return &featureAutoArchiver{
		envStorage:     environmentstoragemock.NewMockEnvironmentStorage(c),
		ftStorage:      featurestoragemock.NewMockFeatureStorage(c),
		codeRefStorage: codeRefStorage,
		evaluator:      domain.NewArchivabilityEvaluator(codeRefStorage),
		featureClient:  featureclientmock.NewMockClient(c),
		opts: &jobs.Options{
			Timeout: 5 * time.Second,
		},
		logger: logger,
	}
}

func createTestEnvironment(id string, unusedDays int32, checkCodeRefs bool) *environmentdomain.EnvironmentV2 {
	return &environmentdomain.EnvironmentV2{
		EnvironmentV2: &environmentproto.EnvironmentV2{
			Id:                       id,
			Name:                     id,
			AutoArchiveEnabled:       true,
			AutoArchiveUnusedDays:    unusedDays,
			AutoArchiveCheckCodeRefs: checkCodeRefs,
		},
	}
}

func createTestFeature(id string, lastUsedAt int64) *featureproto.Feature {
	return &featureproto.Feature{
		Id:       id,
		Name:     id,
		Archived: false,
		LastUsedInfo: &featureproto.FeatureLastUsedInfo{
			FeatureId:  id,
			LastUsedAt: lastUsedAt,
		},
	}
}

func createTestFeatureWithPrerequisite(id string, lastUsedAt int64, prerequisiteID string) *featureproto.Feature {
	return &featureproto.Feature{
		Id:       id,
		Name:     id,
		Archived: false,
		LastUsedInfo: &featureproto.FeatureLastUsedInfo{
			FeatureId:  id,
			LastUsedAt: lastUsedAt,
		},
		Prerequisites: []*featureproto.Prerequisite{
			{
				FeatureId: prerequisiteID,
			},
		},
	}
}
