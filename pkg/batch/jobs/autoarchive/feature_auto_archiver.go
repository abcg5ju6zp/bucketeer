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
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/bucketeer-io/bucketeer/v2/pkg/batch/jobs"
	coderefstorage "github.com/bucketeer-io/bucketeer/v2/pkg/coderef/storage"
	environmentdomain "github.com/bucketeer-io/bucketeer/v2/pkg/environment/domain"
	environmentstorage "github.com/bucketeer-io/bucketeer/v2/pkg/environment/storage/v2"
	featureclient "github.com/bucketeer-io/bucketeer/v2/pkg/feature/client"
	"github.com/bucketeer-io/bucketeer/v2/pkg/feature/domain"
	featurestorage "github.com/bucketeer-io/bucketeer/v2/pkg/feature/storage/v2"
	featureproto "github.com/bucketeer-io/bucketeer/v2/proto/feature"
)

// featureAutoArchiver is a batch job that automatically archives
// feature flags based on environment-level auto-archive settings.
type featureAutoArchiver struct {
	envStorage     environmentstorage.EnvironmentStorage
	ftStorage      featurestorage.FeatureStorage
	codeRefStorage coderefstorage.CodeReferenceStorage
	evaluator      *domain.ArchivabilityEvaluator
	featureClient  featureclient.Client
	opts           *jobs.Options
	logger         *zap.Logger
}

// NewFeatureAutoArchiver creates a new feature auto-archiver batch job.
func NewFeatureAutoArchiver(
	envStorage environmentstorage.EnvironmentStorage,
	ftStorage featurestorage.FeatureStorage,
	codeRefStorage coderefstorage.CodeReferenceStorage,
	featureClient featureclient.Client,
	opts ...jobs.Option,
) jobs.Job {
	dopts := &jobs.Options{
		Timeout: 10 * time.Minute,
		Logger:  zap.NewNop(),
	}
	for _, opt := range opts {
		opt(dopts)
	}
	return &featureAutoArchiver{
		envStorage:     envStorage,
		ftStorage:      ftStorage,
		codeRefStorage: codeRefStorage,
		evaluator:      domain.NewArchivabilityEvaluator(codeRefStorage),
		featureClient:  featureClient,
		opts:           dopts,
		logger:         dopts.Logger.Named("feature-auto-archiver"),
	}
}

// Run executes the feature auto-archive batch job.
// It iterates through all environments with auto-archive enabled,
// evaluates feature archivability, and archives eligible features.
func (a *featureAutoArchiver) Run(ctx context.Context) (lastErr error) {
	startTime := time.Now()
	defer func() {
		jobs.RecordJob(jobs.JobFeatureAutoArchiver, lastErr, time.Since(startTime))
	}()

	ctx, cancel := context.WithTimeout(ctx, a.opts.Timeout)
	defer cancel()

	a.logger.Info("FeatureAutoArchiver start running")

	// Get environments with auto-archive enabled
	environments, err := a.envStorage.ListAutoArchiveEnabledEnvironments(ctx)
	if err != nil {
		a.logger.Error("Failed to list auto-archive enabled environments", zap.Error(err))
		return err
	}

	if len(environments) == 0 {
		a.logger.Info("No environments with auto-archive enabled")
		return nil
	}

	a.logger.Info("Found auto-archive enabled environments", zap.Int("count", len(environments)))

	// Process each environment
	for _, env := range environments {
		if err := a.processEnvironment(ctx, env); err != nil {
			a.logger.Error("Failed to process environment",
				zap.String("environmentId", env.Id),
				zap.Error(err),
			)
			lastErr = err
			// Continue processing other environments even if one fails
			continue
		}
	}

	a.logger.Info("FeatureAutoArchiver finished",
		zap.Duration("elapsedTime", time.Since(startTime)),
	)
	return lastErr
}

// processEnvironment processes a single environment for auto-archiving.
func (a *featureAutoArchiver) processEnvironment(
	ctx context.Context,
	env *environmentdomain.EnvironmentV2,
) error {
	a.logger.Info("Processing environment",
		zap.String("environmentId", env.Id),
		zap.Int32("unusedDaysThreshold", env.AutoArchiveUnusedDays),
		zap.Bool("checkCodeRefs", env.AutoArchiveCheckCodeRefs),
	)

	// Get non-archived features in the environment
	features, err := a.listNonArchivedFeatures(ctx, env.Id)
	if err != nil {
		return err
	}

	if len(features) == 0 {
		a.logger.Info("No non-archived features found", zap.String("environmentId", env.Id))
		return nil
	}

	// Build archivability criteria from environment settings
	criteria := domain.ArchivabilityCriteria{
		UnusedDaysThreshold: env.AutoArchiveUnusedDays,
		CheckCodeReferences: env.AutoArchiveCheckCodeRefs,
	}

	// Evaluate archivability using pre-created evaluator. The feature list
	// (already loaded above) and the reference watermark are fixed for this
	// scan; changes after the watermark are handled in the next round.
	results, watermark, err := a.evaluator.EvaluateArchivability(ctx, features, criteria, env.Id)
	if err != nil {
		a.logger.Error("Failed to evaluate archivability",
			zap.String("environmentId", env.Id),
			zap.Error(err),
		)
		return err
	}

	// Get archivable feature IDs
	archivableIDs := domain.GetArchivableFeatureIDs(results)
	if len(archivableIDs) == 0 {
		a.logger.Info("No archivable features found",
			zap.String("environmentId", env.Id),
			zap.Int("evaluatedCount", len(results)),
		)
		return nil
	}

	a.logger.Info("Found archivable features",
		zap.String("environmentId", env.Id),
		zap.Int("archivableCount", len(archivableIDs)),
		zap.Int("evaluatedCount", len(results)),
		zap.Int64("referenceWatermark", watermark),
	)

	// Archive features in bulk, passing the fixed watermark so each archive
	// is rejected at commit time if references changed after it.
	bulkResult := domain.ArchiveFeaturesInBulk(ctx, archivableIDs, env.Id, a, watermark)

	a.logger.Info("Archive results",
		zap.String("environmentId", env.Id),
		zap.Int("successCount", bulkResult.SuccessCount),
		zap.Int("failureCount", bulkResult.FailureCount),
		zap.Int("deferredCount", bulkResult.DeferredCount),
	)

	// Deferred archives are expected under concurrency and are retried in
	// the next round, so only hard failures make the job return an error.
	if bulkResult.FailureCount > 0 {
		for _, result := range bulkResult.Results {
			if !result.Success && !result.Deferred {
				return result.Error
			}
		}
	}

	return nil
}

// listNonArchivedFeatures retrieves all non-archived features for the given environment.
func (a *featureAutoArchiver) listNonArchivedFeatures(
	ctx context.Context,
	environmentID string,
) ([]*featureproto.Feature, error) {
	archived := false
	features, _, _, err := a.ftStorage.ListFeatures(ctx, featurestorage.ListFeaturesParams{
		EnvironmentID: environmentID,
		Archived:      &archived,
	})
	if err != nil {
		a.logger.Error("Failed to list features",
			zap.String("environmentId", environmentID),
			zap.Error(err),
		)
		return nil, err
	}

	return features, nil
}

// ArchiveFeature implements domain.FeatureArchiver interface.
// It archives a single feature by calling the feature service API. The fixed
// scan watermark is forwarded as reference_watermark; if the feature service
// detects a reference change after it at commit time, it rejects the request
// with FailedPrecondition and this method returns domain.ErrArchiveDeferred,
// so the feature is retried in the next round instead of failing the job.
func (a *featureAutoArchiver) ArchiveFeature(
	ctx context.Context,
	featureID, environmentID string,
	referenceWatermark int64,
) error {
	_, err := a.featureClient.UpdateFeature(ctx, &featureproto.UpdateFeatureRequest{
		Id:                 featureID,
		EnvironmentId:      environmentID,
		Archived:           wrapperspb.Bool(true),
		Comment:            "Archived automatically due to inactivity (environment setting)",
		ReferenceWatermark: referenceWatermark,
	})
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			a.logger.Info("Archive deferred to the next round, references changed",
				zap.String("featureId", featureID),
				zap.String("environmentId", environmentID),
				zap.Int64("referenceWatermark", referenceWatermark),
			)
			return domain.ErrArchiveDeferred
		}
		a.logger.Error("Failed to archive feature",
			zap.String("featureId", featureID),
			zap.String("environmentId", environmentID),
			zap.Error(err),
		)
		return err
	}

	a.logger.Info("Successfully archived feature",
		zap.String("featureId", featureID),
		zap.String("environmentId", environmentID),
	)
	return nil
}
