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

//go:generate mockgen -source=$GOFILE -package=mock -destination=./mock/$GOFILE
package v2

import (
	"context"

	pkgErr "github.com/bucketeer-io/bucketeer/v2/pkg/error"
	"github.com/bucketeer-io/bucketeer/v2/pkg/experiment/domain"
)

var (
	ErrTerminationAlreadyExists = pkgErr.NewErrorAlreadyExists(
		pkgErr.ExperimentPackageName,
		"termination already exists",
	)
	ErrTerminationNotFound = pkgErr.NewErrorNotFound(
		pkgErr.ExperimentPackageName,
		"termination not found",
		"termination",
	)
	ErrTerminationUnexpectedAffectedRows = pkgErr.NewErrorUnexpectedAffectedRows(
		pkgErr.ExperimentPackageName,
		"unexpected affected rows",
	)
	ErrTerminationPhaseConflict = pkgErr.NewErrorFailedPrecondition(
		pkgErr.ExperimentPackageName,
		"termination phase conflict",
	)
)

// TerminationStorage persists the experiment finalization state machines.
// All state transitions are compare-and-swap on the current phase so that a
// crashed owner or a restarted batch job can only resume the stored phase and
// can never produce a second final result.
type TerminationStorage interface {
	// CreateTermination stores a new termination. The unique
	// (environment_id, experiment_id) key makes repeated stop triggers
	// idempotent: a duplicate returns ErrTerminationAlreadyExists and the
	// caller resumes the existing termination.
	CreateTermination(ctx context.Context, t *domain.Termination) error
	// GetTerminationByExperimentID returns the termination of an experiment.
	GetTerminationByExperimentID(
		ctx context.Context,
		experimentID string,
		environmentID string,
	) (*domain.Termination, error)
	// ListTerminationsByPhases returns the terminations currently in any of the
	// given phases in an environment.
	ListTerminationsByPhases(
		ctx context.Context,
		environmentID string,
		phases []int32,
	) ([]*domain.Termination, error)
	// UpdateTerminationCAS persists a phase transition. The UPDATE only
	// succeeds when the stored phase equals expectedPhase and, when
	// expectedOwner is not empty, when the stored owner matches as well. A
	// failed compare returns ErrTerminationPhaseConflict so the caller can
	// reload and retry from the current phase.
	UpdateTerminationCAS(
		ctx context.Context,
		t *domain.Termination,
		expectedPhase int32,
		expectedOwner string,
	) error
}
