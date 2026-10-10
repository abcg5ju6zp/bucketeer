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
package storage

import (
	"context"
	"errors"

	"github.com/bucketeer-io/bucketeer/v2/pkg/coderef/domain"
	bkterr "github.com/bucketeer-io/bucketeer/v2/pkg/error"
	coderefproto "github.com/bucketeer-io/bucketeer/v2/proto/coderef"
	eventproto "github.com/bucketeer-io/bucketeer/v2/proto/event/domain"
)

// EventBuilder is invoked by the storage after the change sequence is
// allocated but before the change row commit, so the domain event can carry
// the sequence and version pair and is stored atomically with the reference
// mutation.
type EventBuilder func(sequence int64) (*eventproto.Event, error)

var (
	ErrCodeReferenceNotFound = bkterr.NewErrorNotFound(
		bkterr.CoderefPackageName,
		"code reference not found", "code_reference",
	)
	ErrCodeReferenceUnexpectedAffectedRows = bkterr.NewErrorUnexpectedAffectedRows(
		bkterr.CoderefPackageName,
		"code reference unexpected affected rows",
	)
	// ErrReferenceChangeNotFound is returned when no change exists for the
	// given id or request fingerprint.
	ErrReferenceChangeNotFound = bkterr.NewErrorNotFound(
		bkterr.CoderefPackageName,
		"reference change not found", "code_reference_change",
	)
)

// Shared list-query errors returned by CodeReferenceStorage implementations.
var (
	ErrInvalidOrderBy = errors.New("coderef/storage: invalid order by")
	ErrInvalidCursor  = errors.New("coderef/storage: invalid cursor")
)

// CodeReferenceStorage persists code references and their changes.
type CodeReferenceStorage interface {
	// GetCodeReference and ListCodeReferences are ordinary read queries;
	// their filtering semantics are unchanged.
	GetCodeReference(ctx context.Context, id string) (*domain.CodeReference, error)
	ListCodeReferences(
		ctx context.Context,
		params ListCodeReferencesParams,
	) ([]*domain.CodeReference, int, int64, error)

	// GetChangeByRequestHash returns the change recorded for a request
	// fingerprint. It is the idempotency lookup.
	GetChangeByRequestHash(
		ctx context.Context,
		environmentID, requestHash string,
	) (*domain.ReferenceChange, error)
	// GetChange returns a change by its stable id.
	GetChange(ctx context.Context, id string) (*domain.ReferenceChange, error)

	// CreateReferenceAndChange inserts the reference row (version 1) and its
	// CREATE change (status PENDING) atomically. The event built by
	// buildEvent is serialized into the change row before commit.
	CreateReferenceAndChange(
		ctx context.Context,
		codeRef *domain.CodeReference,
		change *domain.ReferenceChange,
		buildEvent EventBuilder,
	) error
	// UpdateReferenceAndChange updates the reference row (version bump) and
	// inserts its UPDATE change (status PENDING) atomically.
	UpdateReferenceAndChange(
		ctx context.Context,
		codeRef *domain.CodeReference,
		change *domain.ReferenceChange,
		buildEvent EventBuilder,
	) error
	// DeleteReferenceAndChange deletes the reference row and inserts its
	// DELETE change (status PENDING) atomically.
	DeleteReferenceAndChange(
		ctx context.Context,
		change *domain.ReferenceChange,
		buildEvent EventBuilder,
	) error

	// MarkChangePublished marks a PENDING change as PUBLISHED. Updating an
	// already published change is treated as success.
	MarkChangePublished(ctx context.Context, id string) error

	// ListPendingChanges returns PENDING changes ordered by sequence. It is
	// used by the recovery relay to republish events after failures.
	ListPendingChanges(ctx context.Context, limit int) ([]*domain.ReferenceChange, error)

	// GetConfirmedWatermark returns the reference version watermark for the
	// environment: the sequence of the last change whose event is published
	// with no earlier pending change. Changes after it are left for the next
	// archival round.
	GetConfirmedWatermark(ctx context.Context, environmentID string) (int64, error)

	// CountLiveReferencesAtWatermark returns a map of feature ID to the number
	// of live code references at the given watermark. A reference is live at
	// the watermark when its CREATE change is at or below the watermark and no
	// DELETE change at or below the watermark exists for it. Features with
	// zero references are absent from the map and are treated as zero.
	CountLiveReferencesAtWatermark(
		ctx context.Context,
		environmentID string,
		featureIDs []string,
		watermark int64,
	) (map[string]int64, error)

	// HasReferenceChangesAfter reports whether any change with a sequence
	// greater than the watermark exists for the feature. It first locks the
	// parent feature row (FOR UPDATE), so concurrent reference mutations
	// cannot commit while the archival transaction finishes.
	HasReferenceChangesAfter(
		ctx context.Context,
		environmentID, featureID string,
		watermark int64,
	) (bool, error)
}

type ListCodeReferencesParams struct {
	EnvironmentID    string
	FeatureID        string
	RepositoryName   string
	RepositoryOwner  string
	RepositoryType   coderefproto.CodeReference_RepositoryType
	RepositoryBranch string
	FileExtension    string
	OrderBy          coderefproto.ListCodeReferencesRequest_OrderBy
	OrderDirection   coderefproto.ListCodeReferencesRequest_OrderDirection
	PageSize         int
	Cursor           string
}
