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
	"encoding/json"
	"errors"
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/bucketeer-io/bucketeer/v2/pkg/api/api"
	"github.com/bucketeer-io/bucketeer/v2/pkg/coderef/domain"
	"github.com/bucketeer-io/bucketeer/v2/pkg/coderef/storage"
	domainevent "github.com/bucketeer-io/bucketeer/v2/pkg/domainevent/domain"
	"github.com/bucketeer-io/bucketeer/v2/pkg/log"
	"github.com/bucketeer-io/bucketeer/v2/pkg/uuid"
	accountproto "github.com/bucketeer-io/bucketeer/v2/proto/account"
	coderefproto "github.com/bucketeer-io/bucketeer/v2/proto/coderef"
	eventproto "github.com/bucketeer-io/bucketeer/v2/proto/event/domain"
)

// generateSourceURL generates a URL to view the code in the repository web interface
func generateSourceURL(codeRef *coderefproto.CodeReference) string {
	switch codeRef.RepositoryType {
	case coderefproto.CodeReference_GITHUB:
		return fmt.Sprintf("https://github.com/%s/%s/blob/%s/%s#L%d",
			codeRef.RepositoryOwner,
			codeRef.RepositoryName,
			codeRef.CommitHash,
			codeRef.FilePath,
			codeRef.LineNumber)
	case coderefproto.CodeReference_GITLAB:
		return fmt.Sprintf("https://gitlab.com/%s/%s/-/blob/%s/%s#L%d",
			codeRef.RepositoryOwner,
			codeRef.RepositoryName,
			codeRef.CommitHash,
			codeRef.FilePath,
			codeRef.LineNumber)
	case coderefproto.CodeReference_BITBUCKET:
		return fmt.Sprintf("https://bitbucket.org/%s/%s/src/%s/%s#lines-%d",
			codeRef.RepositoryOwner,
			codeRef.RepositoryName,
			codeRef.CommitHash,
			codeRef.FilePath,
			codeRef.LineNumber)
	default:
		return ""
	}
}

// generateBranchURL generates a URL to view the branch in the repository web interface
func generateBranchURL(codeRef *coderefproto.CodeReference) string {
	switch codeRef.RepositoryType {
	case coderefproto.CodeReference_GITHUB:
		return fmt.Sprintf("https://github.com/%s/%s/tree/%s",
			codeRef.RepositoryOwner,
			codeRef.RepositoryName,
			codeRef.RepositoryBranch)
	case coderefproto.CodeReference_GITLAB:
		return fmt.Sprintf("https://gitlab.com/%s/%s/-/tree/%s",
			codeRef.RepositoryOwner,
			codeRef.RepositoryName,
			codeRef.RepositoryBranch)
	case coderefproto.CodeReference_BITBUCKET:
		return fmt.Sprintf("https://bitbucket.org/%s/%s/src/%s",
			codeRef.RepositoryOwner,
			codeRef.RepositoryName,
			codeRef.RepositoryBranch)
	default:
		return ""
	}
}

func (s *CodeReferenceService) GetCodeReference(
	ctx context.Context,
	req *coderefproto.GetCodeReferenceRequest,
) (*coderefproto.GetCodeReferenceResponse, error) {
	_, err := s.checkEnvironmentRole(
		ctx,
		accountproto.AccountV2_Role_Environment_VIEWER,
		req.EnvironmentId,
	)
	if err != nil {
		return nil, err
	}
	if err := validateGetCodeReferenceRequest(req); err != nil {
		return nil, err
	}
	codeRef, err := s.codeRefStorage.GetCodeReference(ctx, req.Id)
	if err != nil {
		if errors.Is(err, storage.ErrCodeReferenceNotFound) {
			return nil, statusNotFound.Err()
		}
		s.logger.Error(
			"Failed to get code reference",
			log.FieldsFromIncomingContext(ctx).AddFields(
				zap.Error(err),
				zap.String("id", req.Id),
				zap.String("environmentId", req.EnvironmentId),
			)...,
		)
		return nil, api.NewGRPCStatus(err).Err()
	}
	codeRef.SourceUrl = generateSourceURL(&codeRef.CodeReference)
	codeRef.BranchUrl = generateBranchURL(&codeRef.CodeReference)
	return &coderefproto.GetCodeReferenceResponse{CodeReference: &codeRef.CodeReference}, nil
}

func (s *CodeReferenceService) ListCodeReferences(
	ctx context.Context,
	req *coderefproto.ListCodeReferencesRequest,
) (*coderefproto.ListCodeReferencesResponse, error) {
	_, err := s.checkEnvironmentRole(
		ctx,
		accountproto.AccountV2_Role_Environment_VIEWER,
		req.EnvironmentId,
	)
	if err != nil {
		return nil, err
	}
	if err := validateListCodeReferencesRequest(req); err != nil {
		return nil, err
	}
	codeRefs, nextCursor, totalCount, err := s.codeRefStorage.ListCodeReferences(
		ctx,
		storage.ListCodeReferencesParams{
			EnvironmentID:    req.EnvironmentId,
			FeatureID:        req.FeatureId,
			RepositoryName:   req.RepositoryName,
			RepositoryOwner:  req.RepositoryOwner,
			RepositoryType:   req.RepositoryType,
			RepositoryBranch: req.RepositoryBranch,
			FileExtension:    req.FileExtension,
			OrderBy:          req.OrderBy,
			OrderDirection:   req.OrderDirection,
			PageSize:         int(req.PageSize),
			Cursor:           req.Cursor,
		},
	)
	if err != nil {
		if errors.Is(err, storage.ErrInvalidCursor) {
			return nil, statusInvalidCursor.Err()
		}
		if errors.Is(err, storage.ErrInvalidOrderBy) {
			return nil, statusInvalidOrderBy.Err()
		}
		s.logger.Error(
			"Failed to list code references",
			log.FieldsFromIncomingContext(ctx).AddFields(
				zap.Error(err),
				zap.String("environmentId", req.EnvironmentId),
			)...,
		)
		return nil, api.NewGRPCStatus(err).Err()
	}
	protoRefs := make([]*coderefproto.CodeReference, 0, len(codeRefs))
	for _, ref := range codeRefs {
		ref.SourceUrl = generateSourceURL(&ref.CodeReference)
		ref.BranchUrl = generateBranchURL(&ref.CodeReference)
		protoRefs = append(protoRefs, &ref.CodeReference)
	}
	return &coderefproto.ListCodeReferencesResponse{
		CodeReferences: protoRefs,
		Cursor:         fmt.Sprintf("%d", nextCursor),
		TotalCount:     totalCount,
	}, nil
}

func (s *CodeReferenceService) CreateCodeReference(
	ctx context.Context,
	req *coderefproto.CreateCodeReferenceRequest,
) (*coderefproto.CreateCodeReferenceResponse, error) {
	editor, err := s.checkEnvironmentRole(
		ctx,
		accountproto.AccountV2_Role_Environment_EDITOR,
		req.EnvironmentId,
	)
	if err != nil {
		return nil, err
	}
	if err := validateCreateCodeReferenceRequest(req); err != nil {
		return nil, err
	}
	requestHash := createRequestFingerprint(req)

	// Idempotency: a repeated request or a retried request whose response was
	// lost resolves to the existing change and always returns that change's
	// version, never a second reference.
	existing, err := s.codeRefStorage.GetChangeByRequestHash(ctx, req.EnvironmentId, requestHash)
	if err == nil {
		ref, err := s.resolveExistingChange(ctx, existing)
		if err != nil {
			return nil, err
		}
		return &coderefproto.CreateCodeReferenceResponse{CodeReference: ref}, nil
	}
	if !errors.Is(err, storage.ErrReferenceChangeNotFound) {
		s.logger.Error("Failed to look up existing change",
			log.FieldsFromIncomingContext(ctx).AddFields(zap.Error(err))...)
		return nil, statusInternal.Err()
	}

	codeRef, err := domain.NewCodeReference(
		req.FeatureId,
		req.FilePath,
		req.FileExtension,
		req.LineNumber,
		req.CodeSnippet,
		req.ContentHash,
		req.Aliases,
		req.RepositoryName,
		req.RepositoryOwner,
		req.RepositoryType,
		req.RepositoryBranch,
		req.CommitHash,
		req.EnvironmentId,
	)
	if err != nil {
		s.logger.Error(
			"Failed to create code reference",
			log.FieldsFromIncomingContext(ctx).AddFields(zap.Error(err))...,
		)
		return nil, statusInternal.Err()
	}
	changeID, err := newChangeID()
	if err != nil {
		return nil, statusInternal.Err()
	}
	change := &domain.ReferenceChange{
		ID:              changeID,
		CodeReferenceID: codeRef.Id,
		EnvironmentID:   req.EnvironmentId,
		FeatureID:       req.FeatureId,
		ChangeType:      domain.ChangeTypeCreate,
		BeforeVersion:   0,
		AfterVersion:    codeRef.Version,
		RequestHash:     requestHash,
		Status:          domain.ChangeStatusPending,
		CreatedAt:       codeRef.CreatedAt,
		UpdatedAt:       codeRef.UpdatedAt,
	}
	var eventPb *eventproto.Event
	buildEvent := func(sequence int64) (*eventproto.Event, error) {
		createEvent, err := domainevent.NewEvent(
			editor,
			eventproto.Event_CODEREF,
			codeRef.Id,
			eventproto.Event_CODE_REFERENCE_CREATED,
			&eventproto.CodeReferenceCreatedEvent{
				Id:               codeRef.Id,
				FeatureId:        codeRef.FeatureId,
				FilePath:         codeRef.FilePath,
				LineNumber:       codeRef.LineNumber,
				CodeSnippet:      codeRef.CodeSnippet,
				ContentHash:      codeRef.ContentHash,
				Aliases:          codeRef.Aliases,
				RepositoryName:   codeRef.RepositoryName,
				RepositoryOwner:  codeRef.RepositoryOwner,
				RepositoryType:   codeRef.RepositoryType,
				RepositoryBranch: codeRef.RepositoryBranch,
				CommitHash:       codeRef.CommitHash,
				EnvironmentId:    codeRef.EnvironmentId,
				CreatedAt:        codeRef.CreatedAt,
				UpdatedAt:        codeRef.UpdatedAt,
			},
			req.EnvironmentId,
			codeRef,
			&coderefproto.CodeReference{},
			domainevent.WithEventID(changeID),
			domainevent.WithChangeIdentity(sequence, 0, codeRef.Version),
		)
		if err != nil {
			return nil, err
		}
		eventPb = createEvent
		return createEvent, nil
	}
	if err := s.dbClient.RunInTransactionV2(ctx, func(ctxWithTx context.Context) error {
		return s.codeRefStorage.CreateReferenceAndChange(
			ctxWithTx, codeRef, change, buildEvent,
		)
	}); err != nil {
		// A concurrent identical request may have committed the change first.
		if current, lerr := s.codeRefStorage.GetChangeByRequestHash(
			ctx, req.EnvironmentId, requestHash,
		); lerr == nil {
			ref, rerr := s.resolveExistingChange(ctx, current)
			if rerr != nil {
				return nil, rerr
			}
			return &coderefproto.CreateCodeReferenceResponse{CodeReference: ref}, nil
		}
		s.logger.Error(
			"Failed to create code reference",
			log.FieldsFromIncomingContext(ctx).AddFields(
				zap.Error(err),
				zap.String("id", codeRef.Id),
			)...,
		)
		return nil, api.NewGRPCStatus(err).Err()
	}

	// The reference row is committed and the event is stored and therefore
	// recoverable. Publish the exact original event.
	if err := s.publishChangeEvent(ctx, eventPb, changeID); err != nil {
		return nil, err
	}
	return &coderefproto.CreateCodeReferenceResponse{CodeReference: &codeRef.CodeReference}, nil
}

func (s *CodeReferenceService) UpdateCodeReference(
	ctx context.Context,
	req *coderefproto.UpdateCodeReferenceRequest,
) (*coderefproto.UpdateCodeReferenceResponse, error) {
	editor, err := s.checkEnvironmentRole(
		ctx,
		accountproto.AccountV2_Role_Environment_EDITOR,
		req.EnvironmentId,
	)
	if err != nil {
		return nil, err
	}
	if err := validateUpdateCodeReferenceRequest(req); err != nil {
		return nil, err
	}
	requestHash := updateRequestFingerprint(req)

	existing, err := s.codeRefStorage.GetChangeByRequestHash(ctx, req.EnvironmentId, requestHash)
	if err == nil {
		ref, err := s.resolveExistingChange(ctx, existing)
		if err != nil {
			return nil, err
		}
		return &coderefproto.UpdateCodeReferenceResponse{CodeReference: ref}, nil
	}
	if !errors.Is(err, storage.ErrReferenceChangeNotFound) {
		s.logger.Error("Failed to look up existing change",
			log.FieldsFromIncomingContext(ctx).AddFields(zap.Error(err))...)
		return nil, statusInternal.Err()
	}

	var codeRef *domain.CodeReference
	var updatedCodeRef *domain.CodeReference
	err = s.dbClient.RunInTransactionV2(ctx, func(ctxWithTx context.Context) error {
		var err error
		codeRef, err = s.codeRefStorage.GetCodeReference(ctxWithTx, req.Id)
		if err != nil {
			return err
		}
		updatedCodeRef, err = codeRef.Update(
			req.FilePath,
			req.FileExtension,
			req.LineNumber,
			req.CodeSnippet,
			req.ContentHash,
			req.Aliases,
			req.RepositoryBranch,
			req.CommitHash,
		)
		return err
	})
	if err != nil {
		if errors.Is(err, storage.ErrCodeReferenceNotFound) {
			return nil, statusNotFound.Err()
		}
		return nil, api.NewGRPCStatus(err).Err()
	}
	changeID, err := newChangeID()
	if err != nil {
		return nil, statusInternal.Err()
	}
	change := &domain.ReferenceChange{
		ID:              changeID,
		CodeReferenceID: updatedCodeRef.Id,
		EnvironmentID:   req.EnvironmentId,
		FeatureID:       codeRef.FeatureId,
		ChangeType:      domain.ChangeTypeUpdate,
		BeforeVersion:   codeRef.Version,
		AfterVersion:    updatedCodeRef.Version,
		RequestHash:     requestHash,
		Status:          domain.ChangeStatusPending,
		CreatedAt:       updatedCodeRef.CreatedAt,
		UpdatedAt:       updatedCodeRef.UpdatedAt,
	}
	var eventPb *eventproto.Event
	buildEvent := func(sequence int64) (*eventproto.Event, error) {
		updateEvent, err := domainevent.NewEvent(
			editor,
			eventproto.Event_CODEREF,
			updatedCodeRef.Id,
			eventproto.Event_CODE_REFERENCE_UPDATED,
			&eventproto.CodeReferenceUpdatedEvent{
				Id:               updatedCodeRef.Id,
				FilePath:         updatedCodeRef.FilePath,
				LineNumber:       updatedCodeRef.LineNumber,
				CodeSnippet:      updatedCodeRef.CodeSnippet,
				ContentHash:      updatedCodeRef.ContentHash,
				Aliases:          updatedCodeRef.Aliases,
				RepositoryBranch: updatedCodeRef.RepositoryBranch,
				CommitHash:       updatedCodeRef.CommitHash,
				EnvironmentId:    updatedCodeRef.EnvironmentId,
				UpdatedAt:        updatedCodeRef.UpdatedAt,
			},
			req.EnvironmentId,
			updatedCodeRef,
			codeRef,
			domainevent.WithEventID(changeID),
			domainevent.WithChangeIdentity(
				sequence, codeRef.Version, updatedCodeRef.Version,
			),
		)
		if err != nil {
			return nil, err
		}
		eventPb = updateEvent
		return updateEvent, nil
	}
	if err := s.dbClient.RunInTransactionV2(ctx, func(ctxWithTx context.Context) error {
		return s.codeRefStorage.UpdateReferenceAndChange(
			ctxWithTx, updatedCodeRef, change, buildEvent,
		)
	}); err != nil {
		if current, lerr := s.codeRefStorage.GetChangeByRequestHash(
			ctx, req.EnvironmentId, requestHash,
		); lerr == nil {
			ref, rerr := s.resolveExistingChange(ctx, current)
			if rerr != nil {
				return nil, rerr
			}
			return &coderefproto.UpdateCodeReferenceResponse{CodeReference: ref}, nil
		}
		s.logger.Error(
			"Failed to update code reference",
			log.FieldsFromIncomingContext(ctx).AddFields(
				zap.Error(err),
				zap.String("id", req.Id),
			)...,
		)
		return nil, api.NewGRPCStatus(err).Err()
	}
	if err := s.publishChangeEvent(ctx, eventPb, changeID); err != nil {
		return nil, err
	}
	return &coderefproto.UpdateCodeReferenceResponse{CodeReference: &updatedCodeRef.CodeReference}, nil
}

func (s *CodeReferenceService) DeleteCodeReference(
	ctx context.Context,
	req *coderefproto.DeleteCodeReferenceRequest,
) (*coderefproto.DeleteCodeReferenceResponse, error) {
	editor, err := s.checkEnvironmentRole(
		ctx,
		accountproto.AccountV2_Role_Environment_EDITOR,
		req.EnvironmentId,
	)
	if err != nil {
		return nil, err
	}
	if err := validateDeleteCodeReferenceRequest(req); err != nil {
		return nil, err
	}
	requestHash := deleteRequestFingerprint(req)

	existing, err := s.codeRefStorage.GetChangeByRequestHash(ctx, req.EnvironmentId, requestHash)
	if err == nil {
		if _, err := s.resolveExistingChange(ctx, existing); err != nil {
			return nil, err
		}
		return &coderefproto.DeleteCodeReferenceResponse{}, nil
	}
	if !errors.Is(err, storage.ErrReferenceChangeNotFound) {
		s.logger.Error("Failed to look up existing change",
			log.FieldsFromIncomingContext(ctx).AddFields(zap.Error(err))...)
		return nil, statusInternal.Err()
	}

	codeRef, err := s.codeRefStorage.GetCodeReference(ctx, req.Id)
	if err != nil {
		if errors.Is(err, storage.ErrCodeReferenceNotFound) {
			return nil, statusNotFound.Err()
		}
		s.logger.Error(
			"Failed to get code reference",
			log.FieldsFromIncomingContext(ctx).AddFields(
				zap.Error(err),
				zap.String("id", req.Id),
				zap.String("environmentId", req.EnvironmentId),
			)...,
		)
		return nil, api.NewGRPCStatus(err).Err()
	}
	changeID, err := newChangeID()
	if err != nil {
		return nil, statusInternal.Err()
	}
	change := &domain.ReferenceChange{
		ID:              changeID,
		CodeReferenceID: codeRef.Id,
		EnvironmentID:   req.EnvironmentId,
		FeatureID:       codeRef.FeatureId,
		ChangeType:      domain.ChangeTypeDelete,
		BeforeVersion:   codeRef.Version,
		AfterVersion:    0,
		RequestHash:     requestHash,
		Status:          domain.ChangeStatusPending,
		CreatedAt:       storage.NowUnix(),
		UpdatedAt:       storage.NowUnix(),
	}
	var eventPb *eventproto.Event
	buildEvent := func(sequence int64) (*eventproto.Event, error) {
		deleteEvent, err := domainevent.NewEvent(
			editor,
			eventproto.Event_CODEREF,
			codeRef.Id,
			eventproto.Event_CODE_REFERENCE_DELETED,
			&eventproto.CodeReferenceDeletedEvent{
				Id:            codeRef.Id,
				EnvironmentId: codeRef.EnvironmentId,
			},
			req.EnvironmentId,
			nil,
			codeRef,
			domainevent.WithEventID(changeID),
			domainevent.WithChangeIdentity(sequence, codeRef.Version, 0),
		)
		if err != nil {
			return nil, err
		}
		eventPb = deleteEvent
		return deleteEvent, nil
	}
	if err := s.dbClient.RunInTransactionV2(ctx, func(ctxWithTx context.Context) error {
		return s.codeRefStorage.DeleteReferenceAndChange(
			ctxWithTx, change, buildEvent,
		)
	}); err != nil {
		if current, lerr := s.codeRefStorage.GetChangeByRequestHash(
			ctx, req.EnvironmentId, requestHash,
		); lerr == nil {
			if _, rerr := s.resolveExistingChange(ctx, current); rerr != nil {
				return nil, rerr
			}
			return &coderefproto.DeleteCodeReferenceResponse{}, nil
		}
		s.logger.Error(
			"Failed to delete code reference",
			log.FieldsFromIncomingContext(ctx).AddFields(
				zap.Error(err),
				zap.String("id", req.Id),
			)...,
		)
		return nil, api.NewGRPCStatus(err).Err()
	}
	if err := s.publishChangeEvent(ctx, eventPb, changeID); err != nil {
		return nil, err
	}
	return &coderefproto.DeleteCodeReferenceResponse{}, nil
}

// resolveExistingChange finishes and verifies a change that was already
// recorded for the request (repeated request or lost response):
//   - PENDING changes have their stored original event republished (补发原事件)
//     and are then marked published;
//   - PUBLISHED changes are re-verified against the reference record (复核原变更).
//
// The returned reference is the change's stored after-state at the change's
// version, so an old re-delivery can never restore a deleted reference or
// revert a newer edit. It is nil for delete changes.
func (s *CodeReferenceService) resolveExistingChange(
	ctx context.Context,
	ch *domain.ReferenceChange,
) (*coderefproto.CodeReference, error) {
	switch ch.Status {
	case domain.ChangeStatusPending:
		if len(ch.EventData) == 0 {
			s.logger.Error("Pending change without recoverable event",
				zap.String("changeId", ch.ID))
			return nil, statusInternal.Err()
		}
		event := &eventproto.Event{}
		if err := proto.Unmarshal(ch.EventData, event); err != nil {
			s.logger.Error("Failed to unmarshal stored event",
				zap.String("changeId", ch.ID), zap.Error(err))
			return nil, statusInternal.Err()
		}
		if err := s.publisher.Publish(ctx, event); err != nil {
			s.logger.Error("Failed to republish stored event",
				log.FieldsFromIncomingContext(ctx).AddFields(
					zap.Error(err),
					zap.String("changeId", ch.ID),
				)...)
			return nil, statusEventNotConfirmed.Err()
		}
		if err := s.codeRefStorage.MarkChangePublished(ctx, ch.ID); err != nil {
			s.logger.Error("Failed to mark republished change",
				zap.String("changeId", ch.ID), zap.Error(err))
			return nil, statusEventNotConfirmed.Err()
		}
		ch.Status = domain.ChangeStatusPublished
	case domain.ChangeStatusPublished:
		// Event already confirmed; nothing to publish.
	default:
		s.logger.Error("Change in unknown status",
			zap.String("changeId", ch.ID), zap.Any("status", ch.Status))
		return nil, statusInternal.Err()
	}
	return s.afterStateOfChange(ctx, ch)
}

// afterStateOfChange returns the reference snapshot stored in the change's
// event (entity data) and re-verifies that the reference record still
// represents the change. Read-only: it never writes a row.
func (s *CodeReferenceService) afterStateOfChange(
	ctx context.Context,
	ch *domain.ReferenceChange,
) (*coderefproto.CodeReference, error) {
	if ch.ChangeType == domain.ChangeTypeDelete {
		// Delete responses carry no reference.
		return nil, nil
	}
	if len(ch.EventData) == 0 {
		s.logger.Error("Change without recoverable event",
			zap.String("changeId", ch.ID))
		return nil, statusInternal.Err()
	}
	event := &eventproto.Event{}
	if err := proto.Unmarshal(ch.EventData, event); err != nil {
		s.logger.Error("Failed to unmarshal stored event",
			zap.String("changeId", ch.ID), zap.Error(err))
		return nil, statusInternal.Err()
	}
	ref := &coderefproto.CodeReference{}
	if event.EntityData != "" {
		if err := json.Unmarshal([]byte(event.EntityData), ref); err != nil {
			s.logger.Error("Failed to decode stored entity data",
				zap.String("changeId", ch.ID), zap.Error(err))
			return nil, statusInternal.Err()
		}
	}
	// Re-verify the reference record: create/update rows must exist and cannot
	// be older than the change. A later edit is fine; it belongs to a later
	// change and this response still returns the requested change's version.
	current, err := s.codeRefStorage.GetCodeReference(ctx, ch.CodeReferenceID)
	if err != nil {
		if errors.Is(err, storage.ErrCodeReferenceNotFound) {
			s.logger.Error("Reference record missing for an existing change",
				zap.String("changeId", ch.ID),
				zap.String("codeReferenceId", ch.CodeReferenceID))
			return nil, statusInternal.Err()
		}
		s.logger.Error("Failed to re-verify reference record",
			zap.String("changeId", ch.ID), zap.Error(err))
		return nil, statusInternal.Err()
	}
	if current.Version < ch.AfterVersion {
		s.logger.Error("Reference record older than the recorded change",
			zap.String("changeId", ch.ID),
			zap.Int64("currentVersion", current.Version),
			zap.Int64("afterVersion", ch.AfterVersion))
		return nil, statusInternal.Err()
	}
	ref.SourceUrl = generateSourceURL(ref)
	ref.BranchUrl = generateBranchURL(ref)
	return ref, nil
}

// publishChangeEvent publishes the stored event of a freshly committed
// change and marks the change published. A failure of either step is
// reported as statusEventNotConfirmed; the recovery relay and a retry of
// the same request then finish the change without creating a new version.
func (s *CodeReferenceService) publishChangeEvent(
	ctx context.Context,
	event *eventproto.Event,
	changeID string,
) error {
	if event == nil {
		s.logger.Error("No event built for change", zap.String("changeId", changeID))
		return statusInternal.Err()
	}
	if err := s.publisher.Publish(ctx, event); err != nil {
		s.logger.Error(
			"Failed to publish event",
			log.FieldsFromIncomingContext(ctx).AddFields(
				zap.Error(err),
				zap.String("changeId", changeID),
			)...,
		)
		return statusEventNotConfirmed.Err()
	}
	if err := s.codeRefStorage.MarkChangePublished(ctx, changeID); err != nil {
		s.logger.Error(
			"Failed to mark change published",
			log.FieldsFromIncomingContext(ctx).AddFields(
				zap.Error(err),
				zap.String("changeId", changeID),
			)...,
		)
		return statusEventNotConfirmed.Err()
	}
	return nil
}

func newChangeID() (string, error) {
	id, err := uuid.NewUUID()
	if err != nil {
		return "", err
	}
	return id.String(), nil
}
