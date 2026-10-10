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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	accountclientmock "github.com/bucketeer-io/bucketeer/v2/pkg/account/client/mock"
	coderefdomain "github.com/bucketeer-io/bucketeer/v2/pkg/coderef/domain"
	"github.com/bucketeer-io/bucketeer/v2/pkg/coderef/storage"
	storagemock "github.com/bucketeer-io/bucketeer/v2/pkg/coderef/storage/mock"
	"github.com/bucketeer-io/bucketeer/v2/pkg/log"
	"github.com/bucketeer-io/bucketeer/v2/pkg/pubsub/publisher"
	publishermock "github.com/bucketeer-io/bucketeer/v2/pkg/pubsub/publisher/mock"
	"github.com/bucketeer-io/bucketeer/v2/pkg/rpc"
	databasemock "github.com/bucketeer-io/bucketeer/v2/pkg/storage/v2/database/mock"
	"github.com/bucketeer-io/bucketeer/v2/pkg/token"
	accountproto "github.com/bucketeer-io/bucketeer/v2/proto/account"
	coderefproto "github.com/bucketeer-io/bucketeer/v2/proto/coderef"
)

// harness bundles a service with its mocked dependencies.
type harness struct {
	service   *CodeReferenceService
	account   *accountclientmock.MockClient
	storage   *storagemock.MockCodeReferenceStorage
	publisher *publishermock.MockPublisher
}

func newHarness(t *testing.T, ctrl *gomock.Controller) *harness {
	t.Helper()
	logger, err := log.NewLogger()
	require.NoError(t, err)
	h := &harness{
		account:   accountclientmock.NewMockClient(ctrl),
		storage:   storagemock.NewMockCodeReferenceStorage(ctrl),
		publisher: publishermock.NewMockPublisher(ctrl),
	}
	db := databasemock.NewMockClient(ctrl)
	// Every transaction simply executes the callback on the same context.
	db.EXPECT().
		RunInTransactionV2(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, f func(context.Context) error) error {
			return f(ctx)
		}).AnyTimes()
	h.service = NewCodeReferenceService(
		h.account, db, h.storage, h.publisher,
		WithLogger(logger),
	)
	return h
}

// ctxWithEditor builds a request context carrying a logged-in editor.
func ctxWithEditor() context.Context {
	access := &token.AccessToken{Email: "editor@example.com"}
	return context.WithValue(context.TODO(), rpc.AccessTokenKey, access)
}

// editorAccount wires the account lookup performed by the role check. The
// account owns the EDITOR environment role, so write requests are allowed.
func (h *harness) editorAccount() {
	ar := &accountproto.GetAccountV2ByEnvironmentIDResponse{
		Account: &accountproto.AccountV2{
			Email: "editor@example.com",
			EnvironmentRoles: []*accountproto.AccountV2_EnvironmentRole{
				{EnvironmentId: "env-1", Role: accountproto.AccountV2_Role_Environment_EDITOR},
			},
		},
	}
	h.account.EXPECT().
		GetAccountV2ByEnvironmentID(gomock.Any(), gomock.Any()).
		Return(ar, nil).AnyTimes()
}

func newCreateRequest() *coderefproto.CreateCodeReferenceRequest {
	return &coderefproto.CreateCodeReferenceRequest{
		FeatureId:        "feature-1",
		EnvironmentId:    "env-1",
		FilePath:         "src/flags.ts",
		FileExtension:    "ts",
		LineNumber:       12,
		CodeSnippet:      "const flag = true;",
		ContentHash:      "hash-1",
		Aliases:          []string{"flag", "flag_alias"},
		RepositoryName:   "repo",
		RepositoryOwner:  "owner",
		RepositoryType:   coderefproto.CodeReference_GITHUB,
		RepositoryBranch: "main",
		CommitHash:       "abc123",
	}
}

func newPendingChange(chID string, ct coderefdomain.ChangeType, before, after int64) *coderefdomain.ReferenceChange {
	return &coderefdomain.ReferenceChange{
		ID:              chID,
		Seq:             0,
		CodeReferenceID: "ref-1",
		EnvironmentID:   "env-1",
		FeatureID:       "feature-1",
		ChangeType:      ct,
		BeforeVersion:   before,
		AfterVersion:    after,
		RequestHash:     "request-hash",
		Status:          coderefdomain.ChangeStatusPending,
	}
}

// ---------------------------------------------------------------------------
// Create
// ---------------------------------------------------------------------------

func TestCreateCodeReference_Success(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	h := newHarness(t, ctrl)
	h.editorAccount()

	req := newCreateRequest()
	ctx := ctxWithEditor()

	// No earlier change for this request fingerprint.
	h.storage.EXPECT().
		GetChangeByRequestHash(gomock.Any(), "env-1", gomock.Any()).
		Return(nil, ErrChangeNotFound())

	// Transaction: the storage builds ref + change, invokes the event builder
	// with the allocated sequence and stores the event.
	var builtEvent interface{ GetId() string }
	h.storage.EXPECT().
		CreateReferenceAndChange(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			ctx context.Context,
			ref *coderefdomain.CodeReference,
			ch *coderefdomain.ReferenceChange,
			build storage.EventBuilder,
		) error {
			assert.Equal(t, int64(1), ref.Version)
			ev, err := build(7)
			require.NoError(t, err)
			// The event carries the stable change identity: sequence and the
			// before/after version pair.
			assert.Equal(t, int64(7), ev.Options.Sequence)
			assert.Equal(t, int64(0), ev.Options.BeforeVersion)
			assert.Equal(t, int64(1), ev.Options.AfterVersion)
			assert.Equal(t, ch.ID, ev.Id)
			assert.Equal(t, eventCODEREF, ev.EntityType)
			builtEvent = ev
			return nil
		})
	// The exact original event is published, then the change is marked.
	h.publisher.EXPECT().Publish(gomock.Any(), gomock.Any()).DoAndReturn(func(
		ctx context.Context, msg publisher.Message,
	) error {
		assert.Equal(t, builtEvent.GetId(), msg.GetId())
		return nil
	})
	h.storage.EXPECT().MarkChangePublished(gomock.Any(), gomock.Any()).Return(nil)

	resp, err := h.service.CreateCodeReference(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp.CodeReference)
	assert.Equal(t, int64(1), resp.CodeReference.Version)
}

func TestCreateCodeReference_CommitOkPublishFails_RelayRecovers(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	h := newHarness(t, ctrl)
	h.editorAccount()

	req := newCreateRequest()
	ctx := ctxWithEditor()

	h.storage.EXPECT().
		GetChangeByRequestHash(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, ErrChangeNotFound())

	// Store event bytes so the same event can be recovered later.
	storedEvent := mustBuildStoredCreateEvent(t)
	h.storage.EXPECT().
		CreateReferenceAndChange(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			ctx context.Context,
			ref *coderefdomain.CodeReference,
			ch *coderefdomain.ReferenceChange,
			build storage.EventBuilder,
		) error {
			ev, err := build(8)
			require.NoError(t, err)
			ch.EventData = storedEvent
			assert.Equal(t, ev.Id, ch.ID)
			return nil
		})

	// Publish fails after commit. The request is not confirmed; the caller
	// receives Unavailable and can retry the same request.
	h.publisher.EXPECT().
		Publish(gomock.Any(), gomock.Any()).
		Return(errors.New("stream unavailable"))
	resp, err := h.service.CreateCodeReference(ctx, req)
	require.Nil(t, resp)
	assert.Equal(t, codes.Unavailable, status.Code(err))

	// Process restart / next relay pass: the stored ORIGINAL event is
	// republished (补发原事件) and the change is marked published.
	pending := newPendingChange("ch-1", coderefdomain.ChangeTypeCreate, 0, 1)
	pending.EventData = storedEvent
	h.storage.EXPECT().ListPendingChanges(gomock.Any(), gomock.Any()).Return([]*coderefdomain.ReferenceChange{pending}, nil)
	h.publisher.EXPECT().Publish(gomock.Any(), gomock.Any()).DoAndReturn(func(
		ctx context.Context, msg publisher.Message,
	) error {
		assert.Equal(t, pending.ID, msg.GetId())
		return nil
	})
	h.storage.EXPECT().MarkChangePublished(gomock.Any(), "ch-1").Return(nil)
	h.service.recoverPendingChanges(context.Background())
}

func TestCreateCodeReference_ResponseLost_RetrySameVersion(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	h := newHarness(t, ctrl)
	h.editorAccount()

	req := newCreateRequest()
	ctx := ctxWithEditor()

	// Existing published change: no event is republished, but the reference
	// record is re-verified (复核原变更).
	existing := newPendingChange("ch-1", coderefdomain.ChangeTypeCreate, 0, 1)
	existing.Status = coderefdomain.ChangeStatusPublished
	existing.EventData = mustBuildStoredCreateEvent(t)
	h.storage.EXPECT().
		GetChangeByRequestHash(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(existing, nil)
	// Reference row exists at the change's version.
	h.storage.EXPECT().GetCodeReference(gomock.Any(), "ref-1").Return(
		&coderefdomain.CodeReference{
			CodeReference: coderefproto.CodeReference{Id: "ref-1", Version: 1},
		}, nil)

	resp, err := h.service.CreateCodeReference(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp.CodeReference)
	assert.Equal(t, int64(1), resp.CodeReference.Version)
}

func TestCreateCodeReference_RetryAfterNewerEdit_ReturnsStoredVersion(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	h := newHarness(t, ctrl)
	h.editorAccount()

	// Retry of the original create whose response was lost; meanwhile the
	// reference cannot gain a "newer edit" for a CREATE change in reality,
	// but the re-verify must accept rows newer than the change and the
	// response must still describe the change's own version.
	existing := newPendingChange("ch-1", coderefdomain.ChangeTypeCreate, 0, 1)
	existing.Status = coderefdomain.ChangeStatusPublished
	existing.EventData = mustBuildStoredCreateEvent(t)
	h.storage.EXPECT().
		GetChangeByRequestHash(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(existing, nil)
	h.storage.EXPECT().GetCodeReference(gomock.Any(), "ref-1").Return(
		&coderefdomain.CodeReference{
			CodeReference: coderefproto.CodeReference{Id: "ref-1", Version: 5},
		}, nil)

	resp, err := h.service.CreateCodeReference(ctxWithEditor(), newCreateRequest())
	require.NoError(t, err)
	assert.Equal(t, int64(1), resp.CodeReference.Version, "must not return the newer version")
}

func TestCreateCodeReference_PendingDuplicate_RepublishesStoredEvent(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	h := newHarness(t, ctrl)
	h.editorAccount()

	// A concurrent/earlier request left the change pending. Retry must not
	// create anything; the stored event is republished and marked.
	existing := newPendingChange("ch-1", coderefdomain.ChangeTypeCreate, 0, 1)
	existing.EventData = mustBuildStoredCreateEvent(t)
	h.storage.EXPECT().
		GetChangeByRequestHash(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(existing, nil)
	h.publisher.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(nil)
	h.storage.EXPECT().MarkChangePublished(gomock.Any(), "ch-1").Return(nil)
	h.storage.EXPECT().GetCodeReference(gomock.Any(), "ref-1").Return(
		&coderefdomain.CodeReference{
			CodeReference: coderefproto.CodeReference{Id: "ref-1", Version: 1},
		}, nil)

	resp, err := h.service.CreateCodeReference(ctxWithEditor(), newCreateRequest())
	require.NoError(t, err)
	assert.Equal(t, int64(1), resp.CodeReference.Version)
}

// ---------------------------------------------------------------------------
// Update
// ---------------------------------------------------------------------------

func newUpdateRequest() *coderefproto.UpdateCodeReferenceRequest {
	return &coderefproto.UpdateCodeReferenceRequest{
		Id:               "ref-1",
		EnvironmentId:    "env-1",
		FilePath:         "src/flags.ts",
		FileExtension:    "ts",
		LineNumber:       20,
		CodeSnippet:      "const flag = false;",
		ContentHash:      "hash-2",
		Aliases:          []string{"flag"},
		RepositoryName:   "repo",
		RepositoryOwner:  "owner",
		RepositoryType:   coderefproto.CodeReference_GITHUB,
		RepositoryBranch: "main",
		CommitHash:       "def456",
	}
}

func TestUpdateCodeReference_Success(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	h := newHarness(t, ctrl)
	h.editorAccount()

	ctx := ctxWithEditor()
	h.storage.EXPECT().
		GetChangeByRequestHash(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, ErrChangeNotFound())

	// First transaction: load current reference and build the updated one.
	h.storage.EXPECT().GetCodeReference(gomock.Any(), "ref-1").Return(
		&coderefdomain.CodeReference{
			CodeReference: coderefproto.CodeReference{Id: "ref-1", Version: 1},
		}, nil)

	// Second transaction: row update + change with event identity.
	h.storage.EXPECT().
		UpdateReferenceAndChange(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			ctx context.Context,
			ref *coderefdomain.CodeReference,
			ch *coderefdomain.ReferenceChange,
			build storage.EventBuilder,
		) error {
			assert.Equal(t, int64(2), ref.Version)
			ev, err := build(11)
			require.NoError(t, err)
			assert.Equal(t, int64(11), ev.Options.Sequence)
			assert.Equal(t, int64(1), ev.Options.BeforeVersion)
			assert.Equal(t, int64(2), ev.Options.AfterVersion)
			ch.EventData = mustMarshalEvent(t, ev)
			return nil
		})
	h.publisher.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(nil)
	h.storage.EXPECT().MarkChangePublished(gomock.Any(), gomock.Any()).Return(nil)

	resp, err := h.service.UpdateCodeReference(ctx, newUpdateRequest())
	require.NoError(t, err)
	assert.Equal(t, int64(2), resp.CodeReference.Version)
}

func TestUpdateCodeReference_ResponseLost_RetrySameVersionAfterNewerEdit(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	h := newHarness(t, ctrl)
	h.editorAccount()

	// The original update to version 2 was published but the response was
	// lost; a later edit moved the row to version 3. The retry must return
	// version 2, not 3, and must not revert the row.
	existing := newPendingChange("ch-u", coderefdomain.ChangeTypeUpdate, 1, 2)
	existing.Status = coderefdomain.ChangeStatusPublished
	existing.EventData = mustBuildStoredUpdateEvent(t)
	h.storage.EXPECT().
		GetChangeByRequestHash(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(existing, nil)
	h.storage.EXPECT().GetCodeReference(gomock.Any(), "ref-1").Return(
		&coderefdomain.CodeReference{
			CodeReference: coderefproto.CodeReference{Id: "ref-1", Version: 3},
		}, nil)

	resp, err := h.service.UpdateCodeReference(ctxWithEditor(), newUpdateRequest())
	require.NoError(t, err)
	assert.Equal(t, int64(2), resp.CodeReference.Version)
}

func TestUpdateCodeReference_NotFound(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	h := newHarness(t, ctrl)
	h.editorAccount()

	h.storage.EXPECT().
		GetChangeByRequestHash(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, ErrChangeNotFound())
	h.storage.EXPECT().GetCodeReference(gomock.Any(), "ref-1").Return(
		nil, refNotFound())

	resp, err := h.service.UpdateCodeReference(ctxWithEditor(), newUpdateRequest())
	assert.Nil(t, resp)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

// ---------------------------------------------------------------------------
// Delete
// ---------------------------------------------------------------------------

func TestDeleteCodeReference_Success(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	h := newHarness(t, ctrl)
	h.editorAccount()

	ref := &coderefdomain.CodeReference{
		CodeReference: coderefproto.CodeReference{
			Id: "ref-1", FeatureId: "feature-1", Version: 2,
		},
	}
	h.storage.EXPECT().
		GetChangeByRequestHash(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, ErrChangeNotFound())
	h.storage.EXPECT().GetCodeReference(gomock.Any(), "ref-1").Return(ref, nil)
	h.storage.EXPECT().
		DeleteReferenceAndChange(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			ctx context.Context,
			ch *coderefdomain.ReferenceChange,
			build storage.EventBuilder,
		) error {
			assert.Equal(t, int64(2), ch.BeforeVersion)
			assert.Equal(t, int64(0), ch.AfterVersion)
			ev, err := build(15)
			require.NoError(t, err)
			assert.Equal(t, int64(15), ev.Options.Sequence)
			ch.EventData = mustMarshalEvent(t, ev)
			return nil
		})
	h.publisher.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(nil)
	h.storage.EXPECT().MarkChangePublished(gomock.Any(), gomock.Any()).Return(nil)

	resp, err := h.service.DeleteCodeReference(ctxWithEditor(),
		&coderefproto.DeleteCodeReferenceRequest{Id: "ref-1", EnvironmentId: "env-1"})
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestDeleteCodeReference_RepeatedDelete_Idempotent(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	h := newHarness(t, ctrl)
	h.editorAccount()

	existing := newPendingChange("ch-d", coderefdomain.ChangeTypeDelete, 2, 0)
	existing.Status = coderefdomain.ChangeStatusPublished
	existing.EventData = mustBuildStoredDeleteEvent(t)
	h.storage.EXPECT().
		GetChangeByRequestHash(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(existing, nil)

	resp, err := h.service.DeleteCodeReference(ctxWithEditor(),
		&coderefproto.DeleteCodeReferenceRequest{Id: "ref-1", EnvironmentId: "env-1"})
	require.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestDeleteCodeReference_PublishFails_RelayThenRetry(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	h := newHarness(t, ctrl)
	h.editorAccount()

	ref := &coderefdomain.CodeReference{
		CodeReference: coderefproto.CodeReference{
			Id: "ref-1", FeatureId: "feature-1", Version: 1,
		},
	}
	h.storage.EXPECT().
		GetChangeByRequestHash(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, ErrChangeNotFound())
	h.storage.EXPECT().GetCodeReference(gomock.Any(), "ref-1").Return(ref, nil)
	stored := mustBuildStoredDeleteEvent(t)
	h.storage.EXPECT().
		DeleteReferenceAndChange(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(
			ctx context.Context,
			ch *coderefdomain.ReferenceChange,
			build storage.EventBuilder,
		) error {
			ev, err := build(3)
			require.NoError(t, err)
			ch.EventData = stored
			assert.Equal(t, ev.Id, ch.ID)
			return nil
		})
	h.publisher.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(errors.New("fail"))

	resp, err := h.service.DeleteCodeReference(ctxWithEditor(),
		&coderefproto.DeleteCodeReferenceRequest{Id: "ref-1", EnvironmentId: "env-1"})
	assert.Nil(t, resp)
	assert.Equal(t, codes.Unavailable, status.Code(err))

	// Relay after restart republishes the stored delete event.
	pending := newPendingChange("ch-x", coderefdomain.ChangeTypeDelete, 1, 0)
	pending.EventData = stored
	h.storage.EXPECT().ListPendingChanges(gomock.Any(), gomock.Any()).
		Return([]*coderefdomain.ReferenceChange{pending}, nil)
	h.publisher.EXPECT().Publish(gomock.Any(), gomock.Any()).Return(nil)
	h.storage.EXPECT().MarkChangePublished(gomock.Any(), pending.ID).Return(nil)
	h.service.recoverPendingChanges(context.Background())
}

// ---------------------------------------------------------------------------
// Recovery relay edge cases
// ---------------------------------------------------------------------------

func TestRecovery_SkipsRowsWithoutEvent(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	h := newHarness(t, ctrl)

	// A pending row without stored event is corrupt and must be skipped, not
	// crash the relay.
	pending := newPendingChange("ch-bad", coderefdomain.ChangeTypeCreate, 0, 1)
	pending.EventData = nil
	h.storage.EXPECT().ListPendingChanges(gomock.Any(), gomock.Any()).
		Return([]*coderefdomain.ReferenceChange{pending}, nil)
	// No publish, no mark.
	h.service.recoverPendingChanges(context.Background())
}

// ---------------------------------------------------------------------------
// Permission semantics remain unchanged.
// ---------------------------------------------------------------------------

func TestMutation_RoleUnassigned_PermissionDenied(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	h := newHarness(t, ctrl)

	ar := &accountproto.GetAccountV2ByEnvironmentIDResponse{
		Account: &accountproto.AccountV2{
			Email: "viewer@example.com",
			EnvironmentRoles: []*accountproto.AccountV2_EnvironmentRole{
				{EnvironmentId: "env-1", Role: accountproto.AccountV2_Role_Environment_VIEWER},
			},
		},
	}
	h.account.EXPECT().
		GetAccountV2ByEnvironmentID(gomock.Any(), gomock.Any()).
		Return(ar, nil).AnyTimes()

	resp, err := h.service.CreateCodeReference(ctxWithEditor(), newCreateRequest())
	assert.Nil(t, resp)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
}
