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
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/bucketeer-io/bucketeer/v2/pkg/coderef/domain"
	"github.com/bucketeer-io/bucketeer/v2/pkg/coderef/storage"
	domainevent "github.com/bucketeer-io/bucketeer/v2/pkg/domainevent/domain"
	coderefproto "github.com/bucketeer-io/bucketeer/v2/proto/coderef"
	eventproto "github.com/bucketeer-io/bucketeer/v2/proto/event/domain"
)

// ErrChangeNotFound returns the sentinel for a missing change.
func ErrChangeNotFound() error {
	return storage.ErrReferenceChangeNotFound
}

// eventCODEREF is the entity type constant used in events.
var eventCODEREF = eventproto.Event_CODEREF

func mustMarshalEvent(t *testing.T, ev *eventproto.Event) []byte {
	t.Helper()
	b, err := proto.Marshal(ev)
	require.NoError(t, err)
	return b
}

// fullReference builds the complete reference snapshot used as event entity data.
func fullReference(version int64) *domain.CodeReference {
	return &domain.CodeReference{
		CodeReference: coderefproto.CodeReference{
			Id:               "ref-1",
			FeatureId:        "feature-1",
			FilePath:         "src/flags.ts",
			FileExtension:    "ts",
			LineNumber:       12,
			CodeSnippet:      "const flag = true;",
			ContentHash:      "hash-1",
			Aliases:          []string{"flag"},
			RepositoryName:   "repo",
			RepositoryOwner:  "owner",
			RepositoryType:   coderefproto.CodeReference_GITHUB,
			RepositoryBranch: "main",
			CommitHash:       "abc123",
			EnvironmentId:    "env-1",
			CreatedAt:        1000,
			UpdatedAt:        2000,
			Version:          version,
		},
	}
}

func mustBuildStoredCreateEvent(t *testing.T) []byte {
	t.Helper()
	after := fullReference(1)
	ev, err := domainevent.NewEvent(
		&eventproto.Editor{Email: "editor@example.com"},
		eventproto.Event_CODEREF,
		"ref-1",
		eventproto.Event_CODE_REFERENCE_CREATED,
		&eventproto.CodeReferenceCreatedEvent{
			Id: "ref-1", FeatureId: "feature-1",
			FilePath: "src/flags.ts", LineNumber: 12,
			CodeSnippet: "const flag = true;", ContentHash: "hash-1",
			Aliases: []string{"flag"},
			RepositoryName: "repo", RepositoryOwner: "owner",
			RepositoryType: coderefproto.CodeReference_GITHUB,
			RepositoryBranch: "main", CommitHash: "abc123",
			EnvironmentId: "env-1", CreatedAt: 1000, UpdatedAt: 2000,
		},
		"env-1",
		after,
		&coderefproto.CodeReference{},
		domainevent.WithEventID("ch-1"),
		domainevent.WithChangeIdentity(7, 0, 1),
	)
	require.NoError(t, err)
	return mustMarshalEvent(t, ev)
}

func mustBuildStoredUpdateEvent(t *testing.T) []byte {
	t.Helper()
	after := fullReference(2)
	after.UpdatedAt = 3000
	before := fullReference(1)
	ev, err := domainevent.NewEvent(
		&eventproto.Editor{Email: "editor@example.com"},
		eventproto.Event_CODEREF,
		"ref-1",
		eventproto.Event_CODE_REFERENCE_UPDATED,
		&eventproto.CodeReferenceUpdatedEvent{
			Id: "ref-1",
			FilePath: "src/flags.ts", LineNumber: 12,
			CodeSnippet: "const flag = true;", ContentHash: "hash-1",
			Aliases: []string{"flag"},
			RepositoryBranch: "main", CommitHash: "abc123",
			EnvironmentId: "env-1", UpdatedAt: 3000,
		},
		"env-1",
		after,
		before,
		domainevent.WithEventID("ch-u"),
		domainevent.WithChangeIdentity(9, 1, 2),
	)
	require.NoError(t, err)
	return mustMarshalEvent(t, ev)
}

func mustBuildStoredDeleteEvent(t *testing.T) []byte {
	t.Helper()
	before := fullReference(1)
	ev, err := domainevent.NewEvent(
		&eventproto.Editor{Email: "editor@example.com"},
		eventproto.Event_CODEREF,
		"ref-1",
		eventproto.Event_CODE_REFERENCE_DELETED,
		&eventproto.CodeReferenceDeletedEvent{
			Id: "ref-1", EnvironmentId: "env-1",
		},
		"env-1",
		nil,
		before,
		domainevent.WithEventID("ch-d"),
		domainevent.WithChangeIdentity(3, 1, 0),
	)
	require.NoError(t, err)
	return mustMarshalEvent(t, ev)
}

// refNotFound returns the storage sentinel for a missing reference.
func refNotFound() error {
	return storage.ErrCodeReferenceNotFound
}
