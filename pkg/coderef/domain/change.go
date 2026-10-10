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

package domain

// ChangeType is the type of a code reference change. The numeric values are
// persisted in the code_reference_change table and must not change.
type ChangeType int32

const (
	ChangeTypeUnspecified ChangeType = 0
	// ChangeTypeCreate is recorded when a code reference is created.
	ChangeTypeCreate ChangeType = 1
	// ChangeTypeUpdate is recorded when a code reference is updated.
	ChangeTypeUpdate ChangeType = 2
	// ChangeTypeDelete is recorded when a code reference is deleted.
	ChangeTypeDelete ChangeType = 3
)

// ChangeStatus is the publication status of the change. The numeric values
// are persisted in the code_reference_change table and must not change.
type ChangeStatus int32

const (
	// ChangeStatusPending means the reference row is committed but the domain
	// event has not been confirmed as published yet.
	ChangeStatusPending ChangeStatus = 1
	// ChangeStatusPublished means the domain event of the change is published.
	ChangeStatusPublished ChangeStatus = 2
)

// ReferenceChange is one create/update/delete change of a code reference.
// It is the transactional outbox row and the idempotency ledger entry.
type ReferenceChange struct {
	// ID is the stable change identity. It equals the domain event id.
	ID string
	// Seq is the monotonically increasing change sequence. It is used as the
	// reference version watermark by the automatic archiver.
	Seq int64
	// CodeReferenceID is the id of the changed code reference.
	CodeReferenceID string
	EnvironmentID   string
	FeatureID       string
	ChangeType      ChangeType
	// BeforeVersion is the reference version before the change. 0 on create.
	BeforeVersion int64
	// AfterVersion is the reference version after the change. 0 on delete.
	AfterVersion int64
	// RequestHash is the fingerprint of the request that produced the change.
	// Repeated requests with the same fingerprint resolve to this change.
	RequestHash string
	// EventData is the serialized domain event. It enables republishing the
	// exact same event after failures. NULL only for backfilled rows.
	EventData []byte
	Status    ChangeStatus
	CreatedAt int64
	UpdatedAt int64
}
