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

package processor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	accdomain "github.com/bucketeer-io/bucketeer/v2/pkg/account/domain"
	v2as "github.com/bucketeer-io/bucketeer/v2/pkg/account/storage/v2"
	accmock "github.com/bucketeer-io/bucketeer/v2/pkg/account/storage/v2/mock"
	"github.com/bucketeer-io/bucketeer/v2/pkg/email"
	"github.com/bucketeer-io/bucketeer/v2/pkg/email/domain"
	emailstorage "github.com/bucketeer-io/bucketeer/v2/pkg/email/storage/v2"
	"github.com/bucketeer-io/bucketeer/v2/pkg/pubsub/puller"
	domaineventproto "github.com/bucketeer-io/bucketeer/v2/proto/event/domain"

	gomock "go.uber.org/mock/gomock"
)

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

// fakeProvider returns queued outcomes and records every interaction.
type fakeProvider struct {
	name         string
	outcomes     []email.Outcome
	calls        int
	sendIDs      []string
	querier      bool
	queryCalls   int
	queryOutcome email.Outcome
}

func (p *fakeProvider) Name() string { return p.name }

func (p *fakeProvider) Send(_ context.Context, req email.SendRequest) email.SendResult {
	p.calls++
	p.sendIDs = append(p.sendIDs, req.DeliveryID)
	var outcome email.Outcome
	if p.calls <= len(p.outcomes) {
		outcome = p.outcomes[p.calls-1]
	} else {
		outcome = p.outcomes[len(p.outcomes)-1]
	}
	result := email.SendResult{Outcome: outcome}
	switch outcome {
	case email.OutcomeAccepted:
		result.Receipt = email.Receipt{
			StatusCode: 200,
			MessageID:  p.name + "-receipt-" + itoaCalls(p.calls),
		}
	case email.OutcomeRejected:
		result.Err = errors.New(p.name + ": explicit rejection")
		result.Receipt.StatusCode = 400
	case email.OutcomeRetryable:
		result.Err = errors.New(p.name + ": temporary failure")
		result.Receipt.StatusCode = 429
	default:
		result.Err = errors.New(p.name + ": timeout")
		result.Receipt.StatusCode = 0
	}
	return result
}

func (p *fakeProvider) QueryReceipt(_ context.Context, deliveryID string) (email.Outcome, email.Receipt, error) {
	p.queryCalls++
	if !p.querier {
		return 0, email.Receipt{}, email.ErrReceiptQueryUnsupported
	}
	return p.queryOutcome, email.Receipt{
		StatusCode: 200,
		MessageID:  p.name + "-receipt-query",
	}, nil
}

// fakeEmailService implements email.Service.
type fakeEmailService struct {
	disabled     bool
	renderErr    error
	providers    []email.Provider
	renderCalls  int
	renderedLang []string
}

func (s *fakeEmailService) Disabled() bool { return s.disabled }

func (s *fakeEmailService) RenderWelcome(language, to string) (email.RenderedEmail, error) {
	s.renderCalls++
	s.renderedLang = append(s.renderedLang, language)
	if s.renderErr != nil {
		return email.RenderedEmail{}, s.renderErr
	}
	resolved := "en"
	if language == "ja" {
		resolved = "ja"
	}
	return email.RenderedEmail{
		Subject:         "Subject-" + to,
		Body:            "Body",
		Language:        resolved,
		TemplateVersion: "v1",
	}, nil
}

func (s *fakeEmailService) Chain() []email.Provider { return s.providers }

func (s *fakeEmailService) Provider(name string) (email.Provider, bool) {
	for _, p := range s.providers {
		if p.Name() == name {
			return p, true
		}
	}
	return nil, false
}

// fakeDeliveryStorage is an in-memory DeliveryStorage that mirrors the real
// conditional SQL semantics.
type fakeDeliveryStorage struct {
	mu         sync.Mutex
	deliveries map[string]*domain.Delivery
	attempts   map[string][]*domain.Attempt
	// failUpdateAttempt remaining times UpdateAttempt returns an error.
	failUpdateAttempt int
}

func newFakeDeliveryStorage() *fakeDeliveryStorage {
	return &fakeDeliveryStorage{
		deliveries: map[string]*domain.Delivery{},
		attempts:   map[string][]*domain.Attempt{},
	}
}

func (s *fakeDeliveryStorage) CreateDelivery(_ context.Context, d *domain.Delivery) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.deliveries[d.ID]; ok {
		return emailstorage.ErrDeliveryAlreadyExists
	}
	cp := *d
	s.deliveries[d.ID] = &cp
	return nil
}

func (s *fakeDeliveryStorage) UpdateDelivery(_ context.Context, d *domain.Delivery) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.deliveries[d.ID]; !ok {
		return emailstorage.ErrDeliveryNotFound
	}
	cp := *d
	s.deliveries[d.ID] = &cp
	return nil
}

func (s *fakeDeliveryStorage) GetDelivery(_ context.Context, id string) (*domain.Delivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.deliveries[id]
	if !ok {
		return nil, emailstorage.ErrDeliveryNotFound
	}
	cp := *d
	return &cp, nil
}

func (s *fakeDeliveryStorage) CreateAttempt(_ context.Context, a *domain.Attempt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.attempts[a.DeliveryID] {
		if existing.ID == a.ID {
			return emailstorage.ErrAttemptAlreadyExists
		}
	}
	cp := *a
	s.attempts[a.DeliveryID] = append(s.attempts[a.DeliveryID], &cp)
	return nil
}

func (s *fakeDeliveryStorage) UpdateAttempt(_ context.Context, a *domain.Attempt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failUpdateAttempt > 0 {
		s.failUpdateAttempt--
		return errors.New("fake: update attempt failed")
	}
	list := s.attempts[a.DeliveryID]
	for i, existing := range list {
		if existing.ID == a.ID {
			cp := *a
			list[i] = &cp
			s.attempts[a.DeliveryID] = list
			return nil
		}
	}
	return emailstorage.ErrDeliveryNotFound
}

func (s *fakeDeliveryStorage) ListAttempts(_ context.Context, deliveryID string) ([]*domain.Attempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.attempts[deliveryID]
	out := make([]*domain.Attempt, len(list))
	for i, a := range list {
		cp := *a
		out[i] = &cp
	}
	return out, nil
}

func (s *fakeDeliveryStorage) AcquireLease(
	_ context.Context,
	id, owner string,
	ttlSec, now int64,
) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.deliveries[id]
	if !ok || d.Status.Terminal() {
		return false, nil
	}
	if d.LeaseUntil >= now && d.LeaseOwner != owner {
		return false, nil
	}
	d.LeaseOwner = owner
	d.LeaseUntil = now + ttlSec
	d.UpdatedAt = now
	return true, nil
}

func (s *fakeDeliveryStorage) ReleaseLease(_ context.Context, id, owner string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.deliveries[id]
	if !ok || d.LeaseOwner != owner {
		return nil
	}
	d.LeaseOwner = ""
	d.LeaseUntil = 0
	return nil
}

func (s *fakeDeliveryStorage) ListDueDeliveries(
	_ context.Context,
	now int64,
	limit int,
) ([]*domain.Delivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []*domain.Delivery{}
	for _, d := range s.deliveries {
		if d.Status == domain.DeliveryStatusRenderFailed ||
			d.Status == domain.DeliveryStatusBackoff ||
			d.Status == domain.DeliveryStatusPendingConfirmation {
			if d.NextRetryAt <= now && (d.LeaseUntil < now || d.LeaseOwner == "") {
				out = append(out, d)
				if len(out) >= limit {
					break
				}
			}
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func itoaCalls(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

func newTestEmailSender(
	svc email.Service,
	accountStorage v2as.AccountStorage,
	deliveryStorage emailstorage.DeliveryStorage,
) *emailSender {
	return &emailSender{
		emailService:    svc,
		accountStorage:  accountStorage,
		deliveryStorage: deliveryStorage,
		logger:          zap.NewNop(),
		leaseOwner:      "test-owner",
	}
}

func testEvent(eventID string) *domaineventproto.Event {
	return &domaineventproto.Event{
		Id:         eventID,
		EntityType: domaineventproto.Event_ACCOUNT,
		Type:       domaineventproto.Event_ACCOUNT_V2_CREATED,
	}
}

func testAccountEvent(emailAddr, language string) *domaineventproto.AccountV2CreatedEvent {
	return &domaineventproto.AccountV2CreatedEvent{
		Email:          emailAddr,
		Language:       language,
		OrganizationId: "org-1",
	}
}

func existingDelivery(s *fakeDeliveryStorage, eventID string) *domain.Delivery {
	d, _ := s.GetDelivery(context.Background(), domain.DeriveDeliveryID(eventID))
	return d
}

// buildWelcomeMessage builds a puller message wrapping a welcome event.
func buildWelcomeMessage(eventID string, ev *domaineventproto.AccountV2CreatedEvent) *puller.Message {
	data, _ := anypb.New(ev)
	event := &domaineventproto.Event{
		Id:         eventID,
		EntityType: domaineventproto.Event_ACCOUNT,
		Type:       domaineventproto.Event_ACCOUNT_V2_CREATED,
		Data:       data,
	}
	bytes, _ := proto.Marshal(event)
	return &puller.Message{
		ID:         "msg-" + eventID,
		Data:       bytes,
		Attributes: map[string]string{"id": "msg-" + eventID},
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestDeliverHappyPathAndIdempotentRedelivery(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	accountMock := accmock.NewMockAccountStorage(ctrl)
	accountMock.EXPECT().GetAccountV2(gomock.Any(), "user@example.com", "org-1").
		Return(&accdomain.AccountV2{}, nil).Times(1)

	p1 := &fakeProvider{name: "smtp", outcomes: []email.Outcome{email.OutcomeAccepted}}
	svc := &fakeEmailService{providers: []email.Provider{p1}}
	store := newFakeDeliveryStorage()
	e := newTestEmailSender(svc, accountMock, store)

	event := testEvent("event-1")
	ev := testAccountEvent("user@example.com", "en")

	// First delivery: accepted and acknowledged.
	assert.Equal(t, decisionAck, e.deliver(context.Background(), event, ev))
	assert.Equal(t, 1, p1.calls)

	d := existingDelivery(store, "event-1")
	assert.Equal(t, domain.DeliveryStatusAccepted, d.Status)
	assert.Equal(t, "smtp", d.AcceptedProvider)
	assert.Equal(t, "smtp-receipt-1", d.ReceiptID)
	assert.Equal(t, 1, d.AttemptCount)
	assert.NotNil(t, d.Snapshot)

	// Redelivery of the same event: no second email, still acknowledged.
	assert.Equal(t, decisionAck, e.deliver(context.Background(), event, ev))
	assert.Equal(t, 1, p1.calls)
}

func TestDeliverUnknownThenAcceptedSameProviderNoFailover(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	accountMock := accmock.NewMockAccountStorage(ctrl)
	accountMock.EXPECT().GetAccountV2(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&accdomain.AccountV2{}, nil).Times(1)

	p1 := &fakeProvider{name: "ses", outcomes: []email.Outcome{
		email.OutcomeUnknown, email.OutcomeAccepted,
	}}
	p2 := &fakeProvider{name: "sendgrid", outcomes: []email.Outcome{email.OutcomeAccepted}}
	svc := &fakeEmailService{providers: []email.Provider{p1, p2}}
	store := newFakeDeliveryStorage()
	e := newTestEmailSender(svc, accountMock, store)

	event := testEvent("event-2")
	ev := testAccountEvent("user@example.com", "en")

	// Uncertain first response: pending confirmation, not acknowledged.
	assert.Equal(t, decisionNack, e.deliver(context.Background(), event, ev))
	d := existingDelivery(store, "event-2")
	assert.Equal(t, domain.DeliveryStatusPendingConfirmation, d.Status)
	assert.Equal(t, "ses", d.LastProvider)
	assert.True(t, d.NextRetryAt > 0)

	// Redelivery: same provider, same delivery identity, accepted.
	assert.Equal(t, decisionAck, e.deliver(context.Background(), event, ev))
	assert.Equal(t, 2, p1.calls)
	assert.Equal(t, 0, p2.calls, "no failover after an uncertain response")
	assert.Equal(t, []string{
		domain.DeriveDeliveryID("event-2"),
		domain.DeriveDeliveryID("event-2"),
	}, p1.sendIDs)

	attempts, _ := store.ListAttempts(context.Background(), d.ID)
	assert.Equal(t, 2, len(attempts))
	assert.Equal(t, domain.AttemptResultUnknown, attempts[0].Result)
	assert.Equal(t, domain.AttemptResultAccepted, attempts[1].Result)
	assert.Equal(t, domain.DeliveryStatusAccepted, existingDelivery(store, "event-2").Status)
}

func TestDeliverPendingConfirmationResolvedByReceiptQuery(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	accountMock := accmock.NewMockAccountStorage(ctrl)
	accountMock.EXPECT().GetAccountV2(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&accdomain.AccountV2{}, nil).Times(1)

	p1 := &fakeProvider{
		name:         "sendgrid",
		outcomes:     []email.Outcome{email.OutcomeUnknown},
		querier:      true,
		queryOutcome: email.OutcomeAccepted,
	}
	svc := &fakeEmailService{providers: []email.Provider{p1}}
	store := newFakeDeliveryStorage()
	e := newTestEmailSender(svc, accountMock, store)

	event := testEvent("event-query")
	ev := testAccountEvent("user@example.com", "en")

	assert.Equal(t, decisionNack, e.deliver(context.Background(), event, ev))
	// Second pass resolves via query: no extra Send call.
	assert.Equal(t, decisionAck, e.deliver(context.Background(), event, ev))
	assert.Equal(t, 1, p1.calls)
	assert.Equal(t, 1, p1.queryCalls)
	assert.Equal(t, domain.DeliveryStatusAccepted, existingDelivery(store, "event-query").Status)
}

func TestDeliverTemporaryFailureSchedulesBackoff(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	accountMock := accmock.NewMockAccountStorage(ctrl)
	accountMock.EXPECT().GetAccountV2(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&accdomain.AccountV2{}, nil).Times(1)

	p1 := &fakeProvider{name: "smtp", outcomes: []email.Outcome{
		email.OutcomeRetryable, email.OutcomeAccepted,
	}}
	svc := &fakeEmailService{providers: []email.Provider{p1}}
	store := newFakeDeliveryStorage()
	e := newTestEmailSender(svc, accountMock, store)

	event := testEvent("event-backoff")
	ev := testAccountEvent("user@example.com", "en")

	assert.Equal(t, decisionNack, e.deliver(context.Background(), event, ev))
	d := existingDelivery(store, "event-backoff")
	assert.Equal(t, domain.DeliveryStatusBackoff, d.Status)
	assert.True(t, d.NextRetryAt > 0)

	// Redelivery after backoff: same provider, accepted.
	assert.Equal(t, decisionAck, e.deliver(context.Background(), event, ev))
	assert.Equal(t, 2, p1.calls)
}

func TestDeliverExplicitRejectionNoFailoverTerminal(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	accountMock := accmock.NewMockAccountStorage(ctrl)
	accountMock.EXPECT().GetAccountV2(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&accdomain.AccountV2{}, nil).Times(1)

	p1 := &fakeProvider{name: "smtp", outcomes: []email.Outcome{email.OutcomeRejected}}
	// Failover policy disabled: chain contains only the primary.
	svc := &fakeEmailService{providers: []email.Provider{p1}}
	store := newFakeDeliveryStorage()
	e := newTestEmailSender(svc, accountMock, store)

	event := testEvent("event-rejected")
	ev := testAccountEvent("user@example.com", "en")

	assert.Equal(t, decisionAck, e.deliver(context.Background(), event, ev))
	d := existingDelivery(store, "event-rejected")
	assert.Equal(t, domain.DeliveryStatusRejected, d.Status)
	assert.Equal(t, 1, p1.calls)
}

func TestDeliverExplicitRejectionSwitchesBackupProvider(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	accountMock := accmock.NewMockAccountStorage(ctrl)
	accountMock.EXPECT().GetAccountV2(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&accdomain.AccountV2{}, nil).Times(1)

	p1 := &fakeProvider{name: "smtp", outcomes: []email.Outcome{email.OutcomeRejected}}
	p2 := &fakeProvider{name: "ses", outcomes: []email.Outcome{email.OutcomeAccepted}}
	// Failover enabled: chain contains the backup.
	svc := &fakeEmailService{providers: []email.Provider{p1, p2}}
	store := newFakeDeliveryStorage()
	e := newTestEmailSender(svc, accountMock, store)

	event := testEvent("event-failover")
	ev := testAccountEvent("user@example.com", "en")

	// First pass rejected: not acked, backup is available.
	assert.Equal(t, decisionNack, e.deliver(context.Background(), event, ev))
	assert.Equal(t, 1, p1.calls)
	assert.Equal(t, 0, p2.calls)

	// Second pass: backup provider accepts with the SAME delivery identity.
	assert.Equal(t, decisionAck, e.deliver(context.Background(), event, ev))
	assert.Equal(t, 1, p2.calls)
	assert.Equal(t, []string{domain.DeriveDeliveryID("event-failover")}, p2.sendIDs)

	d := existingDelivery(store, "event-failover")
	assert.Equal(t, domain.DeliveryStatusAccepted, d.Status)
	assert.Equal(t, "ses", d.AcceptedProvider)
	assert.Equal(t, 2, d.AttemptCount)
}

func TestDeliverRenderFailureKeptRepairableThenAccepted(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	accountMock := accmock.NewMockAccountStorage(ctrl)
	accountMock.EXPECT().GetAccountV2(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&accdomain.AccountV2{}, nil).AnyTimes()

	p1 := &fakeProvider{name: "smtp", outcomes: []email.Outcome{email.OutcomeAccepted}}
	svc := &fakeEmailService{
		providers: []email.Provider{p1},
		renderErr: errors.New("template broken"),
	}
	store := newFakeDeliveryStorage()
	e := newTestEmailSender(svc, accountMock, store)

	event := testEvent("event-render")
	ev := testAccountEvent("user@example.com", "en")

	assert.Equal(t, decisionNack, e.deliver(context.Background(), event, ev))
	d := existingDelivery(store, "event-render")
	assert.Equal(t, domain.DeliveryStatusRenderFailed, d.Status)
	assert.Nil(t, d.Snapshot)
	assert.Equal(t, 0, p1.calls)
	assert.Equal(t, 0, d.AttemptCount)
	assert.True(t, d.NextRetryAt > 0)

	// Template repaired: redelivery renders and sends.
	svc.renderErr = nil
	assert.Equal(t, decisionAck, e.deliver(context.Background(), event, ev))
	assert.Equal(t, 1, p1.calls)
	assert.Equal(t, domain.DeliveryStatusAccepted, existingDelivery(store, "event-render").Status)
}

func TestDeliverReceiptSaveFailureLeavesAttemptInProgress(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	accountMock := accmock.NewMockAccountStorage(ctrl)
	accountMock.EXPECT().GetAccountV2(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&accdomain.AccountV2{}, nil).Times(1)

	p1 := &fakeProvider{name: "ses", outcomes: []email.Outcome{email.OutcomeAccepted}}
	svc := &fakeEmailService{providers: []email.Provider{p1}}
	store := newFakeDeliveryStorage()
	store.failUpdateAttempt = 1 // receipt cannot be saved on the first pass
	e := newTestEmailSender(svc, accountMock, store)

	event := testEvent("event-receipt")
	ev := testAccountEvent("user@example.com", "en")

	// Provider accepted, but saving the receipt failed: do not acknowledge.
	assert.Equal(t, decisionNack, e.deliver(context.Background(), event, ev))
	attempts, _ := store.ListAttempts(
		context.Background(), domain.DeriveDeliveryID("event-receipt"))
	assert.Equal(t, 1, len(attempts))
	assert.Equal(t, domain.AttemptResultInProgress, attempts[0].Result)

	// Second pass reuses the in-progress attempt and records the receipt.
	assert.Equal(t, decisionAck, e.deliver(context.Background(), event, ev))
	attempts, _ = store.ListAttempts(
		context.Background(), domain.DeriveDeliveryID("event-receipt"))
	assert.Equal(t, 1, len(attempts), "same attempt reused, no new provider email")
	assert.Equal(t, domain.AttemptResultAccepted, attempts[0].Result)
	assert.Equal(t, domain.DeliveryStatusAccepted, existingDelivery(store, "event-receipt").Status)
}

func TestDeliverAccountNotFoundBeforeStartIsCanceled(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	accountMock := accmock.NewMockAccountStorage(ctrl)
	accountMock.EXPECT().GetAccountV2(gomock.Any(), "user@example.com", "org-1").
		Return(nil, v2as.ErrAccountNotFound).Times(1)

	p1 := &fakeProvider{name: "smtp", outcomes: []email.Outcome{email.OutcomeAccepted}}
	svc := &fakeEmailService{providers: []email.Provider{p1}}
	store := newFakeDeliveryStorage()
	e := newTestEmailSender(svc, accountMock, store)

	event := testEvent("event-cancel")
	ev := testAccountEvent("user@example.com", "en")

	assert.Equal(t, decisionAck, e.deliver(context.Background(), event, ev))
	d := existingDelivery(store, "event-cancel")
	assert.Equal(t, domain.DeliveryStatusCanceled, d.Status)
	assert.Equal(t, domain.CancelReasonAccountNotFound, d.CancelReason)
	assert.Equal(t, 0, p1.calls)

	// Redelivery stays canceled.
	assert.Equal(t, decisionAck, e.deliver(context.Background(), event, ev))
	assert.Equal(t, 0, p1.calls)
}

func TestDeliverStartedDeliveryConvergesWithFrozenSnapshotWhenAccountDeleted(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	accountMock := accmock.NewMockAccountStorage(ctrl)
	// Account lookup happens exactly once, before the first attempt.
	accountMock.EXPECT().GetAccountV2(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&accdomain.AccountV2{}, nil).Times(1)

	p1 := &fakeProvider{name: "smtp", outcomes: []email.Outcome{
		email.OutcomeUnknown, email.OutcomeAccepted,
	}}
	svc := &fakeEmailService{providers: []email.Provider{p1}}
	store := newFakeDeliveryStorage()
	e := newTestEmailSender(svc, accountMock, store)

	event := testEvent("event-started")
	ev := testAccountEvent("user@example.com", "en")

	assert.Equal(t, decisionNack, e.deliver(context.Background(), event, ev))
	d := existingDelivery(store, "event-started")
	assert.True(t, d.HasStarted())
	frozen := d.Snapshot

	// Even though the account is gone now, the started delivery converges
	// with the frozen snapshot; no cancellation.
	assert.Equal(t, decisionAck, e.deliver(context.Background(), event, ev))
	d = existingDelivery(store, "event-started")
	assert.Equal(t, domain.DeliveryStatusAccepted, d.Status)
	assert.Same(t, frozen, d.Snapshot)
	assert.Equal(t, "user@example.com", d.Snapshot.To)
	assert.Equal(t, 2, p1.calls)
}

func TestDeliverUnknownLanguageFallsBackToEnglish(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	accountMock := accmock.NewMockAccountStorage(ctrl)
	accountMock.EXPECT().GetAccountV2(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&accdomain.AccountV2{}, nil).Times(1)

	p1 := &fakeProvider{name: "smtp", outcomes: []email.Outcome{email.OutcomeAccepted}}
	svc := &fakeEmailService{providers: []email.Provider{p1}}
	store := newFakeDeliveryStorage()
	e := newTestEmailSender(svc, accountMock, store)

	event := testEvent("event-language")
	ev := testAccountEvent("user@example.com", "fr")

	assert.Equal(t, decisionAck, e.deliver(context.Background(), event, ev))
	d := existingDelivery(store, "event-language")
	assert.Equal(t, "fr", d.Snapshot.RequestedLanguage)
	assert.Equal(t, "en", d.Snapshot.Language, "unknown language falls back to English")
}

func TestHandleMessageDisabledServiceAcksWithoutDelivery(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	accountMock := accmock.NewMockAccountStorage(ctrl)

	p1 := &fakeProvider{name: "smtp", outcomes: []email.Outcome{email.OutcomeAccepted}}
	svc := &fakeEmailService{disabled: true, providers: []email.Provider{p1}}
	store := newFakeDeliveryStorage()
	e := newTestEmailSender(svc, accountMock, store)

	msg := buildWelcomeMessage("event-disabled", testAccountEvent("user@example.com", "en"))
	acked := false
	msg.Ack = func() { acked = true }
	msg.Nack = func() { t.Error("message should not be nacked") }

	e.handleMessage(context.Background(), msg)

	assert.True(t, acked)
	assert.Equal(t, 0, p1.calls)
	assert.Equal(t, 0, len(store.deliveries))
}

func TestSchedulerConvergesDueDelivery(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	accountMock := accmock.NewMockAccountStorage(ctrl)
	accountMock.EXPECT().GetAccountV2(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&accdomain.AccountV2{}, nil).AnyTimes()

	p1 := &fakeProvider{name: "smtp", outcomes: []email.Outcome{
		email.OutcomeRetryable, email.OutcomeAccepted,
	}}
	svc := &fakeEmailService{providers: []email.Provider{p1}}
	store := newFakeDeliveryStorage()
	e := newTestEmailSender(svc, accountMock, store)

	event := testEvent("event-scheduler")
	ev := testAccountEvent("user@example.com", "en")

	// First pass: temporary failure with NextRetryAt due.
	assert.Equal(t, decisionNack, e.deliver(context.Background(), event, ev))

	// Scheduler pass at a later now converges it.
	now := timeNowTest()
	deliveries, err := store.ListDueDeliveries(context.Background(), now, 10)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(deliveries))
	e.scheduleDelivery(context.Background(), deliveries[0], now)

	assert.Equal(t, domain.DeliveryStatusAccepted, existingDelivery(store, "event-scheduler").Status)
	assert.Equal(t, 2, p1.calls)
}

func timeNowTest() int64 {
	// Two minutes beyond the real first-pass time so the retry is due and the
	// 60s lease has expired.
	return time.Now().Unix() + 120
}

// Ensure the fake storage's lease behavior matches the SQL semantics when a
// second owner attempts concurrently.
func TestAcquireLeaseExclusive(t *testing.T) {
	t.Parallel()
	store := newFakeDeliveryStorage()
	d := domain.NewDelivery("event-lease", "org-1", "u@e.com", "en", 1000)
	assert.NoError(t, store.CreateDelivery(context.Background(), d))

	ok, err := store.AcquireLease(context.Background(), d.ID, "owner-a", leaseTTLSeconds, 1000)
	assert.NoError(t, err)
	assert.True(t, ok)

	ok, err = store.AcquireLease(context.Background(), d.ID, "owner-b", leaseTTLSeconds, 1001)
	assert.NoError(t, err)
	assert.False(t, ok, "second owner cannot acquire an active lease")

	// After the lease expires, another owner can take over.
	ok, err = store.AcquireLease(context.Background(), d.ID, "owner-b", leaseTTLSeconds, 1061)
	assert.NoError(t, err)
	assert.True(t, ok)
}

func TestScaledDelay(t *testing.T) {
	t.Parallel()
	assert.Equal(t, int64(60), scaledDelay(60, 1, 1800))
	assert.Equal(t, int64(120), scaledDelay(60, 2, 1800))
	assert.Equal(t, int64(1800), scaledDelay(60, 20, 1800))
	assert.Equal(t, int64(30), scaledDelay(30, 1, 300))
	assert.Equal(t, int64(300), scaledDelay(30, 10, 300))
}
