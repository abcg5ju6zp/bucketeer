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
	"encoding/json"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	v2as "github.com/bucketeer-io/bucketeer/v2/pkg/account/storage/v2"
	"github.com/bucketeer-io/bucketeer/v2/pkg/email"
	"github.com/bucketeer-io/bucketeer/v2/pkg/email/domain"
	emailstorage "github.com/bucketeer-io/bucketeer/v2/pkg/email/storage/v2"
	"github.com/bucketeer-io/bucketeer/v2/pkg/pubsub/puller"
	"github.com/bucketeer-io/bucketeer/v2/pkg/pubsub/puller/codes"
	"github.com/bucketeer-io/bucketeer/v2/pkg/subscriber"
	"github.com/bucketeer-io/bucketeer/v2/pkg/uuid"
	domaineventproto "github.com/bucketeer-io/bucketeer/v2/proto/event/domain"
)

const (
	// leaseTTLSeconds bounds how long one processor may converge a delivery
	// before another processor can take over after a crash.
	leaseTTLSeconds = 60
	// providerSendTimeout bounds every provider interaction.
	providerSendTimeout = 30 * time.Second
	// renderRetryInterval is the retry interval after a repairable render
	// failure.
	renderRetryInterval = 5 * time.Minute
	// schedulerInterval is how often due deliveries are scanned.
	schedulerInterval = 10 * time.Second
	// schedulerBatchSize bounds one scheduler scan.
	schedulerBatchSize = 100
	// backoff settings for explicit temporary provider failures.
	backoffInitialSeconds = 60
	backoffMaxSeconds     = 30 * 60
	// confirmation settings for uncertain provider responses.
	confirmInitialSeconds = 30
	confirmMaxSeconds     = 5 * 60
)

// EmailSenderConfig is the processor configuration.
type EmailSenderConfig struct {
	// Add config fields if needed in the future.
}

type emailSender struct {
	emailService    email.Service
	accountStorage  v2as.AccountStorage
	deliveryStorage emailstorage.DeliveryStorage
	config          EmailSenderConfig
	logger          *zap.Logger

	// leaseOwner uniquely identifies this processor instance for delivery
	// leases.
	leaseOwner    string
	schedulerOnce sync.Once
}

// NewEmailSender creates the welcome-email processor.
func NewEmailSender(
	config interface{},
	emailService email.Service,
	accountStorage v2as.AccountStorage,
	deliveryStorage emailstorage.DeliveryStorage,
	logger *zap.Logger,
) subscriber.PubSubProcessor {
	var emailSenderConfig EmailSenderConfig
	if config != nil {
		jsonConfigMap, ok := config.(map[string]interface{})
		if ok {
			configBytes, err := json.Marshal(jsonConfigMap)
			if err != nil {
				logger.Warn("emailSender: failed to marshal config, using defaults", zap.Error(err))
			} else {
				if err := json.Unmarshal(configBytes, &emailSenderConfig); err != nil {
					logger.Warn("emailSender: failed to unmarshal config, using defaults", zap.Error(err))
				}
			}
		}
	}

	leaseOwner := "subscriber"
	if u, err := uuid.NewUUID(); err == nil {
		leaseOwner = u.String()
	}

	return &emailSender{
		emailService:    emailService,
		accountStorage:  accountStorage,
		deliveryStorage: deliveryStorage,
		config:          emailSenderConfig,
		logger:          logger,
		leaseOwner:      leaseOwner,
	}
}

func (e *emailSender) Process(ctx context.Context, msgChan <-chan *puller.Message) error {
	e.startScheduler(ctx)
	for {
		select {
		case msg, ok := <-msgChan:
			if !ok {
				e.logger.Error("emailSender: message channel closed")
				return nil
			}
			subscriberReceivedCounter.WithLabelValues(subscriberEmailSender).Inc()
			e.handleMessage(ctx, msg)
		case <-ctx.Done():
			e.logger.Debug("subscriber context done, stopped processing messages")
			return nil
		}
	}
}

// deliveryDecision tells whether the source message must be acknowledged or
// left pending for redelivery.
type deliveryDecision int

const (
	decisionAck deliveryDecision = iota
	decisionNack
)

func (e *emailSender) handleMessage(parentCtx context.Context, msg *puller.Message) {
	startTime := time.Now()

	// Check for message ID.
	if id := msg.Attributes["id"]; id == "" {
		msg.Ack()
		subscriberHandledCounter.WithLabelValues(subscriberEmailSender, codes.MissingID.String()).Inc()
		return
	}

	// Unmarshal domain event.
	domainEvent, err := e.unmarshalMessage(msg)
	if err != nil {
		e.logger.Error("Failed to unmarshal message",
			zap.Error(err),
			zap.String("msgID", msg.ID),
		)
		msg.Ack()
		subscriberHandledCounter.WithLabelValues(subscriberEmailSender, codes.BadMessage.String()).Inc()
		return
	}

	// Filter: only handle ACCOUNT_V2_CREATED events.
	if domainEvent.EntityType != domaineventproto.Event_ACCOUNT ||
		domainEvent.Type != domaineventproto.Event_ACCOUNT_V2_CREATED {
		msg.Ack()
		subscriberHandledCounter.WithLabelValues(subscriberEmailSender, codes.OK.String()).Inc()
		return
	}

	// Extract AccountV2CreatedEvent.
	var accountCreatedEvent domaineventproto.AccountV2CreatedEvent
	if err := domainEvent.Data.UnmarshalTo(&accountCreatedEvent); err != nil {
		e.logger.Error("Failed to unmarshal AccountV2CreatedEvent",
			zap.String("event id", domainEvent.Id),
			zap.Error(err),
		)
		msg.Ack()
		subscriberHandledCounter.WithLabelValues(subscriberEmailSender, codes.BadMessage.String()).Inc()
		return
	}

	// When the email feature is disabled, keep the original no-op behavior:
	// no provider call and no delivery record.
	if e.emailService.Disabled() {
		e.logger.Info("Email feature disabled, welcome email not sent",
			zap.String("event id", domainEvent.Id),
		)
		msg.Ack()
		subscriberHandledCounter.WithLabelValues(subscriberEmailSender, codes.OK.String()).Inc()
		return
	}

	ctx, cancel := context.WithTimeout(parentCtx, providerSendTimeout)
	defer cancel()

	decision := e.deliver(ctx, domainEvent, &accountCreatedEvent)
	switch decision {
	case decisionAck:
		msg.Ack()
		subscriberHandledCounter.WithLabelValues(subscriberEmailSender, codes.OK.String()).Inc()
		subscriberHandledHistogram.WithLabelValues(subscriberEmailSender, codes.OK.String()).
			Observe(time.Since(startTime).Seconds())
	default:
		msg.Nack()
		subscriberHandledCounter.WithLabelValues(subscriberEmailSender, codes.RepeatableError.String()).Inc()
		subscriberHandledHistogram.WithLabelValues(subscriberEmailSender, codes.RepeatableError.String()).
			Observe(time.Since(startTime).Seconds())
	}
}

// deliver converges the delivery derived from one account creation event. The
// same event from any transport always maps to the same delivery identity.
func (e *emailSender) deliver(
	ctx context.Context,
	event *domaineventproto.Event,
	ev *domaineventproto.AccountV2CreatedEvent,
) deliveryDecision {
	now := time.Now().Unix()
	deliveryID := domain.DeriveDeliveryID(event.Id)

	d, err := e.deliveryStorage.GetDelivery(ctx, deliveryID)
	if errors.Is(err, emailstorage.ErrDeliveryNotFound) {
		d = domain.NewDelivery(event.Id, ev.OrganizationId, ev.Email, ev.Language, now)
		if cerr := e.deliveryStorage.CreateDelivery(ctx, d); cerr != nil {
			if !errors.Is(cerr, emailstorage.ErrDeliveryAlreadyExists) {
				// The delivery cannot be durably started; leave the message
				// pending so it is redelivered.
				e.logger.Error("Failed to create email delivery",
					zap.String("deliveryId", deliveryID),
					zap.Error(cerr),
				)
				return decisionNack
			}
			d, err = e.deliveryStorage.GetDelivery(ctx, deliveryID)
			if err != nil {
				return decisionNack
			}
		}
	} else if err != nil {
		e.logger.Error("Failed to get email delivery",
			zap.String("deliveryId", deliveryID),
			zap.Error(err),
		)
		return decisionNack
	}

	if d.IsTerminal() {
		// Already accepted, rejected or canceled: the logical email exists only
		// once; nothing more to do.
		return decisionAck
	}

	// Acquire an exclusive lease before any provider communication.
	acquired, err := e.deliveryStorage.AcquireLease(ctx, d.ID, e.leaseOwner, leaseTTLSeconds, now)
	if err != nil {
		e.logger.Error("Failed to acquire delivery lease",
			zap.String("deliveryId", d.ID),
			zap.Error(err),
		)
		return decisionNack
	}
	if !acquired {
		// Another processor is converging the same logical email. Leave this
		// copy pending; the owner records the receipt before acknowledging
		// its own copy.
		e.logger.Debug("Delivery already being converged by another processor",
			zap.String("deliveryId", d.ID),
		)
		return decisionNack
	}
	defer e.releaseLease(d.ID)

	// Work on the latest state after acquiring the lease.
	d, err = e.deliveryStorage.GetDelivery(ctx, deliveryID)
	if err != nil {
		return decisionNack
	}
	if d.IsTerminal() {
		return decisionAck
	}

	return e.tick(ctx, d, ev, now)
}

// tick performs one convergence step: pre-send cancellation checks, snapshot
// freezing, provider selection and one provider interaction.
func (e *emailSender) tick(
	ctx context.Context,
	d *domain.Delivery,
	ev *domaineventproto.AccountV2CreatedEvent,
	now int64,
) deliveryDecision {
	if d.IsTerminal() {
		return decisionAck
	}

	// Before the first provider attempt: cancel if the account no longer
	// exists at the original address; otherwise freeze the rendered snapshot.
	if !d.HasStarted() {
		if _, err := e.accountStorage.GetAccountV2(ctx, ev.Email, d.OrganizationID); err != nil {
			if errors.Is(err, v2as.ErrAccountNotFound) {
				if cerr := d.Cancel(domain.CancelReasonAccountNotFound, now); cerr != nil {
					return decisionNack
				}
				if uerr := e.deliveryStorage.UpdateDelivery(ctx, d); uerr != nil {
					return decisionNack
				}
				e.logger.Info("Canceled welcome email delivery before start",
					zap.String("deliveryId", d.ID),
					zap.String("reason", d.CancelReason),
				)
				return decisionAck
			}
			return decisionNack
		}

		if d.Snapshot == nil {
			rendered, rerr := e.emailService.RenderWelcome(ev.Language, ev.Email)
			if rerr != nil {
				// Repairable state: the template can be fixed and the delivery
				// retried (by the scheduler or an event replay).
				retryAt := now + int64(renderRetryInterval/time.Second)
				if merr := d.MarkRenderFailed(
					email.ErrorCodeRenderFailed, rerr.Error(), retryAt, now,
				); merr != nil {
					return decisionNack
				}
				if uerr := e.deliveryStorage.UpdateDelivery(ctx, d); uerr != nil {
					return decisionNack
				}
				e.logger.Error("Template rendering failed, delivery kept as repairable",
					zap.String("deliveryId", d.ID),
					zap.Error(rerr),
				)
				return decisionNack
			}
			snapshot := &domain.Snapshot{
				To:                ev.Email,
				RequestedLanguage: ev.Language,
				Language:          rendered.Language,
				TemplateVersion:   rendered.TemplateVersion,
				Subject:           rendered.Subject,
				Body:              rendered.Body,
			}
			if ferr := d.Freeze(snapshot, now); ferr != nil {
				return decisionNack
			}
			if uerr := e.deliveryStorage.UpdateDelivery(ctx, d); uerr != nil {
				return decisionNack
			}
		}
	}

	provider, err := e.selectProvider(d)
	if err != nil {
		// Every provider in the chain explicitly rejected the message.
		message := d.LastErrorMessage
		if message == "" {
			message = "all providers rejected the message"
		}
		if terr := d.MarkTerminalRejected(message, now); terr != nil {
			return decisionNack
		}
		if uerr := e.deliveryStorage.UpdateDelivery(ctx, d); uerr != nil {
			return decisionNack
		}
		return decisionAck
	}

	// Record the attempt before contacting the provider.
	attempt, err := e.openAttempt(ctx, d, provider, now)
	if err != nil {
		return decisionNack
	}

	// For an uncertain outcome, first resolve the original delivery identity
	// via a receipt query if the provider supports it; otherwise the retry
	// below uses the same identity.
	if d.Status == domain.DeliveryStatusPendingConfirmation {
		if querier, ok := provider.(email.ReceiptQuerier); ok {
			outcome, receipt, qerr := querier.QueryReceipt(ctx, d.ID)
			if qerr == nil {
				return e.applyProviderOutcome(ctx, d, attempt, outcome, receipt, nil, now)
			}
			e.logger.Debug("Receipt query failed, retrying with the same identity",
				zap.String("deliveryId", d.ID),
				zap.Error(qerr),
			)
		}
	}

	result := provider.Send(ctx, email.SendRequest{
		DeliveryID: d.ID,
		Snapshot:   d.Snapshot,
	})
	return e.applyProviderOutcome(ctx, d, attempt, result.Outcome, result.Receipt, result.Err, now)
}

// selectProvider returns the provider to use for the next interaction.
func (e *emailSender) selectProvider(d *domain.Delivery) (email.Provider, error) {
	chain := e.emailService.Chain()
	if len(chain) == 0 {
		return nil, errors.New("no provider configured")
	}
	if d.LastProvider == "" {
		return chain[0], nil
	}
	idx := indexProvider(chain, d.LastProvider)
	switch d.LastOutcome {
	case domain.AttemptResultRejected:
		// Explicit rejection: the only case that allows switching to the next
		// backup provider. If the rejected provider left the chain, restart
		// from the primary.
		if idx < 0 {
			return chain[0], nil
		}
		if idx+1 < len(chain) {
			return chain[idx+1], nil
		}
		return nil, errors.New("no backup provider left")
	default:
		// Temporary failure (backoff) or uncertain response: keep the same
		// provider and the original delivery identity. No failover.
		if idx < 0 {
			return nil, errors.New("provider no longer available in the chain")
		}
		return chain[idx], nil
	}
}

// openAttempt finds an existing in-progress attempt for the provider (left by
// a crash or before an uncertain result was recorded), or records a new one
// before the provider is contacted.
func (e *emailSender) openAttempt(
	ctx context.Context,
	d *domain.Delivery,
	provider email.Provider,
	now int64,
) (*domain.Attempt, error) {
	attempts, err := e.deliveryStorage.ListAttempts(ctx, d.ID)
	if err != nil {
		return nil, err
	}
	for i := len(attempts) - 1; i >= 0; i-- {
		a := attempts[i]
		if a.Provider == provider.Name() && a.Result == domain.AttemptResultInProgress {
			return a, nil
		}
	}

	seq := d.AttemptCount + 1
	attempt := domain.NewAttempt(d.ID, seq, provider.Name(), now)
	if err := e.deliveryStorage.CreateAttempt(ctx, attempt); err != nil {
		if !errors.Is(err, emailstorage.ErrAttemptAlreadyExists) {
			return nil, err
		}
		// The row already exists: re-list and return it.
		attempts, err = e.deliveryStorage.ListAttempts(ctx, d.ID)
		if err != nil {
			return nil, err
		}
		for i := len(attempts) - 1; i >= 0; i-- {
			if attempts[i].ID == attempt.ID {
				return attempts[i], nil
			}
		}
		return nil, errors.New("attempt exists but could not be loaded")
	}

	d.AttemptCount = seq
	if err := e.deliveryStorage.UpdateDelivery(ctx, d); err != nil {
		return nil, err
	}
	return attempt, nil
}

// markReceiptNotRecorded persists the pending-confirmation state when the
// attempt result (typically the provider receipt) could not be saved. The next
// pass resolves the same delivery identity instead of failing over.
func (e *emailSender) markReceiptNotRecorded(
	ctx context.Context,
	d *domain.Delivery,
	attempt *domain.Attempt,
	now int64,
) {
	retryAt := now + confirmInitialSeconds
	if merr := d.MarkPendingConfirmation(
		attempt.Provider,
		email.ErrorCodeUnknownResponse,
		"receipt not recorded",
		retryAt,
		now,
	); merr != nil {
		return
	}
	if uerr := e.deliveryStorage.UpdateDelivery(ctx, d); uerr != nil {
		e.logger.Error("Failed to persist pending-confirmation after receipt save failure",
			zap.String("deliveryId", d.ID),
			zap.Error(uerr),
		)
	}
}

// applyProviderOutcome records the attempt result and the provider receipt and
// updates the delivery state. Every write completes before an Ack decision.
func (e *emailSender) applyProviderOutcome(
	ctx context.Context,
	d *domain.Delivery,
	attempt *domain.Attempt,
	outcome email.Outcome,
	receipt email.Receipt,
	cause error,
	now int64,
) deliveryDecision {
	switch outcome {
	case email.OutcomeAccepted:
		attempt.Accept(receipt.MessageID, receipt.StatusCode, now)
		if err := e.deliveryStorage.UpdateAttempt(ctx, attempt); err != nil {
			// The provider accepted but the receipt could not be saved. Keep
			// the delivery pending confirmation; the next pass resolves the
			// same identity instead of failing over.
			e.logger.Error("Failed to record provider receipt",
				zap.String("deliveryId", d.ID),
				zap.String("provider", attempt.Provider),
				zap.Error(err),
			)
			e.markReceiptNotRecorded(ctx, d, attempt, now)
			return decisionNack
		}
		if err := d.MarkAccepted(attempt.Provider, receipt.MessageID, now); err != nil {
			return decisionNack
		}
		if err := e.deliveryStorage.UpdateDelivery(ctx, d); err != nil {
			return decisionNack
		}
		e.logger.Info("Successfully sent welcome email",
			zap.String("deliveryId", d.ID),
			zap.String("provider", attempt.Provider),
			zap.String("receiptId", receipt.MessageID),
		)
		return decisionAck

	case email.OutcomeRejected:
		message := emailErrorString(cause)
		attempt.Reject(email.ErrorCodeRejected, message, receipt.StatusCode, now)
		if err := e.deliveryStorage.UpdateAttempt(ctx, attempt); err != nil {
			return decisionNack
		}
		if err := d.MarkRejected(attempt.Provider, email.ErrorCodeRejected, message, now); err != nil {
			return decisionNack
		}
		if err := e.deliveryStorage.UpdateDelivery(ctx, d); err != nil {
			return decisionNack
		}
		// If no backup provider is left, the rejection is terminal and the
		// message can be acknowledged.
		if _, err := e.selectProvider(d); err != nil {
			if terr := d.MarkTerminalRejected(message, now); terr != nil {
				return decisionNack
			}
			if uerr := e.deliveryStorage.UpdateDelivery(ctx, d); uerr != nil {
				return decisionNack
			}
			e.logger.Error("Welcome email rejected by every provider",
				zap.String("deliveryId", d.ID),
			)
			return decisionAck
		}
		e.logger.Warn("Provider rejected welcome email, switching to backup",
			zap.String("deliveryId", d.ID),
			zap.String("provider", attempt.Provider),
		)
		return decisionNack

	case email.OutcomeRetryable:
		message := emailErrorString(cause)
		attempt.MarkRetryable(email.ErrorCodeTemporaryFailure, message, receipt.StatusCode, now)
		if err := e.deliveryStorage.UpdateAttempt(ctx, attempt); err != nil {
			return decisionNack
		}
		retryAt := now + scaledDelay(backoffInitialSeconds, attempt.Seq, backoffMaxSeconds)
		if err := d.MarkBackoff(
			attempt.Provider, email.ErrorCodeTemporaryFailure, message, retryAt, now,
		); err != nil {
			return decisionNack
		}
		if err := e.deliveryStorage.UpdateDelivery(ctx, d); err != nil {
			return decisionNack
		}
		e.logger.Warn("Provider temporary failure, delivery scheduled with backoff",
			zap.String("deliveryId", d.ID),
			zap.String("provider", attempt.Provider),
			zap.Int64("nextRetryAt", retryAt),
		)
		return decisionNack

	default: // email.OutcomeUnknown
		message := emailErrorString(cause)
		attempt.MarkUnknown(email.ErrorCodeUnknownResponse, message, receipt.StatusCode, now)
		if err := e.deliveryStorage.UpdateAttempt(ctx, attempt); err != nil {
			// The result could not be saved: keep the delivery pending
			// confirmation so the original identity is resolved next.
			e.markReceiptNotRecorded(ctx, d, attempt, now)
			return decisionNack
		}
		retryAt := now + scaledDelay(confirmInitialSeconds, attempt.Seq, confirmMaxSeconds)
		if err := d.MarkPendingConfirmation(
			attempt.Provider, email.ErrorCodeUnknownResponse, message, retryAt, now,
		); err != nil {
			return decisionNack
		}
		if err := e.deliveryStorage.UpdateDelivery(ctx, d); err != nil {
			return decisionNack
		}
		e.logger.Warn("Provider response uncertain, delivery pending confirmation",
			zap.String("deliveryId", d.ID),
			zap.String("provider", attempt.Provider),
			zap.Int64("nextRetryAt", retryAt),
		)
		return decisionNack
	}
}

// startScheduler launches the internal scheduler that converges due deliveries
// independently of message redelivery.
func (e *emailSender) startScheduler(ctx context.Context) {
	e.schedulerOnce.Do(func() {
		go e.runScheduler(ctx)
	})
}

func (e *emailSender) runScheduler(ctx context.Context) {
	ticker := time.NewTicker(schedulerInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			e.logger.Debug("email scheduler stopped")
			return
		case <-ticker.C:
			e.scheduleDueDeliveries(ctx)
		}
	}
}

func (e *emailSender) scheduleDueDeliveries(ctx context.Context) {
	now := time.Now().Unix()
	deliveries, err := e.deliveryStorage.ListDueDeliveries(ctx, now, schedulerBatchSize)
	if err != nil {
		e.logger.Error("Scheduler: failed to list due deliveries", zap.Error(err))
		return
	}
	for _, d := range deliveries {
		e.scheduleDelivery(ctx, d, now)
	}
}

// scheduleDelivery converges one due delivery found by the scheduler.
func (e *emailSender) scheduleDelivery(
	ctx context.Context,
	d *domain.Delivery,
	now int64,
) {
	acquired, err := e.deliveryStorage.AcquireLease(ctx, d.ID, e.leaseOwner, leaseTTLSeconds, now)
	if err != nil {
		e.logger.Error("Scheduler: failed to acquire lease",
			zap.String("deliveryId", d.ID),
			zap.Error(err),
		)
		return
	}
	if !acquired {
		return
	}
	defer e.releaseLease(d.ID)

	d, err = e.deliveryStorage.GetDelivery(ctx, d.ID)
	if err != nil {
		return
	}
	if d.IsTerminal() || d.NextRetryAt > now {
		return
	}

	sctx, cancel := context.WithTimeout(ctx, providerSendTimeout)
	defer cancel()
	ev := &domaineventproto.AccountV2CreatedEvent{
		Email:          d.EventEmail,
		Language:       d.EventLanguage,
		OrganizationId: d.OrganizationID,
	}
	// There is no transport message to ack; the scheduler convergence is
	// fully captured by the delivery/attempt rows.
	_ = e.tick(sctx, d, ev, now)
}

// releaseLease best-effort releases a delivery lease owned by this processor.
func (e *emailSender) releaseLease(deliveryID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.deliveryStorage.ReleaseLease(ctx, deliveryID, e.leaseOwner); err != nil {
		e.logger.Debug("Failed to release delivery lease",
			zap.String("deliveryId", deliveryID),
			zap.Error(err),
		)
	}
}

func (e *emailSender) unmarshalMessage(msg *puller.Message) (*domaineventproto.Event, error) {
	event := &domaineventproto.Event{}
	if err := proto.Unmarshal(msg.Data, event); err != nil {
		e.logger.Error("Failed to unmarshal message", zap.Error(err))
		return nil, err
	}
	return event, nil
}

// indexProvider returns the index of the provider with the given name.
func indexProvider(chain []email.Provider, name string) int {
	for i, p := range chain {
		if p.Name() == name {
			return i
		}
	}
	return -1
}

// scaledDelay returns initial*2^(seq-1), capped at maxDelay.
func scaledDelay(initial int64, seq int, maxDelay int64) int64 {
	delay := initial
	for i := 1; i < seq; i++ {
		delay *= 2
		if delay >= maxDelay {
			return maxDelay
		}
	}
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}

// emailErrorString returns the error text for storage, truncated to the
// delivery column limit.
func emailErrorString(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	if len(s) > 511 {
		return s[:511]
	}
	return s
}
