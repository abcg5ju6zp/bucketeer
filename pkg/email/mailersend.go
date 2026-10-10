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

package email

import (
	"context"
	"net/http"

	"github.com/mailersend/mailersend-go"
	"go.uber.org/zap"
)

// MailerSendService implements Provider using MailerSend.
type MailerSendService struct {
	config Config
	logger *zap.Logger
	client *mailersend.Mailersend
}

// NewMailerSendService creates a new MailerSend email service.
func NewMailerSendService(config Config, logger *zap.Logger) *MailerSendService {
	client := mailersend.NewMailersend(config.MailerSend.APIKey)

	return &MailerSendService{
		config: config,
		logger: logger,
		client: client,
	}
}

func (s *MailerSendService) Name() string {
	return "mailersend"
}

func (s *MailerSendService) Send(ctx context.Context, req SendRequest) SendResult {
	message := s.client.Email.NewMessage()

	// Set sender
	message.SetFrom(mailersend.From{
		Name:  s.config.Sender.Name,
		Email: s.config.Sender.Email,
	})

	// Set recipient
	message.SetRecipients([]mailersend.Recipient{
		{
			Email: req.Snapshot.To,
		},
	})

	// Set subject and HTML body
	message.SetSubject(req.Snapshot.Subject)
	message.SetHTML(req.Snapshot.Body)

	// Carry the stable delivery identity as a custom header.
	message.SetHeaders([]mailersend.Header{
		{
			Name:  HeaderDeliveryID,
			Value: req.DeliveryID,
		},
	})

	// Send the email
	response, err := s.client.Email.Send(ctx, message)
	if response == nil {
		// No response at all: the result is uncertain.
		s.logger.Error("Failed to send welcome email via MailerSend",
			zap.Error(err),
			zap.String("to", req.Snapshot.To),
		)
		return SendResult{
			Outcome: OutcomeUnknown,
			Receipt: Receipt{Raw: errorRaw(err, 511)},
			Err:     err,
		}
	}
	return classifyMailerSendResponse(response, err, s.logger)
}

// classifyMailerSendResponse maps a MailerSend response to an outcome.
func classifyMailerSendResponse(
	response *mailersend.Response,
	cause error,
	logger *zap.Logger,
) SendResult {
	receipt := Receipt{
		StatusCode: response.StatusCode,
		MessageID:  response.Header.Get("x-message-id"),
	}
	switch sc := response.StatusCode; {
	case sc >= 200 && sc < 300:
		logger.Info("Welcome email sent successfully",
			zap.String("messageId", receipt.MessageID),
		)
		return SendResult{Outcome: OutcomeAccepted, Receipt: receipt}
	case sc == http.StatusTooManyRequests || sc >= 500:
		logger.Warn("Temporary failure from MailerSend",
			zap.Int("statusCode", sc),
			zap.Error(cause),
		)
		return SendResult{Outcome: OutcomeRetryable, Receipt: receipt, Err: cause}
	default:
		logger.Error("Explicit rejection from MailerSend",
			zap.Int("statusCode", sc),
			zap.Error(cause),
		)
		return SendResult{Outcome: OutcomeRejected, Receipt: receipt, Err: cause}
	}
}
