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

	"github.com/sendgrid/rest"
	"github.com/sendgrid/sendgrid-go"
	"github.com/sendgrid/sendgrid-go/helpers/mail"
	"go.uber.org/zap"
)

// SendGridService implements Provider using SendGrid.
type SendGridService struct {
	config Config
	logger *zap.Logger
}

// NewSendGridService creates a new SendGrid email service.
func NewSendGridService(config Config, logger *zap.Logger) *SendGridService {
	return &SendGridService{
		config: config,
		logger: logger,
	}
}

func (s *SendGridService) Name() string {
	return "sendgrid"
}

func (s *SendGridService) Send(ctx context.Context, req SendRequest) SendResult {
	from := mail.NewEmail(s.config.Sender.Name, s.config.Sender.Email)
	toEmail := mail.NewEmail("", req.Snapshot.To)
	message := mail.NewSingleEmail(from, req.Snapshot.Subject, toEmail, "", req.Snapshot.Body)
	// Carry the stable delivery identity as a message header.
	message.Headers = map[string]string{
		HeaderDeliveryID: req.DeliveryID,
	}

	client := sendgrid.NewSendClient(s.config.SendGrid.APIKey)
	response, err := client.SendWithContext(ctx, message)
	if response == nil {
		// No response at all: the result is uncertain.
		s.logger.Error("Failed to send welcome email via SendGrid",
			zap.Error(err),
			zap.String("to", req.Snapshot.To),
		)
		return SendResult{
			Outcome: OutcomeUnknown,
			Receipt: Receipt{Raw: errorRaw(err, 511)},
			Err:     err,
		}
	}
	return classifySendGridResponse(response, err, s.logger)
}

// classifySendGridResponse maps a SendGrid response to an outcome.
func classifySendGridResponse(
	response *rest.Response,
	cause error,
	logger *zap.Logger,
) SendResult {
	receipt := Receipt{
		StatusCode: response.StatusCode,
		MessageID:  http.Header(response.Headers).Get("X-Message-Id"),
	}
	switch sc := response.StatusCode; {
	case sc >= 200 && sc < 300:
		logger.Info("Welcome email sent successfully",
			zap.String("messageId", receipt.MessageID),
		)
		return SendResult{Outcome: OutcomeAccepted, Receipt: receipt}
	case sc == http.StatusTooManyRequests || sc >= 500:
		logger.Warn("Temporary failure from SendGrid",
			zap.Int("statusCode", sc),
			zap.Error(cause),
		)
		return SendResult{Outcome: OutcomeRetryable, Receipt: receipt, Err: cause}
	default:
		logger.Error("Explicit rejection from SendGrid",
			zap.Int("statusCode", sc),
			zap.Error(cause),
		)
		return SendResult{Outcome: OutcomeRejected, Receipt: receipt, Err: cause}
	}
}
