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
	"errors"
	"fmt"
	"net/smtp"
	"net/textproto"
	"strings"

	"go.uber.org/zap"
)

// SMTPService implements Provider using SMTP.
type SMTPService struct {
	config Config
	logger *zap.Logger
}

// NewSMTPService creates a new SMTP email service.
func NewSMTPService(config Config, logger *zap.Logger) *SMTPService {
	return &SMTPService{
		config: config,
		logger: logger,
	}
}

func (s *SMTPService) Name() string {
	return "smtp"
}

func (s *SMTPService) Send(ctx context.Context, req SendRequest) SendResult {
	message := buildSMTPMessage(s.config.Sender.Email, req)
	addr := fmt.Sprintf("%s:%d", s.config.SMTP.Host, s.config.SMTP.Port)
	auth := smtp.PlainAuth("", s.config.SMTP.Username, s.config.SMTP.Password, s.config.SMTP.Host)
	err := smtp.SendMail(addr, auth, s.config.Sender.Email, []string{req.Snapshot.To}, message)
	if err != nil {
		outcome, code := classifySMTPError(err)
		s.logger.Error("Failed to send welcome email via SMTP",
			zap.Error(err),
			zap.String("to", req.Snapshot.To),
			zap.String("code", code),
		)
		return SendResult{
			Outcome: outcome,
			Receipt: Receipt{Raw: errorRaw(err, 511)},
			Err:     err,
		}
	}
	s.logger.Info("Welcome email sent successfully",
		zap.String("to", req.Snapshot.To),
	)
	return SendResult{Outcome: OutcomeAccepted}
}

// buildSMTPMessage builds the RFC 822 message including the stable delivery
// identity as a custom header.
func buildSMTPMessage(from string, req SendRequest) []byte {
	lines := []string{
		"To: " + req.Snapshot.To,
		"From: " + from,
		"Subject: " + req.Snapshot.Subject,
		HeaderDeliveryID + ": " + req.DeliveryID,
		"MIME-Version: 1.0",
		"Content-Type: text/html; charset=UTF-8",
		"",
		req.Snapshot.Body,
		"",
	}
	return []byte(strings.Join(lines, "\r\n"))
}

// classifySMTPError maps an SMTP error to an outcome.
//
// 4xx response codes are explicit temporary failures (backoff), 5xx codes are
// explicit permanent rejections (failover allowed), and anything else
// (connection errors, failures after DATA was accepted, etc.) is uncertain
// because the message may already have been queued.
func classifySMTPError(err error) (Outcome, string) {
	// SMTP server responses are returned as *textproto.Error.
	var responseErr *textproto.Error
	if errors.As(err, &responseErr) {
		switch code := responseErr.Code; {
		case code >= 400 && code < 500:
			return OutcomeRetryable, ErrorCodeTemporaryFailure
		case code >= 500:
			return OutcomeRejected, ErrorCodeRejected
		}
	}
	return OutcomeUnknown, ErrorCodeUnknownResponse
}
