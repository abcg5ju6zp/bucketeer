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
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"go.uber.org/zap"
)

// SESService implements Provider using Amazon SES.
type SESService struct {
	config Config
	logger *zap.Logger
	client *sesv2.Client
}

// NewSESService creates a new SES email service.
func NewSESService(emailConfig Config, logger *zap.Logger) (*SESService, error) {
	// Create AWS config with explicit credentials
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(emailConfig.SES.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			emailConfig.SES.AccessKey,
			emailConfig.SES.SecretKey,
			"",
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := sesv2.NewFromConfig(cfg)

	return &SESService{
		config: emailConfig,
		logger: logger,
		client: client,
	}, nil
}

func (s *SESService) Name() string {
	return "ses"
}

func (s *SESService) Send(ctx context.Context, req SendRequest) SendResult {
	input := &sesv2.SendEmailInput{
		FromEmailAddress: aws.String(s.config.Sender.Email),
		Destination: &types.Destination{
			ToAddresses: []string{req.Snapshot.To},
		},
		Content: &types.EmailContent{
			Simple: &types.Message{
				Subject: &types.Content{
					Data:    aws.String(req.Snapshot.Subject),
					Charset: aws.String("UTF-8"),
				},
				Body: &types.Body{
					Html: &types.Content{
						Data:    aws.String(req.Snapshot.Body),
						Charset: aws.String("UTF-8"),
					},
				},
				Headers: []types.MessageHeader{
					{
						Name:  aws.String(HeaderDeliveryID),
						Value: aws.String(req.DeliveryID),
					},
				},
			},
		},
	}

	out, err := s.client.SendEmail(ctx, input)
	if err != nil {
		outcome := classifySESError(err)
		s.logger.Error("Failed to send welcome email via SES",
			zap.Error(err),
			zap.String("to", req.Snapshot.To),
		)
		return SendResult{
			Outcome: outcome,
			Receipt: Receipt{Raw: errorRaw(err, 511)},
			Err:     err,
		}
	}

	receipt := Receipt{StatusCode: http.StatusOK}
	if out.MessageId != nil {
		receipt.MessageID = *out.MessageId
	}
	s.logger.Info("Welcome email sent successfully",
		zap.String("to", req.Snapshot.To),
		zap.String("messageId", receipt.MessageID),
	)
	return SendResult{Outcome: OutcomeAccepted, Receipt: receipt}
}

// classifySESError maps an SES error to an outcome.
//
// HTTP 429 and 5xx responses are explicit temporary failures (backoff), every
// other received error is an explicit rejection (failover allowed), and errors
// with no usable HTTP response (timeouts, connection failures) are uncertain
// because SES may have accepted the message.
func classifySESError(err error) Outcome {
	var responseErr *smithyhttp.ResponseError
	if errors.As(err, &responseErr) {
		switch sc := responseErr.HTTPStatusCode(); {
		case sc == http.StatusTooManyRequests || sc >= 500:
			return OutcomeRetryable
		default:
			return OutcomeRejected
		}
	}
	return OutcomeUnknown
}
