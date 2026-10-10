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
	"errors"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"testing"

	"github.com/mailersend/mailersend-go"
	"github.com/sendgrid/rest"
	"github.com/stretchr/testify/assert"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"go.uber.org/zap"
)

func TestClassifySESError(t *testing.T) {
	t.Parallel()
	patterns := []struct {
		desc     string
		err      error
		expected Outcome
	}{
		{
			desc:     "plain connection failure is unknown",
			err:      errors.New("dial: connection refused"),
			expected: OutcomeUnknown,
		},
		{
			desc: "http 400 is an explicit rejection",
			err:  newSmithyError(400, errors.New("bad request")),
			expected: OutcomeRejected,
		},
		{
			desc: "http 403 is an explicit rejection",
			err:  newSmithyError(403, errors.New("forbidden")),
			expected: OutcomeRejected,
		},
		{
			desc: "http 429 is a temporary failure",
			err:  newSmithyError(429, errors.New("too many requests")),
			expected: OutcomeRetryable,
		},
		{
			desc: "http 500 is a temporary failure",
			err:  newSmithyError(500, errors.New("internal error")),
			expected: OutcomeRetryable,
		},
		{
			desc: "http 503 is a temporary failure",
			err:  newSmithyError(503, errors.New("unavailable")),
			expected: OutcomeRetryable,
		},
	}
	for _, p := range patterns {
		t.Run(p.desc, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, p.expected, classifySESError(p.err))
		})
	}
}

func newSmithyError(statusCode int, cause error) *smithyhttp.ResponseError {
	return &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{
			Response: &http.Response{StatusCode: statusCode, Header: http.Header{}},
		},
		Err: cause,
	}
}

func TestClassifySMTPError(t *testing.T) {
	t.Parallel()
	patterns := []struct {
		desc     string
		err      error
		expected Outcome
	}{
		{
			desc:     "network error is unknown",
			err:      errors.New("i/o timeout"),
			expected: OutcomeUnknown,
		},
		{
			desc:     "4xx response is a temporary failure",
			err:      &textproto.Error{Code: 451, Msg: "temporary failure"},
			expected: OutcomeRetryable,
		},
		{
			desc:     "5xx response is an explicit rejection",
			err:      &textproto.Error{Code: 550, Msg: "mailbox unavailable"},
			expected: OutcomeRejected,
		},
	}
	for _, p := range patterns {
		t.Run(p.desc, func(t *testing.T) {
			t.Parallel()
			outcome, _ := classifySMTPError(p.err)
			assert.Equal(t, p.expected, outcome)
		})
	}
}

func TestClassifySendGridResponse(t *testing.T) {
	t.Parallel()
	patterns := []struct {
		desc       string
		statusCode int
		expected   Outcome
	}{
		{"202 accepted", 202, OutcomeAccepted},
		{"400 rejected", 400, OutcomeRejected},
		{"403 rejected", 403, OutcomeRejected},
		{"429 temporary", 429, OutcomeRetryable},
		{"503 temporary", 503, OutcomeRetryable},
	}
	for _, p := range patterns {
		t.Run(p.desc, func(t *testing.T) {
			t.Parallel()
			response := &rest.Response{
				StatusCode: p.statusCode,
				Headers: map[string][]string{
					"X-Message-Id": {"sendgrid-message-1"},
				},
			}
			result := classifySendGridResponse(response, nil, zap.NewNop())
			assert.Equal(t, p.expected, result.Outcome)
			if result.Outcome == OutcomeAccepted {
				assert.Equal(t, "sendgrid-message-1", result.Receipt.MessageID)
			}
		})
	}
}

func TestClassifyMailerSendResponse(t *testing.T) {
	t.Parallel()
	patterns := []struct {
		desc       string
		statusCode int
		expected   Outcome
	}{
		{"202 accepted", 202, OutcomeAccepted},
		{"400 rejected", 400, OutcomeRejected},
		{"429 temporary", 429, OutcomeRetryable},
		{"503 temporary", 503, OutcomeRetryable},
	}
	for _, p := range patterns {
		t.Run(p.desc, func(t *testing.T) {
			t.Parallel()
			recorder := httptest.NewRecorder()
			recorder.Header().Set("x-message-id", "mailersend-message-1")
			recorder.WriteHeader(p.statusCode)
			response := &mailersend.Response{Response: recorder.Result()}
			result := classifyMailerSendResponse(response, nil, zap.NewNop())
			assert.Equal(t, p.expected, result.Outcome)
			if result.Outcome == OutcomeAccepted {
				assert.Equal(t, "mailersend-message-1", result.Receipt.MessageID)
			}
		})
	}
}
