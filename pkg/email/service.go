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
	"fmt"

	"go.uber.org/zap"
)

// RenderedEmail is the result of rendering the welcome template.
type RenderedEmail struct {
	// Subject is the rendered subject.
	Subject string
	// Body is the rendered HTML body.
	Body string
	// Language is the resolved language used (after the unknown-language
	// fallback).
	Language string
	// TemplateVersion is the template version used.
	TemplateVersion string
}

// Service provides the ordered provider chain and template rendering used to
// deliver welcome emails.
type Service interface {
	// Disabled reports whether the email feature is turned off. When disabled
	// no message is sent and no delivery is recorded, preserving the original
	// no-op behavior.
	Disabled() bool
	// RenderWelcome renders the welcome email for the requested language.
	// Unknown languages fall back to English.
	RenderWelcome(language, to string) (RenderedEmail, error)
	// Chain returns the ordered provider chain: the primary provider first,
	// then the configured backups. Empty when disabled.
	Chain() []Provider
	// Provider returns the provider with the given name.
	Provider(name string) (Provider, bool)
}

// newProvider constructs a provider by name using its existing configuration.
func newProvider(name string, config Config, logger *zap.Logger) (Provider, error) {
	switch name {
	case "smtp":
		return NewSMTPService(config, logger), nil
	case "sendgrid":
		return NewSendGridService(config, logger), nil
	case "ses":
		return NewSESService(config, logger)
	case "mailersend":
		return NewMailerSendService(config, logger), nil
	default:
		return nil, fmt.Errorf("unsupported email provider: %s", name)
	}
}

// service is the default Service implementation.
type service struct {
	config    Config
	logger    *zap.Logger
	renderer  *TemplateRenderer
	providers map[string]Provider
	chain     []Provider
}

// NewService creates an email service based on configuration.
func NewService(config Config, logger *zap.Logger) (Service, error) {
	if !config.Enabled {
		return NewNoOpService(logger), nil
	}

	// Build the primary provider first. A construction failure preserves the
	// original behavior of returning an error.
	primary, err := newProvider(config.Provider, config, logger)
	if err != nil {
		return nil, err
	}

	svc := &service{
		config:    config,
		logger:    logger,
		renderer:  NewTemplateRenderer(config),
		providers: map[string]Provider{},
	}
	svc.providers[primary.Name()] = primary
	svc.chain = append(svc.chain, primary)

	// Build the ordered backup chain when the failover policy is enabled.
	// Backup construction failures are logged and skipped so a misconfigured
	// backup never blocks the primary.
	if config.Failover.Enabled {
		for _, name := range config.Failover.Providers {
			if _, exists := svc.providers[name]; exists {
				continue
			}
			backup, berr := newProvider(name, config, logger)
			if berr != nil {
				logger.Error("Failed to construct backup email provider, skipping",
					zap.String("provider", name),
					zap.Error(berr),
				)
				continue
			}
			svc.providers[backup.Name()] = backup
			svc.chain = append(svc.chain, backup)
		}
	}

	return svc, nil
}

func (s *service) Disabled() bool {
	return false
}

func (s *service) RenderWelcome(language, to string) (RenderedEmail, error) {
	subject, body := s.renderer.RenderWelcomeEmail(language, to)
	return RenderedEmail{
		Subject:         subject,
		Body:            body,
		Language:        s.renderer.ResolveLanguage(language),
		TemplateVersion: TemplateVersion,
	}, nil
}

func (s *service) Chain() []Provider {
	return s.chain
}

func (s *service) Provider(name string) (Provider, bool) {
	p, ok := s.providers[name]
	return p, ok
}

// NoOpService is a no-operation email service for when email is disabled.
type NoOpService struct {
	logger *zap.Logger
}

// NewNoOpService creates a no-operation email service.
func NewNoOpService(logger *zap.Logger) *NoOpService {
	return &NoOpService{logger: logger}
}

func (s *NoOpService) Disabled() bool {
	return true
}

func (s *NoOpService) RenderWelcome(language, to string) (RenderedEmail, error) {
	return RenderedEmail{}, nil
}

func (s *NoOpService) Chain() []Provider {
	return nil
}

func (s *NoOpService) Provider(name string) (Provider, bool) {
	return nil, false
}
