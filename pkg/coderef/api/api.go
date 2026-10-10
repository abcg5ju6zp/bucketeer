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
	"context"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	accountclient "github.com/bucketeer-io/bucketeer/v2/pkg/account/client"
	"github.com/bucketeer-io/bucketeer/v2/pkg/api/api"
	"github.com/bucketeer-io/bucketeer/v2/pkg/coderef/storage"
	"github.com/bucketeer-io/bucketeer/v2/pkg/pubsub/publisher"
	"github.com/bucketeer-io/bucketeer/v2/pkg/role"
	"github.com/bucketeer-io/bucketeer/v2/pkg/storage/v2/database"
	accountproto "github.com/bucketeer-io/bucketeer/v2/proto/account"
	coderefproto "github.com/bucketeer-io/bucketeer/v2/proto/coderef"
	eventproto "github.com/bucketeer-io/bucketeer/v2/proto/event/domain"
)

// Recovery relay configuration.
const (
	recoveryInterval     = 30 * time.Second
	recoveryPendingLimit = 100
	recoveryPublishLimit = 10 * time.Second
)

type options struct {
	logger *zap.Logger
}

type Option func(*options)

func WithLogger(logger *zap.Logger) Option {
	return func(opts *options) {
		opts.logger = logger
	}
}

type CodeReferenceService struct {
	accountClient  accountclient.Client
	dbClient       database.Client
	codeRefStorage storage.CodeReferenceStorage
	publisher      publisher.Publisher
	opts           *options
	logger         *zap.Logger
}

func NewCodeReferenceService(
	ac accountclient.Client,
	dbClient database.Client,
	codeRefStorage storage.CodeReferenceStorage,
	p publisher.Publisher,
	opts ...Option,
) *CodeReferenceService {
	dopts := &options{
		logger: zap.NewNop(),
	}
	for _, opt := range opts {
		opt(dopts)
	}
	return &CodeReferenceService{
		accountClient:  ac,
		dbClient:       dbClient,
		codeRefStorage: codeRefStorage,
		publisher:      p,
		opts:           dopts,
		logger:         dopts.logger.Named("api"),
	}
}

func (s *CodeReferenceService) Register(server *grpc.Server) {
	coderefproto.RegisterCodeReferenceServiceServer(server, s)
}

// RunRecovery is the background recovery relay. It continues changes left in
// the PENDING phase by earlier requests, including after a process restart:
// the stored original event is republished (补发原事件) and the change is marked
// published. It stops when ctx is canceled.
func (s *CodeReferenceService) RunRecovery(ctx context.Context) {
	ticker := time.NewTicker(recoveryInterval)
	defer ticker.Stop()
	// Run one pass shortly after startup, then on every tick.
	s.recoverPendingChanges(ctx)
	for {
		select {
		case <-ticker.C:
			s.recoverPendingChanges(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// recoverPendingChanges republishes events for one batch of PENDING changes.
// Each change is processed independently; errors on one do not stop the
// others and are retried on the next pass.
func (s *CodeReferenceService) recoverPendingChanges(ctx context.Context) {
	changes, err := s.codeRefStorage.ListPendingChanges(ctx, recoveryPendingLimit)
	if err != nil {
		s.logger.Error("Failed to list pending changes", zap.Error(err))
		return
	}
	for _, ch := range changes {
		if ctx.Err() != nil {
			return
		}
		if len(ch.EventData) == 0 {
			// Real changes always store their event before commit, so this is
			// unrecoverable corruption; leave the row pending and log.
			s.logger.Error("Pending change without recoverable event",
				zap.String("changeId", ch.ID))
			continue
		}
		event := &eventproto.Event{}
		if err := proto.Unmarshal(ch.EventData, event); err != nil {
			s.logger.Error("Failed to unmarshal stored event",
				zap.String("changeId", ch.ID), zap.Error(err))
			continue
		}
		publishCtx, cancel := context.WithTimeout(ctx, recoveryPublishLimit)
		publishErr := s.publisher.Publish(publishCtx, event)
		cancel()
		if publishErr != nil {
			s.logger.Error("Failed to republish stored event",
				zap.String("changeId", ch.ID), zap.Error(publishErr))
			continue
		}
		if err := s.codeRefStorage.MarkChangePublished(ctx, ch.ID); err != nil {
			s.logger.Error("Failed to mark recovered change",
				zap.String("changeId", ch.ID), zap.Error(err))
			continue
		}
		s.logger.Info("Recovered pending change",
			zap.String("changeId", ch.ID),
			zap.Int64("sequence", ch.Seq),
			zap.Any("changeType", ch.ChangeType))
	}
}

func (s *CodeReferenceService) checkEnvironmentRole(
	ctx context.Context,
	requiredRole accountproto.AccountV2_Role_Environment,
	environmentID string,
) (*eventproto.Editor, error) {
	return role.CheckEnvironmentRoleWithLog(
		ctx,
		requiredRole,
		environmentID,
		func(email string) (*accountproto.AccountV2, error) {
			resp, err := s.accountClient.GetAccountV2ByEnvironmentID(ctx, &accountproto.GetAccountV2ByEnvironmentIDRequest{
				Email:         email,
				EnvironmentId: environmentID,
			})
			if err != nil {
				return nil, err
			}
			return resp.Account, nil
		},
		s.logger,
		statusUnauthenticated.Err(),
		statusPermissionDenied.Err(),
		func(err error) error { return api.NewGRPCStatus(err).Err() },
	)
}
