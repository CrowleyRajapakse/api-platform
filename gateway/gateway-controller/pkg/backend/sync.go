/*
 * Copyright (c) 2026, WSO2 LLC. (https://www.wso2.com).
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package backend

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/wso2/api-platform/common/eventhub"
	"github.com/wso2/api-platform/gateway/gateway-controller/pkg/models"
	"github.com/wso2/api-platform/gateway/gateway-controller/pkg/storage"
	"github.com/wso2/api-platform/gateway/gateway-controller/pkg/templateengine"
	"github.com/wso2/api-platform/gateway/gateway-controller/pkg/upstreamguard"
)

// SnapshotUpdater rebuilds and pushes the Envoy xDS snapshot.
type SnapshotUpdater interface {
	UpdateSnapshot(ctx context.Context, correlationID string) error
}

// Syncer applies Backend changes on this replica: from the database into memory, then into the
// router's clusters and the policy engine's records, in the order that never routes a request to
// a cluster Envoy lacks.
type Syncer struct {
	// mu serialises each read from the database with its apply, for local changes and replica
	// events alike, so an older read can never overwrite a newer one in memory.
	mu sync.Mutex

	db        storage.BackendStorage
	store     *storage.BackendStore
	snapshot  SnapshotUpdater
	publisher *Publisher
	logger    *slog.Logger
}

// NewSyncer creates a Syncer. snapshot and publisher may be nil in tests.
func NewSyncer(db storage.BackendStorage, store *storage.BackendStore, snapshot SnapshotUpdater, publisher *Publisher, logger *slog.Logger) *Syncer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Syncer{db: db, store: store, snapshot: snapshot, publisher: publisher, logger: logger}
}

// LoadAll loads every Backend from the database into memory. Called at startup, before API
// hydration and the first snapshot, so the first CDS and lazy-resource pushes already hold every
// Backend.
func (s *Syncer) LoadAll() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	backends, err := s.db.GetAllBackends()
	if err != nil {
		return fmt.Errorf("loading backends: %w", err)
	}
	permitted := make([]*models.StoredBackend, 0, len(backends))
	for _, backend := range backends {
		if s.permitted(backend) {
			permitted = append(permitted, backend)
		}
	}
	s.store.ReplaceAll(permitted)
	s.logger.Info("Loaded global backends", slog.Int("count", len(permitted)))
	return nil
}

// permitted reports whether the gateway's network access control allows every endpoint of the
// Backend. A refused Backend stays in the database but gets no cluster and no record, so the
// gateway never dials it; the operator sees why in the log.
func (s *Syncer) permitted(backend *models.StoredBackend) bool {
	err := templateengine.RunPostRenderCheck(string(models.KindBackend), &backend.Configuration)
	if err == nil {
		return true
	}
	if upstreamguard.IsRefused(err) {
		s.logger.Error("Global backend refused by network access control; it is not served",
			slog.String("name", backend.GetHandle()),
			slog.String("field", upstreamguard.RefusedFieldPath(err)),
			slog.String("reason", upstreamguard.RefusalReason(err)))
	} else {
		s.logger.Error("Failed to check global backend against network access control; it is not served",
			slog.String("name", backend.GetHandle()), slog.Any("error", err))
	}
	return false
}

// HandleBackendEvent applies one replica-sync event. It is idempotent: the database is read
// again, so a duplicate or reordered event converges on the stored state.
func (s *Syncer) HandleBackendEvent(event eventhub.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch event.Action {
	case "CREATE", "UPDATE":
		s.applyUpsert(event.EntityID, event.EventID)
	case "DELETE":
		s.applyDelete(event.EntityID, event.EventID)
	default:
		s.logger.Warn("Unknown backend event action",
			slog.String("action", event.Action), slog.String("entity_id", event.EntityID))
	}
}

// applyUpsert stores the Backend, publishes its cluster, and only then its record: a policy
// that can see the record can always reach the cluster. If the cluster update fails, the record
// is not published; the next successful sync publishes it together with its cluster.
func (s *Syncer) applyUpsert(uuid, correlationID string) {
	backend, err := s.db.GetBackend(uuid)
	if err != nil {
		if storage.IsNotFoundError(err) {
			// Deleted after this event was published; the delete event converges the state.
			s.applyDelete(uuid, correlationID)
			return
		}
		s.logger.Error("Failed to load backend for sync", slog.String("uuid", uuid), slog.Any("error", err))
		return
	}
	if !s.permitted(backend) {
		// A Backend the access control refuses is withdrawn if an earlier version was served.
		s.applyDelete(uuid, correlationID)
		return
	}
	if err := s.store.Upsert(backend); err != nil {
		s.logger.Error("Failed to store backend in memory", slog.String("uuid", uuid), slog.Any("error", err))
		return
	}
	if err := s.updateSnapshot(correlationID); err != nil {
		s.logger.Error("Failed to update xDS snapshot for backend change; its record is not published until its cluster is",
			slog.String("name", backend.GetHandle()), slog.Any("error", err))
		return
	}
	s.publish(correlationID)
	s.logger.Info("Applied global backend", slog.String("name", backend.GetHandle()), slog.String("event_id", correlationID))
}

// applyDelete withdraws the Backend's record first and its cluster second, the reverse of
// applyUpsert. The memory entry is removed by UUID, so a delete for an old UUID never removes a
// Backend created again under the same name. If withdrawing the record fails, the cluster stays
// until it succeeds, and a repeated delete retries the withdrawal.
func (s *Syncer) applyDelete(uuid, correlationID string) {
	backend := s.store.DeleteByUUID(uuid)
	if backend == nil {
		// Already gone on this replica. Finish an earlier withdrawal that failed.
		if s.publisher.Stale() {
			if s.publish(correlationID) {
				s.logSnapshotError(s.updateSnapshot(correlationID))
			}
		}
		return
	}
	if !s.publish(correlationID) {
		return
	}
	s.logSnapshotError(s.updateSnapshot(correlationID))
	s.logger.Info("Removed global backend", slog.String("name", backend.GetHandle()), slog.String("event_id", correlationID))
}

func (s *Syncer) updateSnapshot(correlationID string) error {
	if s.snapshot == nil {
		return nil
	}
	return s.snapshot.UpdateSnapshot(context.Background(), correlationID)
}

func (s *Syncer) logSnapshotError(err error) {
	if err != nil {
		s.logger.Error("Failed to update xDS snapshot for backend change", slog.Any("error", err))
	}
}

// publish reconciles the policy engine's records and reports whether it succeeded.
func (s *Syncer) publish(correlationID string) bool {
	if err := s.publisher.Publish(correlationID); err != nil {
		s.logger.Error("Failed to publish backend records to the policy engine; the next sync retries",
			slog.Any("error", err))
		return false
	}
	return true
}
