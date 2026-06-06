package kafka2

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"
)

// OutboxRelayImpl polls pending outbox records and publishes them to Kafka.
type OutboxRelayImpl struct {
	store     OutboxStore
	publisher EventPublisher
	cfg       OutboxConfig
	log       *zap.Logger
}

// NewOutboxRelay creates a background relay for the transactional outbox.
func NewOutboxRelay(store OutboxStore, publisher EventPublisher, cfg OutboxConfig, log *zap.Logger) *OutboxRelayImpl {
	if log == nil {
		log = zap.NewNop()
	}
	return &OutboxRelayImpl{
		store:     store,
		publisher: publisher,
		cfg:       cfg,
		log:       log,
	}
}

// Run polls pending outbox events until ctx is cancelled.
func (r *OutboxRelayImpl) Run(ctx context.Context) error {
	interval := time.Duration(r.cfg.PollIntervalMs) * time.Millisecond
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	batch := r.cfg.BatchSize
	if batch <= 0 {
		batch = 100
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	r.log.Info("kafka2 outbox relay started")
	for {
		select {
		case <-ctx.Done():
			r.log.Info("kafka2 outbox relay stopped")
			return ctx.Err()
		case <-ticker.C:
			if err := r.processBatch(ctx, batch); err != nil {
				r.log.Error("kafka2 outbox relay batch failed", zap.Error(err))
			}
		}
	}
}

func (r *OutboxRelayImpl) processBatch(ctx context.Context, limit int) error {
	records, err := r.store.FetchPending(ctx, limit)
	if err != nil {
		return err
	}
	for _, rec := range records {
		if err := r.publishOne(ctx, rec); err != nil {
			r.log.Error("kafka2 outbox publish failed",
				zap.String("id", rec.ID),
				zap.Error(err),
			)
		}
	}
	return nil
}

func (r *OutboxRelayImpl) publishOne(ctx context.Context, rec OutboxRecord) error {
	var event Event
	if err := json.Unmarshal(rec.Payload, &event); err != nil {
		if markErr := r.store.MarkFailed(ctx, rec.ID, err.Error()); markErr != nil {
			return fmt.Errorf("mark failed after unmarshal: %w", markErr)
		}
		return fmt.Errorf("unmarshal outbox payload: %w", err)
	}
	if event.EventID == "" {
		event.EventID = rec.ID
	}
	if event.AggregateID == "" {
		event.AggregateID = rec.AggregateID
	}
	if event.EventType == "" {
		event.EventType = rec.EventType
	}
	if event.EventVersion == "" {
		event.EventVersion = rec.EventVersion
	}

	if err := r.publisher.PublishSync(ctx, &event); err != nil {
		if markErr := r.store.MarkFailed(ctx, rec.ID, err.Error()); markErr != nil {
			return fmt.Errorf("mark failed after publish: %w", markErr)
		}
		return err
	}
	return r.store.MarkPublished(ctx, rec.ID)
}
