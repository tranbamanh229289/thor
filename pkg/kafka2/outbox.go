package kafka2

import (
	"context"
	"time"
)

// OutboxRecord is a pending event stored in the service database.
type OutboxRecord struct {
	ID          string
	AggregateID string
	EventType   string
	EventVersion string
	Payload     []byte
	Status      OutboxStatus
	CreatedAt   time.Time
	PublishedAt *time.Time
}

// OutboxStatus tracks relay progress.
type OutboxStatus string

const (
	OutboxPending   OutboxStatus = "pending"
	OutboxPublished OutboxStatus = "published"
	OutboxFailed    OutboxStatus = "failed"
)

// OutboxStore persists events atomically with business transactions.
type OutboxStore interface {
	Insert(ctx context.Context, event *Event) error
	FetchPending(ctx context.Context, limit int) ([]OutboxRecord, error)
	MarkPublished(ctx context.Context, id string) error
	MarkFailed(ctx context.Context, id string, reason string) error
}

// OutboxRelay polls pending records and publishes them via EventPublisher.
type OutboxRelay interface {
	Run(ctx context.Context) error
}
