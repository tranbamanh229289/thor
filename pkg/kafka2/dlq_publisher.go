package kafka2

import (
	"context"
	"fmt"
	"time"
)

// DLQPublisherImpl routes failed events to dead-letter topics.
type DLQPublisherImpl struct {
	publisher EventPublisher
}

// NewDLQPublisher creates a DLQ publisher backed by an EventPublisher.
func NewDLQPublisher(publisher EventPublisher) *DLQPublisherImpl {
	return &DLQPublisherImpl{publisher: publisher}
}

// Publish sends the event to the DLQ topic with error metadata headers.
func (d *DLQPublisherImpl) Publish(ctx context.Context, originalTopic string, event *Event, reason error) error {
	if d.publisher == nil {
		return fmt.Errorf("kafka2: dlq publisher is nil")
	}
	headers := map[string]string{
		HeaderOriginalTopic: originalTopic,
		"x-error":           reason.Error(),
		"x-failed-at":       time.Now().UTC().Format(time.RFC3339),
	}
	dlqTopic := DLQTopic(originalTopic)
	if err := d.publisher.PublishToTopicSync(ctx, dlqTopic, event, headers); err != nil {
		return fmt.Errorf("kafka2: dlq publish: %w", err)
	}
	return nil
}
