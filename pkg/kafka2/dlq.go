package kafka2

import "context"

// DLQPublisher routes unrecoverable messages to dead-letter topics.
type DLQPublisher interface {
	Publish(ctx context.Context, originalTopic string, event *Event, reason error) error
}
