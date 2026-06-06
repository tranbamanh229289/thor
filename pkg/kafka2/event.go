package kafka2

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"time"
)

// Header keys propagated on every Kafka message.
const (
	HeaderEventID        = "x-event-id"
	HeaderEventType      = "x-event-type"
	HeaderEventVersion   = "x-event-version"
	HeaderSource         = "x-source"
	HeaderCorrelationID  = "x-correlation-id"
	HeaderCausationID    = "x-causation-id"
	HeaderContentType    = "x-content-type"
	HeaderRetryCount     = "x-retry-count"
	HeaderOriginalTopic  = "x-original-topic"
)

const (
	ContentTypeJSON = "application/json"
)

// Event is the canonical envelope for all Kafka messages in the EDA system.
// Business payload lives in Data; wire format is JSON.
type Event struct {
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	EventVersion  string          `json:"event_version"`
	Source        string          `json:"source"`
	OccurredAt    time.Time       `json:"occurred_at"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	CausationID   string          `json:"causation_id,omitempty"`
	AggregateID   string          `json:"aggregate_id,omitempty"`
	Data          json.RawMessage `json:"data"`

	// Kafka metadata — populated on consume, ignored on produce.
	Topic     string `json:"-"`
	Partition int32  `json:"-"`
	Offset    int64  `json:"-"`
}

// UnmarshalData decodes the business payload into dst.
func (e *Event) UnmarshalData(dst any) error {
	return json.Unmarshal(e.Data, dst)
}

// SetData encodes v as the business payload.
func (e *Event) SetData(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	e.Data = b
	return nil
}

// TopicName returns the Kafka topic for this event, e.g. "order.created.v1".
func (e *Event) TopicName() string {
	return e.EventType + "." + e.EventVersion
}

// PartitionKey returns the key used for Kafka partitioning (ordering per aggregate).
func (e *Event) PartitionKey() []byte {
	return []byte(e.AggregateID)
}

// NewEvent creates an event with a generated ID and current timestamp.
func NewEvent(eventType, version, source, aggregateID string, data any) (*Event, error) {
	id, err := newEventID()
	if err != nil {
		return nil, err
	}
	e := &Event{
		EventID:      id,
		EventType:    eventType,
		EventVersion: version,
		Source:       source,
		AggregateID:  aggregateID,
		OccurredAt:   time.Now().UTC(),
	}
	if data != nil {
		if err := e.SetData(data); err != nil {
			return nil, err
		}
	}
	return e, nil
}

func newEventID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("kafka2: generate event id: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
