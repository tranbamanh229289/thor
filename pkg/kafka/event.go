package kafka

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	HeaderEventID          = "x-event-id"
	HeaderEventType        = "x-event-type"
	HeaderEventVersion     = "x-event-version"
	HeaderEventSource      = "x-source"
	HeaderEventContentType = "x-content-type"
)

const (
	ContentTypeJSON = "application/json"
)

type Event struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Version     string          `json:"version"`
	Timestamp   time.Time       `json:"timestamp"`
	Source      string          `json:"source"`
	AggregateID string          `json:"aggregate_id"`
	Data        json.RawMessage `json:"data"`
	Topic       string          `json:"-"`
	Partition   int32           `json:"-"`
	Offset      int64           `json:"-"`
}

func (e *Event) TopicName() string {
	return e.Type + "." + e.Version
}

func (e *Event) PartitionKey() []byte {
	return []byte(e.AggregateID)
}

func encodeEvent(event *Event) (*kafka.Message, error) {
	if event.ID == "" {
		return nil, fmt.Errorf("kafka event id is required")
	}
	if event.Type == "" || event.Version == "" {
		return nil, fmt.Errorf("kafka event type and event version are required")
	}

	topic := event.TopicName()
	km := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:           event.PartitionKey(),
		Value:         event.Data,
		Timestamp:     event.Timestamp,
		TimestampType: kafka.TimestampCreateTime,
		Headers: []kafka.Header{
			{Key: HeaderEventID, Value: []byte(event.ID)},
			{Key: HeaderEventType, Value: []byte(event.Type)},
			{Key: HeaderEventVersion, Value: []byte(event.Version)},
			{Key: HeaderEventSource, Value: []byte(event.Source)},
			{Key: HeaderEventContentType, Value: []byte(ContentTypeJSON)},
		},
	}
	return km, nil
}

func decodeEvent(km *kafka.Message) (*Event, error) {
	headers := headersToMap(km.Headers)
	event := &Event{
		ID:          headers[HeaderEventID],
		Type:        headers[HeaderEventType],
		Version:     headers[HeaderEventVersion],
		Source:      headers[HeaderEventSource],
		AggregateID: string(km.Key),
		Data:        km.Value,
		Topic:       *km.TopicPartition.Topic,
		Partition:   km.TopicPartition.Partition,
		Offset:      int64(km.TopicPartition.Offset),
		Timestamp:   km.Timestamp,
	}

	return event, nil
}

func headersToMap(headers []kafka.Header) map[string]string {
	m := make(map[string]string, len(headers))
	for _, h := range headers {
		m[h.Key] = string(h.Value)
	}
	return m
}
