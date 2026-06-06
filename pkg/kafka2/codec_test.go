package kafka2

import (
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestEncodeDecodeEvent(t *testing.T) {
	event, err := NewEvent("order.created", "v1", "product-service", "order-1", map[string]string{"amount": "100"})
	if err != nil {
		t.Fatalf("NewEvent: %v", err)
	}
	event.CorrelationID = "corr-1"

	km, err := encodeEvent(event)
	if err != nil {
		t.Fatalf("encodeEvent: %v", err)
	}

	decoded, err := decodeEvent(km)
	if err != nil {
		t.Fatalf("decodeEvent: %v", err)
	}

	if decoded.EventID != event.EventID {
		t.Fatalf("event_id: got %s want %s", decoded.EventID, event.EventID)
	}
	if decoded.EventType != "order.created" {
		t.Fatalf("event_type: got %s", decoded.EventType)
	}
	if decoded.CorrelationID != "corr-1" {
		t.Fatalf("correlation_id: got %s", decoded.CorrelationID)
	}
	if decoded.TopicName() != "order.created.v1" {
		t.Fatalf("topic name: got %s", decoded.TopicName())
	}
}

func TestDecodeEventFromHeaders(t *testing.T) {
	topic := "order.created.v1"
	km := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 2, Offset: 99},
		Key:            []byte("order-1"),
		Headers: []kafka.Header{
			{Key: HeaderEventID, Value: []byte("evt-1")},
			{Key: HeaderEventType, Value: []byte("order.created")},
			{Key: HeaderEventVersion, Value: []byte("v1")},
			{Key: HeaderSource, Value: []byte("product-service")},
		},
		Timestamp: time.Now(),
	}

	event, err := decodeEvent(km)
	if err != nil {
		t.Fatalf("decodeEvent: %v", err)
	}
	if event.EventID != "evt-1" {
		t.Fatalf("event_id: got %s", event.EventID)
	}
	if event.AggregateID != "order-1" {
		t.Fatalf("aggregate_id: got %s", event.AggregateID)
	}
	if event.Partition != 2 || event.Offset != 99 {
		t.Fatalf("metadata: partition=%d offset=%d", event.Partition, event.Offset)
	}
}
