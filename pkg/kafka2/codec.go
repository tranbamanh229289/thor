package kafka2

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func encodeEvent(event *Event) (*kafka.Message, error) {
	if event == nil {
		return nil, fmt.Errorf("kafka2: event is nil")
	}
	if event.EventID == "" {
		return nil, fmt.Errorf("kafka2: event_id is required")
	}
	if event.EventType == "" || event.EventVersion == "" {
		return nil, fmt.Errorf("kafka2: event_type and event_version are required")
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}

	body, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("kafka2: marshal event: %w", err)
	}

	topic := event.TopicName()
	km := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:           event.PartitionKey(),
		Value:         body,
		Timestamp:     event.OccurredAt,
		TimestampType: kafka.TimestampCreateTime,
		Headers: []kafka.Header{
			{Key: HeaderEventID, Value: []byte(event.EventID)},
			{Key: HeaderEventType, Value: []byte(event.EventType)},
			{Key: HeaderEventVersion, Value: []byte(event.EventVersion)},
			{Key: HeaderSource, Value: []byte(event.Source)},
			{Key: HeaderContentType, Value: []byte(ContentTypeJSON)},
		},
	}
	if event.CorrelationID != "" {
		km.Headers = append(km.Headers, kafka.Header{Key: HeaderCorrelationID, Value: []byte(event.CorrelationID)})
	}
	if event.CausationID != "" {
		km.Headers = append(km.Headers, kafka.Header{Key: HeaderCausationID, Value: []byte(event.CausationID)})
	}
	return km, nil
}

func decodeEvent(km *kafka.Message) (*Event, error) {
	if km == nil || km.TopicPartition.Topic == nil {
		return nil, fmt.Errorf("kafka2: invalid kafka message")
	}

	var event Event
	if len(km.Value) > 0 {
		if err := json.Unmarshal(km.Value, &event); err != nil {
			return nil, fmt.Errorf("kafka2: unmarshal event: %w", err)
		}
	}

	headers := headersToMap(km.Headers)
	if event.EventID == "" {
		event.EventID = headers[HeaderEventID]
	}
	if event.EventType == "" {
		event.EventType = headers[HeaderEventType]
	}
	if event.EventVersion == "" {
		event.EventVersion = headers[HeaderEventVersion]
	}
	if event.Source == "" {
		event.Source = headers[HeaderSource]
	}
	if event.CorrelationID == "" {
		event.CorrelationID = headers[HeaderCorrelationID]
	}
	if event.CausationID == "" {
		event.CausationID = headers[HeaderCausationID]
	}
	if event.AggregateID == "" && len(km.Key) > 0 {
		event.AggregateID = string(km.Key)
	}
	if event.OccurredAt.IsZero() && !km.Timestamp.IsZero() {
		event.OccurredAt = km.Timestamp
	}

	event.Topic = *km.TopicPartition.Topic
	event.Partition = km.TopicPartition.Partition
	event.Offset = int64(km.TopicPartition.Offset)

	return &event, nil
}

func headersToMap(headers []kafka.Header) map[string]string {
	m := make(map[string]string, len(headers))
	for _, h := range headers {
		m[h.Key] = string(h.Value)
	}
	return m
}

func retryCountFromHeaders(headers []kafka.Header) int {
	for _, h := range headers {
		if h.Key == HeaderRetryCount {
			n, err := strconv.Atoi(string(h.Value))
			if err == nil {
				return n
			}
		}
	}
	return 0
}

func applyHeaderMap(km *kafka.Message, headers map[string]string) {
	for k, v := range headers {
		km.Headers = append(km.Headers, kafka.Header{Key: k, Value: []byte(v)})
	}
}

func withRetryCount(km *kafka.Message, count int) {
	for i, h := range km.Headers {
		if h.Key == HeaderRetryCount {
			km.Headers[i].Value = []byte(strconv.Itoa(count))
			return
		}
	}
	km.Headers = append(km.Headers, kafka.Header{
		Key:   HeaderRetryCount,
		Value: []byte(strconv.Itoa(count)),
	})
}
