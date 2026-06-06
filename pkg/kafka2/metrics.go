package kafka2

import "time"

// MetricsCollector receives produce/consume telemetry.
// Implement with Prometheus in production; use NoopMetrics in tests.
type MetricsCollector interface {
	IncProduce(topic, eventType, status string)
	ObserveProduceLatency(topic string, d time.Duration)

	IncConsume(topic, eventType, status string)
	ObserveConsumeLatency(topic, eventType string, d time.Duration)

	SetConsumerLag(groupID, topic string, partition int32, lag int64)

	IncDLQ(topic, eventType string)
	IncRetry(topic string)
}

// NoopMetrics discards all metrics.
type NoopMetrics struct{}

func (NoopMetrics) IncProduce(_, _, _ string)                          {}
func (NoopMetrics) ObserveProduceLatency(_ string, _ time.Duration)    {}
func (NoopMetrics) IncConsume(_, _, _ string)                          {}
func (NoopMetrics) ObserveConsumeLatency(_, _ string, _ time.Duration) {}
func (NoopMetrics) SetConsumerLag(_, _ string, _ int32, _ int64)       {}
func (NoopMetrics) IncDLQ(_, _ string)                                 {}
func (NoopMetrics) IncRetry(_ string)                                  {}
