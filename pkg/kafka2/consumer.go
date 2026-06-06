package kafka2

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

// Handler processes a single domain event.
type Handler func(ctx context.Context, event *Event) error

// Middleware wraps a Handler with cross-cutting concerns.
type Middleware func(Handler) Handler

// EventConsumer subscribes to topics and dispatches events via a partition-aware worker pool.
type EventConsumer interface {
	Use(mw ...Middleware)
	Subscribe(ctx context.Context, topics []string, handler Handler) error
	Close(ctx context.Context) error
}

// ConsumerOption configures Consumer construction.
type ConsumerOption func(*Consumer)

// WithConsumerMetrics sets the metrics collector.
func WithConsumerMetrics(m MetricsCollector) ConsumerOption {
	return func(c *Consumer) { c.metrics = m }
}

// WithRetryPublisher enables retry-topic routing on retryable errors.
func WithRetryPublisher(p EventPublisher) ConsumerOption {
	return func(c *Consumer) { c.publisher = p }
}

// WithDLQPublisher enables dead-letter routing on fatal errors.
func WithDLQPublisher(d DLQPublisher) ConsumerOption {
	return func(c *Consumer) { c.dlq = d }
}

// Consumer implements EventConsumer.
type Consumer struct {
	consumer    *kafka.Consumer
	log         *zap.Logger
	cfg         Config
	metrics     MetricsCollector
	middlewares []Middleware
	publisher   EventPublisher
	dlq         DLQPublisher
	retryPolicy RetryPolicy

	pool   *workerPool
	closed bool
	mu     sync.Mutex
}

// NewConsumer creates a partition-aware Kafka event consumer.
func NewConsumer(cfg Config, log *zap.Logger, opts ...ConsumerOption) (*Consumer, error) {
	if log == nil {
		log = zap.NewNop()
	}
	c := &Consumer{
		log:         log,
		cfg:         cfg,
		metrics:     NoopMetrics{},
		retryPolicy: DefaultRetryPolicy(cfg.Retry),
	}
	for _, opt := range opts {
		opt(c)
	}

	consumer, err := kafka.NewConsumer(consumerConfigMap(cfg))
	if err != nil {
		return nil, fmt.Errorf("kafka2: new consumer: %w", err)
	}
	c.consumer = consumer
	c.pool = newWorkerPool(cfg.Consumer.WorkerBufferSize)
	log.Info("kafka2 consumer created", zap.String("group_id", cfg.Consumer.GroupID))
	return c, nil
}

func (c *Consumer) Use(mw ...Middleware) {
	c.middlewares = append(c.middlewares, mw...)
}

func (c *Consumer) Subscribe(ctx context.Context, topics []string, handler Handler) error {
	if err := c.consumer.SubscribeTopics(topics, nil); err != nil {
		return fmt.Errorf("kafka2: subscribe %v: %w", topics, err)
	}
	c.log.Info("kafka2 consumer subscribed", zap.Strings("topics", topics))

	handler = Chain(handler, c.middlewares...)
	pollTimeout := c.cfg.Consumer.PollTimeoutMs
	if pollTimeout <= 0 {
		pollTimeout = 100
	}

	for {
		select {
		case <-ctx.Done():
			c.log.Info("kafka2 consumer shutting down")
			c.pool.shutdown()
			return ctx.Err()
		default:
		}

		ev := c.consumer.Poll(pollTimeout)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:
			km := e
			c.pool.dispatch(km.TopicPartition.Partition, km, func(msg *kafka.Message) {
				if err := c.processMessage(ctx, handler)(msg); err != nil {
					c.log.Error("kafka2 message processing failed", zap.Error(err))
				}
			})
		case kafka.Error:
			if e.IsFatal() {
				return fmt.Errorf("kafka2: fatal consumer error: %w", e)
			}
			c.log.Error("kafka2 consumer error", zap.Error(e))
		case kafka.AssignedPartitions:
			c.log.Info("kafka2 partitions assigned", zap.Any("partitions", e.Partitions))
		case kafka.RevokedPartitions:
			c.log.Info("kafka2 partitions revoked", zap.Any("partitions", e.Partitions))
			for _, tp := range e.Partitions {
				c.pool.remove(tp.Partition)
			}
		case kafka.OffsetsCommitted:
			if e.Error != nil {
				c.log.Warn("kafka2 offset commit failed", zap.Error(e.Error))
			}
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, handler Handler) func(*kafka.Message) error {
	return func(km *kafka.Message) error {
		start := time.Now()
		event, err := decodeEvent(km)
		if err != nil {
			c.log.Error("kafka2 decode failed", zap.Error(err))
			if commitErr := c.commit(km); commitErr != nil {
				return commitErr
			}
			return nil
		}

		err = handler(ctx, event)
		topic := event.Topic
		eventType := event.EventType

		switch {
		case err == nil:
			c.metrics.IncConsume(topic, eventType, "success")
		case IsSkip(err):
			c.metrics.IncConsume(topic, eventType, "skip")
		case IsRetryable(err):
			c.metrics.IncConsume(topic, eventType, "retry")
		case IsFatal(err):
			c.metrics.IncConsume(topic, eventType, "fatal")
		default:
			err = Retryable(err)
			c.metrics.IncConsume(topic, eventType, "retry")
		}
		c.metrics.ObserveConsumeLatency(topic, eventType, time.Since(start))

		if handleErr := c.handleResult(ctx, km, event, err); handleErr != nil {
			return handleErr
		}
		return c.commit(km)
	}
}

func (c *Consumer) handleResult(ctx context.Context, km *kafka.Message, event *Event, err error) error {
	if err == nil || IsSkip(err) {
		return nil
	}

	retryCount := retryCountFromHeaders(km.Headers)

	if IsRetryable(err) && c.publisher != nil && retryCount < c.retryPolicy.MaxAttempts {
		retryEvent := *event
		retryTopic := RetryTopic(event.Topic)
		headers := map[string]string{
			HeaderOriginalTopic: event.Topic,
			HeaderRetryCount:    strconv.Itoa(retryCount + 1),
		}
		if pubErr := c.publisher.PublishToTopicSync(ctx, retryTopic, &retryEvent, headers); pubErr != nil {
			return fmt.Errorf("kafka2: retry publish: %w", pubErr)
		}
		c.metrics.IncRetry(event.Topic)
		return nil
	}

	if c.dlq != nil {
		reason := err
		if !IsFatal(err) {
			reason = Fatal(fmt.Errorf("max retries exceeded (%d): %w", retryCount, err))
		}
		if dlqErr := c.dlq.Publish(ctx, event.Topic, event, reason); dlqErr != nil {
			return fmt.Errorf("kafka2: dlq publish: %w", dlqErr)
		}
		c.metrics.IncDLQ(event.Topic, event.EventType)
	}
	return nil
}

func (c *Consumer) commit(km *kafka.Message) error {
	if c.cfg.Consumer.EnableAutoCommit {
		return nil
	}
	if _, err := c.consumer.CommitMessage(km); err != nil {
		c.log.Error("kafka2 commit failed", zap.Error(err))
		return fmt.Errorf("kafka2: commit: %w", err)
	}
	return nil
}

func (c *Consumer) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.consumer == nil {
		return nil
	}
	c.closed = true
	c.pool.shutdown()

	if !c.cfg.Consumer.EnableAutoCommit {
		if _, err := c.consumer.Commit(); err != nil {
			c.log.Warn("kafka2 final commit failed", zap.Error(err))
		}
	}
	if err := c.consumer.Close(); err != nil {
		return fmt.Errorf("kafka2: close consumer: %w", err)
	}
	c.log.Info("kafka2 consumer closed")
	return nil
}

func consumerConfigMap(cfg Config) *kafka.ConfigMap {
	cc := cfg.Consumer
	cm := &kafka.ConfigMap{
		"bootstrap.servers":             cc.BootstrapServers,
		"group.id":                      cc.GroupID,
		"auto.offset.reset":             cc.AutoOffsetReset,
		"enable.auto.commit":            cc.EnableAutoCommit,
		"session.timeout.ms":            cc.SessionTimeoutMs,
		"heartbeat.interval.ms":         cc.HeartbeatIntervalMs,
		"max.poll.interval.ms":          cc.MaxPollIntervalMs,
		"fetch.min.bytes":               cc.FetchMinBytes,
		"fetch.max.bytes":               cc.FetchMaxBytes,
		"partition.assignment.strategy": cc.PartitionAssignmentStrategy,
		"socket.keepalive.enable":       cc.SocketKeepAliveEnable,
	}
	applySecurity(cm, cfg.Security)
	return cm
}

// workerPool dispatches messages to per-partition workers to preserve ordering.
type workerPool struct {
	buffer int
	mu     sync.Mutex
	workers map[int32]*partitionWorker
	wg     sync.WaitGroup
}

type partitionWorker struct {
	ch   chan *kafka.Message
	done chan struct{}
}

func newWorkerPool(buffer int) *workerPool {
	if buffer <= 0 {
		buffer = 256
	}
	return &workerPool{
		buffer:  buffer,
		workers: make(map[int32]*partitionWorker),
	}
}

func (wp *workerPool) dispatch(partition int32, km *kafka.Message, fn func(*kafka.Message)) {
	wp.mu.Lock()
	w, ok := wp.workers[partition]
	if !ok {
		w = &partitionWorker{
			ch:   make(chan *kafka.Message, wp.buffer),
			done: make(chan struct{}),
		}
		wp.workers[partition] = w
		wp.wg.Add(1)
		go wp.runWorker(w, fn)
	}
	wp.mu.Unlock()

	select {
	case w.ch <- km:
	case <-w.done:
	}
}

func (wp *workerPool) runWorker(w *partitionWorker, fn func(*kafka.Message)) {
	defer wp.wg.Done()
	for km := range w.ch {
		fn(km)
	}
}

func (wp *workerPool) remove(partition int32) {
	wp.mu.Lock()
	w, ok := wp.workers[partition]
	if ok {
		delete(wp.workers, partition)
		close(w.ch)
	}
	wp.mu.Unlock()
}

func (wp *workerPool) shutdown() {
	wp.mu.Lock()
	for p, w := range wp.workers {
		close(w.ch)
		delete(wp.workers, p)
	}
	wp.mu.Unlock()
	wp.wg.Wait()
}
