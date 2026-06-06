package kafka2

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
)

// Recovery recovers panics and converts them to fatal errors.
func Recovery() Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, event *Event) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = Fatal(fmt.Errorf("panic: %v", r))
				}
			}()
			return next(ctx, event)
		}
	}
}

// Logging logs consume processing with structured fields.
func Logging(log *zap.Logger) Middleware {
	if log == nil {
		log = zap.NewNop()
	}
	return func(next Handler) Handler {
		return func(ctx context.Context, event *Event) error {
			start := time.Now()
			err := next(ctx, event)
			fields := []zap.Field{
				zap.String("event_id", event.EventID),
				zap.String("event_type", event.EventType),
				zap.String("correlation_id", event.CorrelationID),
				zap.String("topic", event.Topic),
				zap.Int32("partition", event.Partition),
				zap.Int64("offset", event.Offset),
				zap.Duration("latency", time.Since(start)),
			}
			if err != nil {
				log.Error("kafka2 event handler failed", append(fields, zap.Error(err))...)
			} else {
				log.Info("kafka2 event handled", fields...)
			}
			return err
		}
	}
}

// Metrics records handler outcomes.
func Metrics(collector MetricsCollector) Middleware {
	if collector == nil {
		collector = NoopMetrics{}
	}
	return func(next Handler) Handler {
		return func(ctx context.Context, event *Event) error {
			start := time.Now()
			err := next(ctx, event)
			status := "success"
			switch {
			case err == nil:
			case IsSkip(err):
				status = "skip"
			case IsRetryable(err):
				status = "retry"
			case IsFatal(err):
				status = "fatal"
			default:
				status = "error"
			}
			collector.IncConsume(event.Topic, event.EventType, status)
			collector.ObserveConsumeLatency(event.Topic, event.EventType, time.Since(start))
			return err
		}
	}
}

// Idempotency skips events that were already processed.
func Idempotency(store IdempotencyStore) Middleware {
	return func(next Handler) Handler {
		return func(ctx context.Context, event *Event) error {
			if store == nil || event.EventID == "" {
				return next(ctx, event)
			}
			seen, err := store.Seen(ctx, event.EventID)
			if err != nil {
				return Retryable(fmt.Errorf("idempotency check: %w", err))
			}
			if seen {
				return nil
			}
			if err := next(ctx, event); err != nil {
				return err
			}
			if markErr := store.Mark(ctx, event.EventID); markErr != nil {
				return Retryable(fmt.Errorf("idempotency mark: %w", markErr))
			}
			return nil
		}
	}
}
