package kafka2

import "time"

// RetryPolicy defines how failed messages are retried.
type RetryPolicy struct {
	MaxAttempts      int
	InitialBackoff   time.Duration
	MaxBackoff       time.Duration
	RetryTopicSuffix string // default: ".retry"
}

// DefaultRetryPolicy returns the standard retry configuration.
func DefaultRetryPolicy(cfg RetryConfig) RetryPolicy {
	return RetryPolicy{
		MaxAttempts:      cfg.MaxAttempts,
		InitialBackoff:   time.Duration(cfg.InitialBackoffMs) * time.Millisecond,
		MaxBackoff:       time.Duration(cfg.MaxBackoffMs) * time.Millisecond,
		RetryTopicSuffix: ".retry",
	}
}

// Backoff calculates delay for a given attempt (0-indexed).
func (p RetryPolicy) Backoff(attempt int) time.Duration {
	if attempt <= 0 {
		return p.InitialBackoff
	}
	delay := p.InitialBackoff
	for i := 0; i < attempt; i++ {
		delay *= 2
		if delay >= p.MaxBackoff {
			return p.MaxBackoff
		}
	}
	return delay
}
