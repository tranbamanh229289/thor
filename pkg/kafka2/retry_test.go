package kafka2

import (
	"testing"
	"time"
)

func TestRetryBackoff(t *testing.T) {
	p := RetryPolicy{
		MaxAttempts:    5,
		InitialBackoff: time.Second,
		MaxBackoff:     10 * time.Second,
	}
	if got := p.Backoff(0); got != time.Second {
		t.Fatalf("backoff 0: got %v", got)
	}
	if got := p.Backoff(3); got != 8*time.Second {
		t.Fatalf("backoff 3: got %v", got)
	}
	if got := p.Backoff(10); got != 10*time.Second {
		t.Fatalf("backoff capped: got %v", got)
	}
}
