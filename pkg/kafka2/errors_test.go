package kafka2

import (
	"errors"
	"testing"
)

func TestErrorClassification(t *testing.T) {
	base := errors.New("db down")
	if !IsRetryable(Retryable(base)) {
		t.Fatal("expected retryable")
	}
	if !IsFatal(Fatal(base)) {
		t.Fatal("expected fatal")
	}
	if !IsSkip(Skip(base)) {
		t.Fatal("expected skip")
	}
}
