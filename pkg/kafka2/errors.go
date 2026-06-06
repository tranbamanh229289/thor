package kafka2

import "errors"

// ErrRetryable signals the consumer middleware to route the message to a retry topic.
var ErrRetryable = errors.New("kafka2: retryable error")

// ErrFatal signals the consumer middleware to route the message to DLQ.
var ErrFatal = errors.New("kafka2: fatal error")

// ErrSkip signals the consumer to commit and drop the message (poison pill).
var ErrSkip = errors.New("kafka2: skip message")

// Retryable wraps err as a retryable consumer error.
func Retryable(err error) error {
	return errors.Join(ErrRetryable, err)
}

// Fatal wraps err as a fatal consumer error.
func Fatal(err error) error {
	return errors.Join(ErrFatal, err)
}

// Skip wraps err as a skip consumer error.
func Skip(err error) error {
	return errors.Join(ErrSkip, err)
}

// IsRetryable reports whether err should be retried.
func IsRetryable(err error) bool {
	return errors.Is(err, ErrRetryable)
}

// IsFatal reports whether err should go to DLQ.
func IsFatal(err error) bool {
	return errors.Is(err, ErrFatal)
}

// IsSkip reports whether err should be committed and dropped.
func IsSkip(err error) bool {
	return errors.Is(err, ErrSkip)
}
