package kafka2

// Chain composes middlewares around a handler. First middleware is outermost.
func Chain(handler Handler, mws ...Middleware) Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		handler = mws[i](handler)
	}
	return handler
}

// IdempotencyStore tracks processed event IDs to prevent duplicate handling.
type IdempotencyStore interface {
	// Seen returns true if eventID was already processed.
	Seen(ctx context.Context, eventID string) (bool, error)
	// Mark records eventID as processed.
	Mark(ctx context.Context, eventID string) error
}
