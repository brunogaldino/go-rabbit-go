package rabbitmq

func WithRoutingKey(rks []string) func(*Consumer) {
	return func(c *Consumer) {
		c.params.RoutingKey = rks
	}
}

func WithExchangeName(exc string) func(*Consumer) {
	return func(c *Consumer) {
		c.params.ExchangeName = exc
	}
}

func WithPrefetch(pre int) func(*Consumer) {
	return func(c *Consumer) {
		c.params.Prefetch = pre
	}
}

func WithAutoDelete() func(*Consumer) {
	return func(c *Consumer) {
		c.params.AutoDelete = true
	}
}

func WithRetryDisabled() func(*Consumer) {
	return func(c *Consumer) {
		c.params.RetryStrategy.Enabled = false
	}
}

func WithRetryExchangeName(ex string) func(c *Consumer) {
	return func(c *Consumer) {
		c.params.RetryStrategy.Exchange = ex
	}
}

func WithRetryMaxAttempt(max int) func(*Consumer) {
	return func(c *Consumer) {
		c.params.RetryStrategy.MaxAttempt = max
	}
}

func WithRetryFn(fn func(d Delivery, attempt int32, err error) int32) func(*Consumer) {
	return func(c *Consumer) {
		c.params.RetryStrategy.DelayFn = fn
	}
}

func WithDLQFn(fn func(string) bool) func(*Consumer) {
	return func(c *Consumer) {
		c.params.DeadletterStrategy.CallbackFn = fn
	}
}

// WithHeadersBinding sets custom header arguments used when binding the queue
// to the exchange. Use this with exchanges of type "headers".
func WithHeadersBinding(headers map[string]any) func(*Consumer) {
	return func(c *Consumer) {
		c.params.HeadersBinding = headers
	}
}
