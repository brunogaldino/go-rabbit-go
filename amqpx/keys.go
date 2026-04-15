package amqpx

// AMQP table keys used across queue/exchange declarations and headers.
const (
	KeyQueueType          = "x-queue-type"
	KeyDeadLetterExchange = "x-dead-letter-exchange"
	KeyDeadLetterRouteKey = "x-dead-letter-routing-key"
	KeyRetriesCount       = "x-retries-count"
	KeyOriginalExchange   = "x-original-exchange"
	KeyOriginalRouteKey   = "x-original-routing-key"
	KeyPublishedAt        = "x-published-at"
	KeyConnectionName     = "connection_name"
)

// QueueTypeQuorum is the AMQP queue type for quorum queues.
const QueueTypeQuorum = "quorum"

// Queue/connection name suffixes.
const (
	SuffixRetry     = ".retry"
	SuffixDLQ       = ".dlq"
	SuffixPublisher = "publisher"
	SuffixConsumer  = "consumer"
)
