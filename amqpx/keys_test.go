package amqpx

import "testing"

func TestConstants_NonEmpty(t *testing.T) {
	constants := map[string]string{
		"KeyQueueType":          KeyQueueType,
		"KeyDeadLetterExchange": KeyDeadLetterExchange,
		"KeyDeadLetterRouteKey": KeyDeadLetterRouteKey,
		"KeyRetriesCount":       KeyRetriesCount,
		"KeyOriginalExchange":   KeyOriginalExchange,
		"KeyOriginalRouteKey":   KeyOriginalRouteKey,
		"KeyPublishedAt":        KeyPublishedAt,
		"KeyConnectionName":     KeyConnectionName,
		"QueueTypeQuorum":       QueueTypeQuorum,
		"SuffixRetry":           SuffixRetry,
		"SuffixDLQ":             SuffixDLQ,
		"SuffixPublisher":       SuffixPublisher,
		"SuffixConsumer":        SuffixConsumer,
	}

	for name, val := range constants {
		if val == "" {
			t.Errorf("constant %s should not be empty", name)
		}
	}
}

func TestConstants_UniqueKeys(t *testing.T) {
	keys := []string{
		KeyQueueType, KeyDeadLetterExchange, KeyDeadLetterRouteKey,
		KeyRetriesCount, KeyOriginalExchange, KeyOriginalRouteKey,
		KeyPublishedAt, KeyConnectionName,
	}

	seen := make(map[string]bool, len(keys))
	for _, k := range keys {
		if seen[k] {
			t.Errorf("duplicate key constant: %q", k)
		}
		seen[k] = true
	}
}
