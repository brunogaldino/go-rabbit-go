package amqpx

import (
	"maps"

	amqp "github.com/rabbitmq/amqp091-go"
)

// MergeTable returns a new [amqp.Table] containing all entries from base
// with entries from override merged on top. Neither input table is modified.
func MergeTable(base, override amqp.Table) amqp.Table {
	merged := maps.Clone(base)
	if merged == nil {
		merged = amqp.Table{}
	}
	maps.Copy(merged, override)

	return merged
}
