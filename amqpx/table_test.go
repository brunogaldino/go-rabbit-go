package amqpx

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestMergeTable_BothPopulated(t *testing.T) {
	base := amqp.Table{"a": 1, "b": 2}
	over := amqp.Table{"b": 99, "c": 3}

	got := MergeTable(base, over)

	if got["a"] != 1 {
		t.Errorf("expected a=1, got %v", got["a"])
	}
	if got["b"] != 99 {
		t.Errorf("expected b=99 (overridden), got %v", got["b"])
	}
	if got["c"] != 3 {
		t.Errorf("expected c=3, got %v", got["c"])
	}
}

func TestMergeTable_NilBase(t *testing.T) {
	over := amqp.Table{"x": "y"}

	got := MergeTable(nil, over)

	if got["x"] != "y" {
		t.Errorf("expected x=y, got %v", got["x"])
	}
}

func TestMergeTable_NilOverride(t *testing.T) {
	base := amqp.Table{"a": 1}

	got := MergeTable(base, nil)

	if got["a"] != 1 {
		t.Errorf("expected a=1, got %v", got["a"])
	}
}

func TestMergeTable_BothNil(t *testing.T) {
	got := MergeTable(nil, nil)

	if got == nil {
		t.Fatal("expected non-nil table")
	}
	if len(got) != 0 {
		t.Errorf("expected empty table, got %v", got)
	}
}

func TestMergeTable_DoesNotMutateInputs(t *testing.T) {
	base := amqp.Table{"a": 1}
	over := amqp.Table{"b": 2}

	_ = MergeTable(base, over)

	if _, ok := base["b"]; ok {
		t.Error("base was mutated")
	}
	if _, ok := over["a"]; ok {
		t.Error("override was mutated")
	}
}
