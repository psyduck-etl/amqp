package main

import (
	"testing"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

func TestQueueArgs(t *testing.T) {
	if got := (&queueConfig{}).queueArgs(); got != nil {
		t.Fatalf("no arguments should yield nil table, got %v", got)
	}

	args := (&queueConfig{
		MessageTTL:         60000,
		MaxLength:          1000,
		DeadLetterExchange: "dlx",
		QueueType:          "quorum",
		Overflow:           "reject-publish",
	}).queueArgs()
	want := amqp091.Table{
		"x-message-ttl":          60000,
		"x-max-length":           1000,
		"x-dead-letter-exchange": "dlx",
		"x-queue-type":           "quorum",
		"x-overflow":             "reject-publish",
	}
	if len(args) != len(want) {
		t.Fatalf("args = %v, want %v", args, want)
	}
	for k, v := range want {
		if args[k] != v {
			t.Fatalf("args[%q] = %v, want %v", k, args[k], v)
		}
	}

	// Test that empty overflow (default) is not included in args
	argsNoOverflow := (&queueConfig{
		MaxLength: 1000,
	}).queueArgs()
	if argsNoOverflow["x-overflow"] != nil {
		t.Fatalf("empty overflow should not be in args, got %v", argsNoOverflow)
	}
}

func TestPublishTarget(t *testing.T) {
	// no exchange -> default exchange, routed by queue name
	if ex, key := (&queueConfig{Queue: "q1"}).publishTarget(); ex != "" || key != "q1" {
		t.Fatalf("default exchange target = (%q,%q), want (\"\",\"q1\")", ex, key)
	}

	// named exchange -> exchange + routing key
	c := &queueConfig{Queue: "q1", Exchange: "events", RoutingKey: "orders.created"}
	if ex, key := c.publishTarget(); ex != "events" || key != "orders.created" {
		t.Fatalf("named exchange target = (%q,%q), want (\"events\",\"orders.created\")", ex, key)
	}
}

func TestDeliveryMode(t *testing.T) {
	if m := (&queueConfig{}).deliveryMode(); m != amqp091.Transient {
		t.Fatalf("default delivery mode = %d, want Transient", m)
	}
	if m := (&queueConfig{Persistent: true}).deliveryMode(); m != amqp091.Persistent {
		t.Fatalf("persistent delivery mode = %d, want Persistent", m)
	}
}
