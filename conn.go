package main

import (
	amqp091 "github.com/rabbitmq/amqp091-go"
)

type queueConfig struct {
	Connection string `psy:"connection"`
	Queue      string `psy:"queue"`

	// exchange routing. When Exchange is empty, publishing uses the default
	// exchange and routes by queue name (the original behaviour).
	Exchange     string `psy:"exchange"`
	ExchangeType string `psy:"exchange-type"`
	RoutingKey   string `psy:"routing-key"`

	ContentType string `psy:"content-type"`
	StopAfter   int    `psy:"stop-after"`
	Prefetch    int    `psy:"prefetch"`
	NoWait      bool   `psy:"no-wait"`
	AutoAck     bool   `psy:"auto-ack"`

	// topology
	Durable         bool `psy:"durable"`
	AutoDelete      bool `psy:"auto-delete"`
	Exclusive       bool `psy:"exclusive"`
	DeclareExchange bool `psy:"declare-exchange"`

	// reliable publishing
	Confirm    bool `psy:"confirm"`
	Mandatory  bool `psy:"mandatory"`
	Persistent bool `psy:"persistent"`

	// queue arguments
	MessageTTL         int    `psy:"message-ttl"`
	MaxLength          int    `psy:"max-length"`
	DeadLetterExchange string `psy:"dead-letter-exchange"`
	QueueType          string `psy:"queue-type"`
	Overflow           string `psy:"overflow"`
}

// queueArgs assembles the x-* argument table from the typed config fields.
// It returns nil when no arguments are set so QueueDeclare uses broker
// defaults.
func (c *queueConfig) queueArgs() amqp091.Table {
	args := amqp091.Table{}
	if c.MessageTTL > 0 {
		args["x-message-ttl"] = c.MessageTTL
	}
	if c.MaxLength > 0 {
		args["x-max-length"] = c.MaxLength
	}
	if c.DeadLetterExchange != "" {
		args["x-dead-letter-exchange"] = c.DeadLetterExchange
	}
	if c.QueueType != "" {
		args["x-queue-type"] = c.QueueType
	}
	if c.Overflow != "" {
		args["x-overflow"] = c.Overflow
	}
	if len(args) == 0 {
		return nil
	}
	return args
}

// publishTarget resolves the exchange and routing key to publish with. With
// no exchange configured, messages go to the default exchange keyed by the
// queue name; with an exchange, they go to it keyed by routing-key.
func (c *queueConfig) publishTarget() (exchange, key string) {
	if c.Exchange != "" {
		return c.Exchange, c.RoutingKey
	}
	return "", c.Queue
}

func (c *queueConfig) deliveryMode() uint8 {
	if c.Persistent {
		return amqp091.Persistent
	}
	return amqp091.Transient
}

// connect dials the broker, opens a channel, and declares the configured
// topology: an optional exchange, the queue (with arguments), and — when an
// exchange is set — a binding between them.
func (config *queueConfig) connect() (*amqp091.Connection, *amqp091.Channel, amqp091.Queue, error) {
	conn, err := amqp091.Dial(config.Connection)
	if err != nil {
		return nil, nil, amqp091.Queue{}, err
	}

	channel, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, amqp091.Queue{}, err
	}

	if config.DeclareExchange && config.Exchange != "" {
		kind := config.ExchangeType
		if kind == "" {
			kind = "direct"
		}
		if err := channel.ExchangeDeclare(config.Exchange, kind, config.Durable, config.AutoDelete, false, config.NoWait, nil); err != nil {
			disconnect(conn, channel, nil)
			return nil, nil, amqp091.Queue{}, err
		}
	}

	queue, err := channel.QueueDeclare(config.Queue, config.Durable, config.AutoDelete, config.Exclusive, config.NoWait, config.queueArgs())
	if err != nil {
		disconnect(conn, channel, nil)
		return nil, nil, amqp091.Queue{}, err
	}

	if config.Exchange != "" {
		if err := channel.QueueBind(queue.Name, config.RoutingKey, config.Exchange, config.NoWait, nil); err != nil {
			disconnect(conn, channel, nil)
			return nil, nil, amqp091.Queue{}, err
		}
	}

	return conn, channel, queue, nil
}

// disconnect closes the channel then the connection. errs may be nil, in
// which case close errors are discarded (used on setup-failure paths).
func disconnect(conn *amqp091.Connection, channel *amqp091.Channel, errs chan<- error) {
	if err := channel.Close(); err != nil && errs != nil {
		errs <- err
	}

	if err := conn.Close(); err != nil && errs != nil {
		errs <- err
	}
}
