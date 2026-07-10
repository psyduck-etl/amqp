package main

import (
	"strings"

	"github.com/psyduck-etl/sdk"
	"github.com/rabbitmq/amqp091-go"
)

type queueConfig struct {
	Connection  string `cty:"connection"`
	Queue       string `cty:"queue"`
	ContentType string `cty:"content-type"`
	Encoding    string `cty:"encoding"`
	StopAfter   int    `cty:"stop-after"`
	ChunkSize   uint   `cty:"chunk-size"`
	NoWait      bool   `cty:"no-wait"`
	AutoAck     bool   `cty:"auto-ack"`
}

func connect(config *queueConfig) (*amqp091.Connection, *amqp091.Channel, amqp091.Queue, error) {
	conn, err := amqp091.Dial(config.Connection)
	if err != nil {
		return nil, nil, amqp091.Queue{}, err
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, nil, amqp091.Queue{}, err
	}

	// TODO sdk should support an object, where we would have queue declare options set
	// but for now, just defaults
	queue, err := channel.QueueDeclare(config.Queue, false, false, false, false, nil)
	if err != nil {
		return nil, nil, amqp091.Queue{}, err
	}

	return conn, channel, queue, nil
}

func disconnect(conn *amqp091.Connection, channel *amqp091.Channel, errs chan<- error) {
	if err := conn.Close(); err != nil {
		errs <- err
	}

	if err := channel.Close(); err != nil {
		errs <- err
	}
}

// resolveCodec returns the codec named by config.Encoding, or nil if unset.
func resolveCodec(config *queueConfig) (sdk.Codec, error) {
	if config.Encoding == "" {
		return nil, nil
	}
	return sdk.GetCodec(strings.ToLower(config.Encoding))
}
