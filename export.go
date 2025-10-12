package main

import (
	"context"
	"io"

	"github.com/psyduck-etl/sdk"
	"github.com/rabbitmq/amqp091-go"
	"github.com/zclconf/go-cty/cty"
)

type queueConfig struct {
	Connection  string `cty:"connection"`
	Queue       string `cty:"queue"`
	ContentType string `cty:"content-type"`
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

func disconnect(conn *amqp091.Connection, channel *amqp091.Channel) error {
	var firstErr error
	if err := channel.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := conn.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// AMQP Producer
type amqpProducer struct {
	config *queueConfig
	conn   *amqp091.Connection
	channel *amqp091.Channel
	queue  amqp091.Queue
}

func (p *amqpProducer) Start(ctx context.Context, send func([]byte) error) error {
	defer disconnect(p.conn, p.channel)

	messages, err := p.channel.Consume(p.queue.Name, "", p.config.AutoAck, false, false, p.config.NoWait, nil)
	if err != nil {
		return err
	}

	iters := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-messages:
			if !p.config.AutoAck {
				if err := msg.Ack(false); err != nil {
					return err
				}
			}
			
			if err := send(msg.Body); err != nil {
				return err
			}
			
			iters++
			if p.config.StopAfter != 0 && iters >= p.config.StopAfter {
				return nil
			}
		}
	}
}

func (p *amqpProducer) Stop() error {
	return disconnect(p.conn, p.channel)
}

// AMQP Consumer
type amqpConsumer struct {
	config *queueConfig
	conn   *amqp091.Connection
	channel *amqp091.Channel
	queue  amqp091.Queue
}

func (c *amqpConsumer) Consume(ctx context.Context, recv func() ([]byte, error)) error {
	defer disconnect(c.conn, c.channel)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			data, err := recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}

			if err := c.channel.PublishWithContext(ctx, "", c.queue.Name, false, false, amqp091.Publishing{
				ContentType: c.config.ContentType,
				Body:        data,
			}); err != nil {
				return err
			}
		}
	}
}

func (c *amqpConsumer) Stop() error {
	return disconnect(c.conn, c.channel)
}

// Provider types
type amqpQueueProvider struct{}

func (amqpQueueProvider) ProvideProducer(parse sdk.Parser) (sdk.Producer, error) {
	config := new(queueConfig)
	if err := parse(config); err != nil {
		return nil, err
	}

	conn, channel, queue, err := connect(config)
	if err != nil {
		return nil, err
	}

	return &amqpProducer{
		config:  config,
		conn:    conn,
		channel: channel,
		queue:   queue,
	}, nil
}

func (amqpQueueProvider) ProvideConsumer(parse sdk.Parser) (sdk.Consumer, error) {
	config := new(queueConfig)
	if err := parse(config); err != nil {
		return nil, err
	}

	conn, channel, queue, err := connect(config)
	if err != nil {
		return nil, err
	}

	return &amqpConsumer{
		config:  config,
		conn:    conn,
		channel: channel,
		queue:   queue,
	}, nil
}

var AMQPQueue = amqpQueueProvider{}

func main() {
	plugin := &sdk.Plugin{
		Name: "amqp",
		Resources: []*sdk.Resource{
			{
				Name:            "amqp-queue",
				Kinds:           sdk.PRODUCER | sdk.CONSUMER,
				ProvideProducer: AMQPQueue,
				ProvideConsumer: AMQPQueue,
				Spec: []*sdk.Spec{
					{
						Name:        "connection",
						Description: "AMQP broker server connection string - amqp://{user}:{password}@{hostname}:{port}",
						Type:        cty.String,
						Required:    true,
					},
					{
						Name:        "queue",
						Description: "Name of the rmqp queue to interact with",
						Type:        cty.String,
						Required:    true,
					},
					{
						Name:        "content-type",
						Description: "Content type",
						Type:        cty.String,
						Default:     cty.StringVal("text/plain"),
					},
					{
						Name:        "stop-after",
						Description: "Stop after n iterations",
						Type:        cty.Number,
						Default:     cty.NumberIntVal(0),
					},
					{
						Name:        "chunk-size",
						Description: "Number of messages to get from the channel before ACK",
						Type:        cty.Number,
						Default:     cty.NumberUIntVal(1),
					},
					{
						Name:        "no-wait",
						Description: "TODO",
						Type:        cty.Bool,
						Default:     cty.BoolVal(false),
					},
					{
						Name:        "auto-ack",
						Description: "TODO",
						Type:        cty.Bool,
						Default:     cty.BoolVal(false),
					},
				},
			},
		},
	}

	// Run as gRPC client process
	sdk.RunAsClientProcess(plugin)
}
