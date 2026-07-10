package main

import (
	"context"
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

// produceFromQueue builds a Producer that consumes messages from an AMQP queue
// and emits them as records. Messages are consumed in chunks and acknowledged
// in bulk for efficiency. If encoding is set, messages are decoded and re-encoded
// with the target codec (transcoding).
//
// The hot path (per-message processing) is inlined directly in the loop to avoid
// per-record allocations and function call overhead.
func produceFromQueue(conn *amqp091.Connection, channel *amqp091.Channel, queueName string, config *queueConfig, codec sdk.Codec) sdk.Producer {
	return func(ctx context.Context, send chan<- []byte, errs chan<- error) {
		messages, err := channel.Consume(queueName, "", config.AutoAck, false, false, config.NoWait, nil)
		if err != nil {
			if ctx.Err() == nil {
				errs <- err
			}
			close(send)
			close(errs)
			return
		}

		// Hoist message buffer allocation outside the loop and reuse it.
		// This avoids one allocation per chunk and multiple allocs per message.
		msgBuf := make([]amqp091.Delivery, config.ChunkSize)

		iters := 0
		defer close(send)
		defer close(errs)
		defer disconnect(conn, channel, errs)

		for {
			// Consume a chunk of messages
			for i := uint(0); i < config.ChunkSize; i++ {
				select {
				case msgBuf[i] = <-messages:
				case <-ctx.Done():
					// ctx was cancelled; don't report it as an error
					return
				}
			}

			// Acknowledge the chunk if not auto-acking
			if !config.AutoAck {
				if err := msgBuf[len(msgBuf)-1].Ack(true); err != nil {
					errs <- err
					return
				}
			}

			// Send messages downstream, applying codec if configured
			for _, msg := range msgBuf {
				var body []byte

				if codec != nil {
					// Decode and re-encode for transcoding
					decoded, err := codec.Decode(msg.Body)
					if err != nil {
						errs <- err
						return
					}
					encoded, err := codec.Encode(decoded)
					if err != nil {
						errs <- err
						return
					}
					body = encoded
				} else {
					// Pass through as-is
					body = msg.Body
				}

				select {
				case send <- body:
					iters++
					if config.StopAfter != 0 && iters >= config.StopAfter {
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// consumeToQueue builds a Consumer that receives records and publishes them
// to an AMQP queue. If encoding is set, records are decoded and re-encoded
// with the target codec (transcoding).
//
// The hot path (per-message processing) is inlined directly in the loop to avoid
// per-record allocations and function call overhead.
func consumeToQueue(channel *amqp091.Channel, queueName string, config *queueConfig, codec sdk.Codec, conn *amqp091.Connection) sdk.Consumer {
	return func(ctx context.Context, recv <-chan []byte, errs chan<- error, done chan<- struct{}) {
		defer close(done)
		defer close(errs)
		defer disconnect(conn, channel, errs)

		for data := range recv {
			var body []byte

			if codec != nil {
				// Decode and re-encode for transcoding
				decoded, err := codec.Decode(data)
				if err != nil {
					if ctx.Err() == nil {
						errs <- err
					}
					return
				}
				encoded, err := codec.Encode(decoded)
				if err != nil {
					if ctx.Err() == nil {
						errs <- err
					}
					return
				}
				body = encoded
			} else {
				// Pass through as-is
				body = data
			}

			if err := channel.PublishWithContext(ctx, "", queueName, false, false, amqp091.Publishing{
				ContentType: config.ContentType,
				Body:        body,
			}); err != nil {
				if ctx.Err() == nil {
					errs <- err
				}
				return
			}
		}
	}
}

func Plugin() sdk.Plugin {
	return sdk.NewInProc("amqp", &sdk.Resource{
		Kinds: sdk.PRODUCER | sdk.CONSUMER,
		Name:  "amqp-queue",
		Spec: []*sdk.Spec{
			{
				Name:        "connection",
				Description: "AMQP broker server connection string - amqp://{user}:{password}@{hostname}:{port}",
				Required:    true,
				Type:        sdk.TypeString,
			},
			{
				Name:        "queue",
				Description: "Name of the rmqp queue to interact with",
				Required:    true,
				Type:        sdk.TypeString,
			},
			{
				Name:        "content-type",
				Description: "Content type",
				Required:    false,
				Type:        sdk.TypeString,
				Default:     "text/plain",
			},
			{
				Name:        "encoding",
				Description: "Optional codec spec to decode producer messages and re-encode consumer messages (e.g., \"json\", \"csv\"). When unset, messages are passed through as-is without codec processing",
				Required:    false,
				Type:        sdk.TypeString,
			},
			{
				Name:        "stop-after",
				Description: "Stop after n iterations",
				Required:    false,
				Type:        sdk.TypeInt,
				Default:     0,
			},
			{
				Name:        "chunk-size",
				Description: "Number of messages to get from the channel before ACK",
				Required:    false,
				Type:        sdk.TypeInt,
				Default:     1,
			},
			{
				Name:        "no-wait",
				Description: "TODO",
				Required:    false,
				Type:        sdk.TypeBool,
				Default:     false,
			},
			{
				Name:        "auto-ack",
				Description: "TODO",
				Required:    false,
				Type:        sdk.TypeBool,
				Default:     false,
			},
		},
		ProvideProducer: func(parse sdk.Parser) (sdk.Producer, error) {
			config := new(queueConfig)
			if err := parse(config); err != nil {
				return nil, err
			}

			var codec sdk.Codec
			if config.Encoding != "" {
				c, err := sdk.GetCodec(strings.ToLower(config.Encoding))
				if err != nil {
					return nil, err
				}
				codec = c
			}

			conn, channel, queue, err := connect(config)
			if err != nil {
				return nil, err
			}

			return produceFromQueue(conn, channel, queue.Name, config, codec), nil
		},
		ProvideConsumer: func(parse sdk.Parser) (sdk.Consumer, error) {
			config := new(queueConfig)
			if err := parse(config); err != nil {
				return nil, err
			}

			var codec sdk.Codec
			if config.Encoding != "" {
				c, err := sdk.GetCodec(strings.ToLower(config.Encoding))
				if err != nil {
					return nil, err
				}
				codec = c
			}

			conn, channel, queue, err := connect(config)
			if err != nil {
				return nil, err
			}

			return consumeToQueue(channel, queue.Name, config, codec, conn), nil
		},
	})
}
