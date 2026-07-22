package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/psyduck-etl/sdk"
	"github.com/psyduck-etl/sdk/rpc"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

// main serves the plugin over gRPC to the psyduck host that launched this
// binary as a subprocess.
func main() { rpc.Serve(Plugin()) }

// errDeliveryClosed is reported when the broker closes the delivery channel
// (connection or channel died). Without this, a for-range over the closed
// channel would silently return as if the queue had drained cleanly.
var errDeliveryClosed = errors.New("amqp: delivery channel closed by broker")

func Plugin() sdk.Plugin {
	return sdk.NewInProc("amqp",
		&sdk.Resource{
			Kinds: sdk.PRODUCER | sdk.CONSUMER,
			Name:  "queue",
			Spec: []*sdk.Spec{
				{
					Name:        "connection",
					Description: "AMQP broker server connection string - amqp://{user}:{password}@{hostname}:{port}",
					Required:    true,
					Type:        sdk.TypeString,
				},
				{
					Name:        "queue",
					Description: "Name of the queue to interact with",
					Required:    true,
					Type:        sdk.TypeString,
				},
				{
					Name:        "exchange",
					Description: "Exchange to publish to / bind the queue to. Empty uses the default exchange, routing directly by queue name",
					Required:    false,
					Type:        sdk.TypeString,
					Default:     "",
				},
				{
					Name:        "exchange-type",
					Description: "Exchange type when declaring one: direct, fanout, topic, or headers",
					Required:    false,
					Type:        sdk.TypeString,
					Default:     "direct",
				},
				{
					Name:        "routing-key",
					Description: "Routing key used when publishing to (and binding against) a named exchange",
					Required:    false,
					Type:        sdk.TypeString,
					Default:     "",
				},
				{
					Name:        "content-type",
					Description: "Content type set on published messages",
					Required:    false,
					Type:        sdk.TypeString,
					Default:     "text/plain",
				},
				{
					Name:        "stop-after",
					Description: "As a producer, stop after consuming n messages (0 = unbounded)",
					Required:    false,
					Type:        sdk.TypeInt,
					Default:     0,
				},
				{
					Name:        "prefetch",
					Description: "Consumer QoS prefetch: max unacknowledged messages the broker delivers at once (0 = unlimited). Provides backpressure and batching",
					Required:    false,
					Type:        sdk.TypeInt,
					Default:     0,
				},
				{
					Name:        "no-wait",
					Description: "Issue topology/consume declarations without waiting for broker confirmation",
					Required:    false,
					Type:        sdk.TypeBool,
					Default:     false,
				},
				{
					Name:        "auto-ack",
					Description: "As a producer, let the broker consider messages acknowledged on delivery instead of acking after they are forwarded",
					Required:    false,
					Type:        sdk.TypeBool,
					Default:     false,
				},
				{
					Name:        "durable",
					Description: "Declare the queue (and any declared exchange) as durable so it survives broker restarts",
					Required:    false,
					Type:        sdk.TypeBool,
					Default:     false,
				},
				{
					Name:        "auto-delete",
					Description: "Declare the queue (and any declared exchange) auto-delete: removed when the last consumer/binding goes away",
					Required:    false,
					Type:        sdk.TypeBool,
					Default:     false,
				},
				{
					Name:        "exclusive",
					Description: "Declare and consume the queue exclusively for this connection",
					Required:    false,
					Type:        sdk.TypeBool,
					Default:     false,
				},
				{
					Name:        "declare-exchange",
					Description: "Declare the exchange before use (in addition to the queue, which is always declared)",
					Required:    false,
					Type:        sdk.TypeBool,
					Default:     false,
				},
				{
					Name:        "confirm",
					Description: "As a consumer, put the channel in publisher-confirm mode and wait for a broker ack per published message",
					Required:    false,
					Type:        sdk.TypeBool,
					Default:     false,
				},
				{
					Name:        "mandatory",
					Description: "As a consumer, publish with the mandatory flag so unroutable messages are returned rather than silently dropped",
					Required:    false,
					Type:        sdk.TypeBool,
					Default:     false,
				},
				{
					Name:        "persistent",
					Description: "As a consumer, mark published messages persistent so a durable queue retains them across restarts",
					Required:    false,
					Type:        sdk.TypeBool,
					Default:     false,
				},
				{
					Name:        "message-ttl",
					Description: "Queue argument x-message-ttl in milliseconds (0 = unset)",
					Required:    false,
					Type:        sdk.TypeInt,
					Default:     0,
				},
				{
					Name:        "max-length",
					Description: "Queue argument x-max-length: max ready messages before the queue drops/dead-letters (0 = unset)",
					Required:    false,
					Type:        sdk.TypeInt,
					Default:     0,
				},
				{
					Name:        "dead-letter-exchange",
					Description: "Queue argument x-dead-letter-exchange: where rejected/expired messages are routed",
					Required:    false,
					Type:        sdk.TypeString,
					Default:     "",
				},
				{
					Name:        "queue-type",
					Description: "Queue argument x-queue-type, e.g. classic or quorum",
					Required:    false,
					Type:        sdk.TypeString,
					Default:     "",
				},
				{
					Name:        "overflow",
					Description: "Queue argument x-overflow: drop-head or reject-publish (empty = broker default drop-head)",
					Required:    false,
					Type:        sdk.TypeString,
					Default:     "",
				},
			},
			ProvideProducer: func(ctx context.Context, parse sdk.Parser) (sdk.Producer, error) {
				config := new(queueConfig)
				if err := parse(config); err != nil {
					return nil, err
				}

				conn, channel, queue, err := config.connect()
				if err != nil {
					return nil, err
				}

				return func(ctx context.Context, send chan<- []byte, errs chan<- error) {
					defer close(send)
					defer close(errs)
					defer disconnect(conn, channel, errs)

					if config.Prefetch > 0 {
						if err := channel.Qos(config.Prefetch, 0, false); err != nil {
							if ctx.Err() == nil {
								errs <- err
							}
							return
						}
					}

					messages, err := channel.Consume(queue.Name, "", config.AutoAck, config.Exclusive, false, config.NoWait, nil)
					if err != nil {
						if ctx.Err() == nil {
							errs <- err
						}
						return
					}

					iters := 0
					for {
						select {
						case msg, ok := <-messages:
							if !ok {
								errs <- errDeliveryClosed
								return
							}
							select {
							case send <- msg.Body:
							case <-ctx.Done():
								return
							}
							if !config.AutoAck {
								if err := msg.Ack(false); err != nil {
									errs <- err
									return
								}
							}

							iters++
							if config.StopAfter != 0 && iters >= config.StopAfter {
								return
							}
						case <-ctx.Done():
							return
						}
					}
				}, nil
			},
			ProvideConsumer: func(ctx context.Context, parse sdk.Parser) (sdk.Consumer, error) {
				config := new(queueConfig)
				if err := parse(config); err != nil {
					return nil, err
				}

				conn, channel, _, err := config.connect()
				if err != nil {
					return nil, err
				}

				return func(ctx context.Context, recv <-chan []byte, errs chan<- error, done chan<- struct{}) {
					defer close(done)
					defer close(errs)
					defer disconnect(conn, channel, errs)

					var confirms chan amqp091.Confirmation
					if config.Confirm {
						if err := channel.Confirm(false); err != nil {
							if ctx.Err() == nil {
								errs <- err
							}
							return
						}
						confirms = channel.NotifyPublish(make(chan amqp091.Confirmation, 1))
					}

					exchange, key := config.publishTarget()
					mode := config.deliveryMode()

					for d := range recv {
						if err := channel.PublishWithContext(ctx, exchange, key, config.Mandatory, false, amqp091.Publishing{
							ContentType:  config.ContentType,
							DeliveryMode: mode,
							Body:         d,
						}); err != nil {
							if ctx.Err() == nil {
								errs <- err
							}
							return
						}

						if confirms != nil {
							select {
							case c, ok := <-confirms:
								if !ok {
									return
								}
								if !c.Ack {
									errs <- fmt.Errorf("broker nacked published message (delivery tag %d)", c.DeliveryTag)
								}
							case <-ctx.Done():
								return
							}
						}
					}
				}, nil
			},
		},
	)
}
