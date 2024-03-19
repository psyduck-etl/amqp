package main

import (
	"context"

	"github.com/psyduck-std/sdk"
	"github.com/rabbitmq/amqp091-go"
	"github.com/zclconf/go-cty/cty"
)

type queueConfig struct {
	Connection  string `psy:"connection"`
	Queue       string `psy:"queue"`
	ContentType string `psy:"content-type"`
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

func disconnect(conn *amqp091.Connection, channel *amqp091.Channel, errs chan error) {
	if err := conn.Close(); err != nil {
		errs <- err
	}

	if err := channel.Close(); err != nil {
		errs <- err
	}
}

func Plugin() *sdk.Plugin {
	return &sdk.Plugin{
		Name: "amqp",
		Resources: []*sdk.Resource{
			{
				Kinds: sdk.PRODUCER | sdk.CONSUMER,
				Name:  "amqp-queue",
				Spec: map[string]*sdk.Spec{
					"connection": {
						Name:        "connection",
						Description: "AMQP broker server connection string - amqp://{user}:{password}@{hostname}:{port}",
						Required:    true,
						Type:        sdk.String,
					},
					"queue": {
						Name:        "queue",
						Description: "Name of the rmqp queue to interact with",
						Required:    true,
						Type:        sdk.String,
					},
					"content-type": {
						Name:        "content-type",
						Description: "Content type ",
						Required:    false,
						Type:        sdk.String,
						Default:     cty.StringVal("text/plain"),
					},
				},
				ProvideProducer: func(parse sdk.Parser, _ sdk.SpecParser) (sdk.Producer, error) {
					config := new(queueConfig)
					if err := parse(config); err != nil {
						return nil, err
					}

					conn, channel, queue, err := connect(config)
					if err != nil {
						return nil, err
					}

					// TODO if we encounter an err before we return data, errs, the function will deadlock if errs is unbuffered
					return func() (chan []byte, chan error) {
						data, errs := make(chan []byte), make(chan error, 10)
						messages, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
						if err != nil {
							errs <- err
						}

						go func() {
							defer disconnect(conn, channel, errs)
							for msg := range messages {
								data <- msg.Body
								if err := msg.Ack(false); err != nil {
									errs <- err
								}
							}
						}()

						return data, errs

					}, nil
				},
				ProvideConsumer: func(parse sdk.Parser, _ sdk.SpecParser) (sdk.Consumer, error) {
					config := new(queueConfig)
					if err := parse(config); err != nil {
						return nil, err
					}

					conn, channel, queue, err := connect(config)
					if err != nil {
						return nil, err
					}
					return func() (chan []byte, chan error, chan bool) {
						data, errs, done := make(chan []byte), make(chan error, 10), make(chan bool)
						go func() {
							defer disconnect(conn, channel, errs)

							for d := range data {
								if err := channel.PublishWithContext(context.Background(), "", queue.Name, false, false, amqp091.Publishing{
									ContentType: config.ContentType,
									Body:        d,
								}); err != nil {
									errs <- err
								}
							}
							done <- true
						}()

						return data, errs, done
					}, nil
				},
			},
		},
	}
}
