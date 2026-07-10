package main

import (
	"context"
	"errors"

	"github.com/psyduck-etl/sdk"
	"github.com/rabbitmq/amqp091-go"
)

// errDeliveryClosed is reported when the broker closes the delivery channel
// (connection or channel died). Without this, a closed channel would yield
// zero-value Deliveries forever.
var errDeliveryClosed = errors.New("amqp: delivery channel closed by broker")

// produceFromQueue builds a Producer that consumes messages from an AMQP queue
// and emits their raw bytes. Messages are consumed in chunks and acknowledged
// in bulk for efficiency.
func produceFromQueue(conn *amqp091.Connection, channel *amqp091.Channel, queueName string, config *queueConfig) sdk.Producer {
	return func(ctx context.Context, send chan<- []byte, errs chan<- error) {
		defer close(send)
		defer close(errs)
		defer disconnect(conn, channel, errs)

		messages, err := channel.Consume(queueName, "", config.AutoAck, false, false, config.NoWait, nil)
		if err != nil {
			if ctx.Err() == nil {
				errs <- err
			}
			return
		}

		// Reused across chunks to avoid one allocation per chunk.
		msgBuf := make([]amqp091.Delivery, config.ChunkSize)

		iters := 0

		for {
			for i := uint(0); i < config.ChunkSize; i++ {
				select {
				case msg, ok := <-messages:
					if !ok {
						errs <- errDeliveryClosed
						return
					}
					msgBuf[i] = msg
				case <-ctx.Done():
					return
				}
			}

			if !config.AutoAck {
				if err := msgBuf[len(msgBuf)-1].Ack(true); err != nil {
					errs <- err
					return
				}
			}

			for _, msg := range msgBuf {
				select {
				case send <- msg.Body:
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

// consumeToQueue builds a Consumer that publishes raw record bytes to an
// AMQP queue.
func consumeToQueue(conn *amqp091.Connection, channel *amqp091.Channel, queueName string, config *queueConfig) sdk.Consumer {
	return func(ctx context.Context, recv <-chan []byte, errs chan<- error, done chan<- struct{}) {
		defer close(done)
		defer close(errs)
		defer disconnect(conn, channel, errs)

		for data := range recv {
			if err := channel.PublishWithContext(ctx, "", queueName, false, false, amqp091.Publishing{
				ContentType: config.ContentType,
				Body:        data,
			}); err != nil {
				if ctx.Err() == nil {
					errs <- err
				}
				return
			}
		}
	}
}
