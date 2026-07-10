package main

import (
	"context"

	"github.com/psyduck-etl/sdk"
	"github.com/rabbitmq/amqp091-go"
)

// produceFromQueue builds a Producer that consumes messages from an AMQP queue
// and emits them as records. Messages are consumed in chunks and acknowledged
// in bulk for efficiency. If codec is non-nil, messages are decoded and
// re-encoded (transcoding).
//
// The per-message hot path is inlined in the loop to avoid per-record
// allocations and call overhead.
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

		// Reused across chunks to avoid one allocation per chunk.
		msgBuf := make([]amqp091.Delivery, config.ChunkSize)

		iters := 0
		defer close(send)
		defer close(errs)
		defer disconnect(conn, channel, errs)

		for {
			for i := uint(0); i < config.ChunkSize; i++ {
				select {
				case msgBuf[i] = <-messages:
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
				body := msg.Body
				if codec != nil {
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
// to an AMQP queue. If codec is non-nil, records are decoded and re-encoded
// (transcoding).
func consumeToQueue(conn *amqp091.Connection, channel *amqp091.Channel, queueName string, config *queueConfig, codec sdk.Codec) sdk.Consumer {
	return func(ctx context.Context, recv <-chan []byte, errs chan<- error, done chan<- struct{}) {
		defer close(done)
		defer close(errs)
		defer disconnect(conn, channel, errs)

		for data := range recv {
			body := data
			if codec != nil {
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
