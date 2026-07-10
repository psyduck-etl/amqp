package main

import (
	"github.com/psyduck-etl/sdk"
)

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

			codec, err := resolveCodec(config)
			if err != nil {
				return nil, err
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

			codec, err := resolveCodec(config)
			if err != nil {
				return nil, err
			}

			conn, channel, queue, err := connect(config)
			if err != nil {
				return nil, err
			}

			return consumeToQueue(conn, channel, queue.Name, config, codec), nil
		},
	})
}
