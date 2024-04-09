module github.com/psyduck-std/amqp

go 1.22.1

require (
	github.com/psyduck-etl/sdk v0.2.2
	github.com/rabbitmq/amqp091-go v1.9.0
	github.com/zclconf/go-cty v1.14.4
)

require (
	github.com/apparentlymart/go-textseg/v15 v15.0.0 // indirect
	golang.org/x/text v0.14.0 // indirect
)

replace github.com/zclconf/go-cty => github.com/gastrodon/go-cty v1.14.4-1
