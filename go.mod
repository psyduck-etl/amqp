module github.com/psyduck-std/amqp

go 1.22.1

require (
	github.com/psyduck-etl/sdk v0.4.0-rc2
	github.com/rabbitmq/amqp091-go v1.9.0
	github.com/zclconf/go-cty v1.15.0
)

require (
	github.com/agext/levenshtein v1.2.3 // indirect
	github.com/apparentlymart/go-textseg/v15 v15.0.0 // indirect
	github.com/hashicorp/hcl/v2 v2.22.0 // indirect
	github.com/mitchellh/go-wordwrap v1.0.1 // indirect
	golang.org/x/mod v0.21.0 // indirect
	golang.org/x/sync v0.8.0 // indirect
	golang.org/x/text v0.19.0 // indirect
	golang.org/x/tools v0.26.0 // indirect
)

replace github.com/zclconf/go-cty => github.com/gastrodon/go-cty v1.14.4-1
