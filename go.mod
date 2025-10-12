module github.com/psyduck-std/amqp

go 1.24.0

require (
	github.com/psyduck-etl/sdk v0.2.2
	github.com/rabbitmq/amqp091-go v1.10.0
	github.com/zclconf/go-cty v1.15.0
)

require (
	github.com/agext/levenshtein v1.2.3 // indirect
	github.com/apparentlymart/go-textseg/v15 v15.0.0 // indirect
	github.com/hashicorp/hcl/v2 v2.22.0 // indirect
	github.com/mitchellh/go-wordwrap v1.0.1 // indirect
	golang.org/x/mod v0.25.0 // indirect
	golang.org/x/net v0.42.0 // indirect
	golang.org/x/sync v0.16.0 // indirect
	golang.org/x/sys v0.34.0 // indirect
	golang.org/x/text v0.27.0 // indirect
	golang.org/x/tools v0.34.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250804133106-a7a43d27e69b // indirect
	google.golang.org/grpc v1.76.0 // indirect
	google.golang.org/protobuf v1.36.10 // indirect
)

replace github.com/zclconf/go-cty => github.com/gastrodon/go-cty v1.14.4-1

replace github.com/psyduck-etl/sdk => ../sdk
