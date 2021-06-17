module jaeger-mongodb

go 1.15

require (
	github.com/hashicorp/go-hclog v0.14.0
	github.com/jaegertracing/jaeger v1.20.0
	github.com/spf13/viper v1.7.1
	github.com/stretchr/testify v1.7.0
	go.mongodb.org/mongo-driver v1.3.2
	go.opentelemetry.io/otel v0.19.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout v0.19.0 // indirect
	go.opentelemetry.io/otel/metric v0.19.0 // indirect
	go.opentelemetry.io/otel/sdk v0.19.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v0.19.0 // indirect
	go.opentelemetry.io/otel/trace v0.19.0 // indirect
)
