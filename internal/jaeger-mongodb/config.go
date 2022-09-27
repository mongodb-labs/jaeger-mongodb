package jaeger_mongodb

import (
	"flag"
	"time"

	"github.com/spf13/viper"
)

const (
	mongoUrl             = "mongo_url"
	mongoDatabase        = "mongo_database"
	mongoCollection      = "mongo_collection"
	mongoTimeoutDuration = "mongo_timeout_duration"
	mongoSpanTTLDuration = "mongo_span_ttl_duration"
	otelTracingRatio     = "otel_tracing_ratio"
	otelExporterEndpoint = "otel_exporter_endpoint"
)

type Configuration struct {
	MongoUrl             string        `yaml:"mongo_url"`
	MongoDatabase        string        `yaml:"mongo_database"`
	MongoCollection      string        `yaml:"mongo_collection"`
	MongoTimeoutDuration time.Duration `yaml:"mongo_timeout_duration"`
	MongoSpanTTLDuration time.Duration `yaml:"mongo_span_ttl_duration"`
	OtelTracingRatio     float64       `yaml:"otel_tracing_ratio"`
	OtelExporterEndpoint string        `yaml:"otel_exporter_endpoint"`
}

// Options stores the configuration entries for this storage
type Options struct {
	Configuration Configuration
}

// AddFlags from this storage to the CLI
func AddFlags(flagSet *flag.FlagSet) {
	//flagSet.Int(limit, 0, "The maximum amount of traces to store in memory. The default number of traces is unbounded.")
}

// InitFromViper initializes the options struct with values from Viper
func (opt *Options) InitFromViper(v *viper.Viper) {

	v.SetDefault(mongoUrl, "mongodb://localhost:27017")
	v.SetDefault(mongoDatabase, "traces")
	v.SetDefault(mongoCollection, "spans")
	v.SetDefault(mongoTimeoutDuration, "5s")
	v.SetDefault(mongoSpanTTLDuration, "14d")
	v.SetDefault(otelTracingRatio, 0.0) // tracing is disabled by default
	v.SetDefault(otelExporterEndpoint, "http://localhost:14268/api/traces")

	opt.Configuration.MongoUrl = v.GetString(mongoUrl)
	opt.Configuration.MongoDatabase = v.GetString(mongoDatabase)
	opt.Configuration.MongoCollection = v.GetString(mongoCollection)
	opt.Configuration.MongoTimeoutDuration = v.GetDuration(mongoTimeoutDuration)
	opt.Configuration.MongoSpanTTLDuration = v.GetDuration(mongoSpanTTLDuration)
	opt.Configuration.OtelTracingRatio = v.GetFloat64(otelTracingRatio)
	opt.Configuration.OtelExporterEndpoint = v.GetString(otelExporterEndpoint)
}
