package main

import (
	"context"
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	jaeger_mongodb "jaeger-mongodb/internal/jaeger-mongodb"
)

var configPath string

func main() {
	flag.StringVar(&configPath, "config", "", "A path to the plugin's configuration file")
	flag.Parse()

	logger := hclog.New(&hclog.LoggerOptions{
		Name:       "jaeger-mongodb",
		Level:      hclog.Warn, // Jaeger only captures >= Warn, so don't bother logging below Warn
		JSONFormat: true,
	})

	v := viper.New()
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))
	if configPath != "" { // If configPath is absent from arguments, set default config
		v.SetConfigFile(configPath)
		err := v.ReadInConfig()
		if err != nil {
			logger.Error("failed to parse configuration file", "err", err)
			os.Exit(1)
		}
	}

	opts := jaeger_mongodb.Options{}
	opts.InitFromViper(v)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	m, err := mongo.Connect(ctx, options.Client().
		ApplyURI(opts.Configuration.MongoUrl).
		SetWriteConcern(writeconcern.New(writeconcern.W(1))))

	if err != nil {
		log.Fatal(err)
	}

	collection := m.Database(opts.Configuration.MongoDatabase).Collection(opts.Configuration.MongoCollection)
	readerStorage := jaeger_mongodb.NewMongoReaderStorage(collection)

	createIndexes(ctx, logger, collection, opts)

	defer func() {
		if err = m.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	if opts.Configuration.OtelTracingRatio > 0.0 {
		tp, err := setupTraceExporter(opts.Configuration.OtelExporterEndpoint, opts.Configuration.OtelTracingRatio)
		if err != nil {
			log.Fatal(err)
		}

		// Register our TracerProvider as the global so any imported
		// instrumentation in the future will default to using it.
		otel.SetTracerProvider(tp)
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()

		// Cleanly shutdown and flush telemetry when the application exits.
		defer func(ctx context.Context) {
			// Do not make the application hang when it is shutdown.
			ctx, cancel = context.WithTimeout(ctx, time.Second*5)
			defer cancel()
			if err := tp.Shutdown(ctx); err != nil {
				log.Fatal(err)
			}
		}(ctx)
	}

	plugin := &mongoStorePlugin{
		reader: jaeger_mongodb.NewSpanReader(readerStorage, logger, opts.Configuration.MongoTimeoutDuration),
		writer: jaeger_mongodb.NewSpanWriter(collection, logger),
	}

	grpc.Serve(&shared.PluginServices{
		Store: plugin,
	})

}

func createIndexes(ctx context.Context, logger hclog.Logger, collection *mongo.Collection, opts jaeger_mongodb.Options) {
	ttlIndex := mongo.IndexModel{
		Keys: bson.M{"startTime": 1},
		Options: &options.IndexOptions{
			ExpireAfterSeconds: Int32(int32(opts.Configuration.MongoSpanTTLDuration.Seconds())),
			Name:               String("TTLIndex"),
		},
	}

	serviceNameIndex := mongo.IndexModel{
		Keys: bson.D{
			bson.E{Key: "process.serviceName", Value: 1},
			bson.E{Key: "operationName", Value: 1},
			bson.E{Key: "startTime", Value: -1},
		},
		Options: &options.IndexOptions{
			Name: String("ServiceNameAndOperationsIndex"),
		},
	}

	traceIDIndex := mongo.IndexModel{
		Keys: bson.M{"traceID": 1},
		Options: &options.IndexOptions{
			Name: String("TraceIDIndex"),
		},
	}

	tagsIndex := mongo.IndexModel{
		Keys: bson.D{
			bson.E{Key: "process.ServiceName", Value: 1},
			bson.E{Key: "operationName", Value: 1},
			bson.E{Key: "tags.key", Value: 1},
			bson.E{Key: "tags.value", Value: 1},
			bson.E{Key: "startTime", Value: -1},
		},
		Options: &options.IndexOptions{
			Name: String("TagsIndex"),
		},
	}

	if _, err := collection.Indexes().CreateMany(
		ctx,
		[]mongo.IndexModel{
			ttlIndex,
			serviceNameIndex,
			traceIDIndex,
			tagsIndex,
		},
	); err != nil {
		logger.Error("Could not create indexes:", err)
	}
}

func setupTraceExporter(url string, ratio float64) (*tracesdk.TracerProvider, error) {
	exp, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(url)))
	if err != nil {
		return nil, err
	}

	tp := tracesdk.NewTracerProvider(
		tracesdk.WithBatcher(exp),
		tracesdk.WithSampler(tracesdk.TraceIDRatioBased(ratio)),
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("jaeger-query"),
		)),
	)
	return tp, nil
}

type mongoStorePlugin struct {
	reader *jaeger_mongodb.SpanReader
	writer *jaeger_mongodb.SpanWriter
}

func (s *mongoStorePlugin) DependencyReader() dependencystore.Reader {
	return s.reader
}

func (s *mongoStorePlugin) SpanReader() spanstore.Reader {
	return s.reader
}

func (s *mongoStorePlugin) SpanWriter() spanstore.Writer {
	return s.writer
}

func Int32(i int32) *int32 {
	return &i
}

func String(s string) *string {
	return &s
}
