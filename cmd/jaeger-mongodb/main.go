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

	// Add TTL index to set threshold data expiration
	index_opt := options.Index()
	index_opt.SetExpireAfterSeconds(int32(opts.Configuration.ExpireAfterSeconds))
	ttlIndex := mongo.IndexModel{Keys: bson.M{"startTime": 1}, Options: index_opt}
	serviceNameIndex := mongo.IndexModel{
		Keys: bson.D{
			bson.E{Key: "process.serviceName", Value: 1},
			bson.E{Key: "operationName", Value: 1},
		},
	}
	traceIDIndex := mongo.IndexModel{Keys: bson.M{"traceID": 1}}

	if _, err := collection.Indexes().CreateMany(
		ctx,
		[]mongo.IndexModel{ttlIndex, serviceNameIndex, traceIDIndex},
	); err != nil {
		log.Println("Could not create indices:", err)
	}

	defer func() {
		if err = m.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	plugin := &mongoStorePlugin{
		reader: jaeger_mongodb.NewSpanReader(logger, readerStorage),
		writer: jaeger_mongodb.NewSpanWriter(collection, logger),
	}

	grpc.Serve(&shared.PluginServices{
		Store: plugin,
	})

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
