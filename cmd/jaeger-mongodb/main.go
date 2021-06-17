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
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"

	jager_mongodb "jaeger-mongodb/internal/jager-mongodb"
)

var configPath string
var defaultConfigPath = "run/default-config.yaml"

func main() {
	flag.StringVar(&configPath, "config", "", "A path to the plugin's configuration file")
	flag.Parse()

	logger := hclog.New(&hclog.LoggerOptions{
		Name:       "jaeger-mongodb",
		Level:      hclog.Warn, // Jaeger only captures >= Warn, so don't bother logging below Warn
		JSONFormat: true,
	})

	archiveLogger := hclog.New(&hclog.LoggerOptions{
		Name:       "jaeger-mongodb",
		Level:      hclog.Warn, // Jaeger only captures >= Warn, so don't bother logging below Warn
		JSONFormat: true,
	})

	v := viper.New()
	v.AutomaticEnv()
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))
	if configPath == "" { // If configPath is absent from arguments, set default config
		v.SetConfigFile(defaultConfigPath)
	} else {
		v.SetConfigFile(configPath)
	}

	err := v.ReadInConfig()
	if err != nil {
		logger.Error("failed to parse configuration file", "err", err)
		os.Exit(1)
	}

	opts := jager_mongodb.Options{}
	opts.InitFromViper(v)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	m, err := mongo.Connect(ctx, options.Client().
		ApplyURI(opts.Configuration.MongoUrl).
		SetWriteConcern(writeconcern.New(writeconcern.W(1))))

	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err = m.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()
	collection := m.Database(opts.Configuration.MongoDatabase).Collection(opts.Configuration.MongoCollection)
	archiveCollection := m.Database(opts.Configuration.MongoDatabase).Collection(opts.Configuration.ArchiveCollection)

	plugin := &mongoStorePlugin{
		reader: jager_mongodb.NewSpanReader(collection, logger),
		writer: jager_mongodb.NewSpanWriter(collection, logger),
	}

	archivePlugin := &mongoStorePlugin{
		reader: jager_mongodb.NewArchiveReader(archiveCollection, archiveLogger),
		writer: jager_mongodb.NewSpanWriter(archiveCollection, archiveLogger),
	}

	grpc.Serve(&shared.PluginServices{
		Store:        plugin,
		ArchiveStore: archivePlugin,
	})

}

type mongoStorePlugin struct {
	reader *jager_mongodb.SpanReader
	writer *jager_mongodb.SpanWriter
}

type archiveStorePlugin struct {
	reader *jager_mongodb.ArchiveReader
	writer *jager_mongodb.SpanWriter
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

func (s *mongoStorePlugin) ArchiveSpanReader() spanstore.Reader {
	return s.reader
}

func (s *mongoStorePlugin) ArchiveSpanWriter() spanstore.Writer {
	return s.writer
}
