package jager_mongodb

import (
	"flag"

	"github.com/spf13/viper"
)

const (
	mongoUrl           = "mongo_url"
	mongoDatabase      = "mongo_database"
	mongoCollection    = "mongo_collection"
	expireAfterSeconds = "expire_after_seconds"
)

type Configuration struct {
	MongoUrl           string `yaml:"mongo_url"`
	MongoDatabase      string `yaml:"mongo_database"`
	MongoCollection    string `yaml:"mongo_collection"`
	ExpireAfterSeconds int    `yaml:"expire_after_seconds"`
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
	v.SetDefault(expireAfterSeconds, 1209600)

	opt.Configuration.MongoUrl = v.GetString(mongoUrl)
	opt.Configuration.MongoDatabase = v.GetString(mongoDatabase)
	opt.Configuration.MongoCollection = v.GetString(mongoCollection)
	opt.Configuration.ExpireAfterSeconds = v.GetInt(expireAfterSeconds)
}
