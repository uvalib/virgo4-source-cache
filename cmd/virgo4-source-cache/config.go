package main

import (
	"log"
	"os"
	"strconv"
)

// ServiceConfig defines all of the service configuration parameters
type ServiceConfig struct {
	InQueueName       string
	MessageBucketName string
	PollTimeOut       int64
	Workers           int
	WorkerQueueSize   int
	WorkerFlushTime   int
	Deleters          int
	DeleteQueueSize   int
	PostgresHost      string
	PostgresPort      int
	PostgresUser      string
	PostgresPass      string
	PostgresDatabase  string
	PostgresTable     string
	PostgresBatchSize int
}

func ensureSet(env string) string {
	val, set := os.LookupEnv(env)

	if set == false {
		log.Printf("FATAL: environment variable not set: [%s]", env)
		os.Exit(1)
	}

	return val
}

func ensureSetAndNonEmpty(env string) string {
	val := ensureSet(env)

	if val == "" {
		log.Printf("FATAL: environment variable set but empty: [%s]", env)
		os.Exit(1)
	}

	return val
}

func envToInt(env string) int {
	number := ensureSetAndNonEmpty(env)

	n, err := strconv.Atoi(number)
	if err != nil {
		log.Fatal(err)
	}

	return n
}

// LoadConfiguration will load the service configuration from env/cmdline
// and return a pointer to it. Any failures are fatal.
func LoadConfiguration() *ServiceConfig {

	log.Printf("Loading configuration...")

	var cfg ServiceConfig

	cfg.InQueueName = ensureSetAndNonEmpty("VIRGO4_SOURCE_CACHE_IN_QUEUE")
	cfg.MessageBucketName = ensureSetAndNonEmpty("VIRGO4_SQS_MESSAGE_BUCKET")
	cfg.PollTimeOut = int64(envToInt("VIRGO4_SOURCE_CACHE_POLL_TIMEOUT"))
	cfg.Workers = envToInt("VIRGO4_SOURCE_CACHE_WORKERS")
	cfg.WorkerQueueSize = envToInt("VIRGO4_SOURCE_CACHE_WORKER_QUEUE_SIZE")
	cfg.WorkerFlushTime = envToInt("VIRGO4_SOURCE_CACHE_WORKER_FLUSH_TIME")
	cfg.Deleters = envToInt("VIRGO4_SOURCE_CACHE_DELETERS")
	cfg.DeleteQueueSize = envToInt("VIRGO4_SOURCE_CACHE_DELETE_QUEUE_SIZE")
	cfg.PostgresHost = ensureSetAndNonEmpty("VIRGO4_SOURCE_CACHE_POSTGRES_HOST")
	cfg.PostgresPort = envToInt("VIRGO4_SOURCE_CACHE_POSTGRES_PORT")
	cfg.PostgresUser = ensureSetAndNonEmpty("VIRGO4_SOURCE_CACHE_POSTGRES_USER")
	cfg.PostgresPass = ensureSetAndNonEmpty("VIRGO4_SOURCE_CACHE_POSTGRES_PASS")
	cfg.PostgresDatabase = ensureSetAndNonEmpty("VIRGO4_SOURCE_CACHE_POSTGRES_DATABASE")
	cfg.PostgresTable = ensureSetAndNonEmpty("VIRGO4_SOURCE_CACHE_POSTGRES_TABLE")
	cfg.PostgresBatchSize = envToInt("VIRGO4_SOURCE_CACHE_POSTGRES_BATCH_SIZE")

	log.Printf("[CONFIG] InQueueName       = [%s]", cfg.InQueueName)
	log.Printf("[CONFIG] MessageBucketName = [%s]", cfg.MessageBucketName)
	log.Printf("[CONFIG] PollTimeOut       = [%d]", cfg.PollTimeOut)
	log.Printf("[CONFIG] Workers           = [%d]", cfg.Workers)
	log.Printf("[CONFIG] WorkerQueueSize   = [%d]", cfg.WorkerQueueSize)
	log.Printf("[CONFIG] WorkerFlushTime   = [%d]", cfg.WorkerFlushTime)
	log.Printf("[CONFIG] Deleters          = [%d]", cfg.Deleters)
	log.Printf("[CONFIG] DeleteQueueSize   = [%d]", cfg.DeleteQueueSize)
	log.Printf("[CONFIG] PostgresHost      = [%s]", cfg.PostgresHost)
	log.Printf("[CONFIG] PostgresPort      = [%d]", cfg.PostgresPort)
	log.Printf("[CONFIG] PostgresUser      = [%s]", cfg.PostgresUser)
	log.Printf("[CONFIG] PostgresPass      = [REDACTED]")
	log.Printf("[CONFIG] PostgresDatabase  = [%s]", cfg.PostgresDatabase)
	log.Printf("[CONFIG] PostgresTable     = [%s]", cfg.PostgresTable)
	log.Printf("[CONFIG] PostgresBatchSize = [%d]", cfg.PostgresBatchSize)

	return &cfg
}

//
// end of file
//
