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
	WorkerQueueSize   int
	Workers           int
	RedisHost         string
	RedisPort         int
	RedisPass         string
	RedisDB           int
}

func ensureSet(env string) string {
	val, set := os.LookupEnv(env)

	if set == false {
		log.Printf("environment variable not set: [%s]", env)
		os.Exit(1)
	}

	return val
}

func ensureSetAndNonEmpty(env string) string {
	val := ensureSet(env)

	if val == "" {
		log.Printf("environment variable not set: [%s]", env)
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
	cfg.WorkerQueueSize = envToInt("VIRGO4_SOURCE_CACHE_WORKER_QUEUE_SIZE")
	cfg.Workers = envToInt("VIRGO4_SOURCE_CACHE_WORKERS")
	cfg.RedisHost = ensureSetAndNonEmpty("VIRGO4_SOURCE_CACHE_REDIS_HOST")
	cfg.RedisPort = envToInt("VIRGO4_SOURCE_CACHE_REDIS_PORT")
	cfg.RedisPass = ensureSet("VIRGO4_SOURCE_CACHE_REDIS_PASS")
	cfg.RedisDB = envToInt("VIRGO4_SOURCE_CACHE_REDIS_DB")

	log.Printf("[CONFIG] InQueueName       = [%s]", cfg.InQueueName)
	log.Printf("[CONFIG] MessageBucketName = [%s]", cfg.MessageBucketName)
	log.Printf("[CONFIG] PollTimeOut       = [%d]", cfg.PollTimeOut)
	log.Printf("[CONFIG] WorkerQueueSize   = [%d]", cfg.WorkerQueueSize)
	log.Printf("[CONFIG] Workers           = [%d]", cfg.Workers)
	log.Printf("[CONFIG] RedisHost         = [%s]", cfg.RedisHost)
	log.Printf("[CONFIG] RedisPort         = [%d]", cfg.RedisPort)
	log.Printf("[CONFIG] RedisPass         = [REDACTED]")
	log.Printf("[CONFIG] RedisDB           = [%d]", cfg.RedisDB)

	return &cfg
}

//
// end of file
//
