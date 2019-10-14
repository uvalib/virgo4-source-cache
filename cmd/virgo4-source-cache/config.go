package main

import (
	"flag"
	"log"
)

// ServiceConfig defines all of the service configuration parameters
type ServiceConfig struct {
	InQueueName string
	PollTimeOut int64
	RedisHost   string
	RedisPort   int
	RedisPass   string
	RedisPrefix string
	RedisDB     int
}

// LoadConfiguration will load the service configuration from env/cmdline
// and return a pointer to it. Any failures are fatal.
func LoadConfiguration() *ServiceConfig {

	log.Printf("Loading configuration...")
	var cfg ServiceConfig
	flag.StringVar(&cfg.InQueueName, "inqueue", "", "Inbound queue name")
	flag.Int64Var(&cfg.PollTimeOut, "pollwait", 15, "Poll wait time (in seconds)")
	flag.StringVar(&cfg.RedisHost, "redis_host", "localhost", "Redis host (default localhost)")
	flag.IntVar(&cfg.RedisPort, "redis_port", 6379, "Redis port (default 6379)")
	flag.StringVar(&cfg.RedisPass, "redis_pass", "", "Redis password")
	flag.StringVar(&cfg.RedisPrefix, "redis_prefix", "v4_pools", "Redis key prefix")
	flag.IntVar(&cfg.RedisDB, "redis_db", 0, "Redis database instance")

	flag.Parse()

	log.Printf("[CONFIG] InQueueName          = [%s]", cfg.InQueueName)
	log.Printf("[CONFIG] PollTimeOut          = [%d]", cfg.PollTimeOut)
	log.Printf("[CONFIG] RedisHost            = [%s]", cfg.RedisHost)
	log.Printf("[CONFIG] RedisPort            = [%d]", cfg.RedisPort)
	log.Printf("[CONFIG] RedisPass            = [REDACTED]")
	log.Printf("[CONFIG] RedisPrefix          = [%s]", cfg.RedisPrefix)
	log.Printf("[CONFIG] RedisDB              = [%d]", cfg.RedisDB)

	if cfg.RedisHost == "" || cfg.RedisPrefix == "" {
		flag.Usage()
		log.Fatal("FATAL: Missing redis configuration")
	}

	return &cfg
}

//
// end of file
//
