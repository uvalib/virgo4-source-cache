package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/go-redis/redis"
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
)

//
// main entry point
//
func main() {

	log.Printf("===> %s service starting up (version: %s) <===", os.Args[0], Version())

	// Get config params and use them to init service context. Any issues are fatal
	cfg := LoadConfiguration()

	// load our AWS_SQS helper object
	aws, err := awssqs.NewAwsSqs(awssqs.AwsSqsConfig{MessageBucketName: cfg.MessageBucketName})
	if err != nil {
		log.Fatal(err)
	}

	// get the queue handles from the queue name
	inQueueHandle, err := aws.QueueHandle(cfg.InQueueName)
	if err != nil {
		log.Fatal(err)
	}

	// connect to redis instance
	redisHost := fmt.Sprintf("%s:%d", cfg.RedisHost, cfg.RedisPort)
	redisOpts := redis.Options{
		DialTimeout:  time.Duration(cfg.RedisTimeout) * time.Second,
		WriteTimeout: time.Duration(cfg.RedisTimeout) * time.Second,
		Addr:         redisHost,
		DB:           cfg.RedisDB,
		Password:     cfg.RedisPass,
		PoolSize:     cfg.Workers,
	}

	rc := redis.NewClient(&redisOpts)

	// see if the connection is good
	_, err = rc.Ping().Result()
	if err != nil {
		log.Fatal(err)
	}

	// create the message deletion channel and start deleters
	deleteChan := make(chan []awssqs.Message, cfg.DeleteQueueSize)
	for d := 1; d <= cfg.Deleters; d++ {
		go deleter(d, *cfg, aws, inQueueHandle, deleteChan)
	}

	// create the message processing channel and start workers
	processChan := make(chan awssqs.Message, cfg.WorkerQueueSize)
	for w := 1; w <= cfg.Workers; w++ {
		go worker(w, *cfg, rc, processChan, deleteChan)
	}

	total := 0

	for {
		log.Printf("[main] backlog: process = %d, delete = %d", len(processChan), len(deleteChan))

		//log.Printf("[main] waiting for messages...")
		start := time.Now()

		// wait for a batch of messages
		messages, err := aws.BatchMessageGet(inQueueHandle, awssqs.MAX_SQS_BLOCK_COUNT, time.Duration(cfg.PollTimeOut)*time.Second)
		if err != nil {
			log.Fatal(err)
		}

		// did we receive any?
		sz := len(messages)
		if sz != 0 {

			//log.Printf("[main] received %d messages", sz)

			for _, m := range messages {
				processChan <- m
			}

			total = total + sz
			if total%1000 == 0 {
				duration := time.Since(start)
				log.Printf("[main] queued %d records (%0.2f tps)", total, float64(sz)/duration.Seconds())
			}
		} else {
			log.Printf("[main] no messages received")
		}
	}
}

//
// end of file
//
