package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/go-redis/redis"
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
)

type cacheMessage struct {
	message  awssqs.Message // the message
	received time.Time      // when it was read from the queue
}

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
	deleteChan := make(chan []cacheMessage, cfg.DeleteQueueSize)
	for d := 1; d <= cfg.Deleters; d++ {
		go deleter(d, *cfg, aws, inQueueHandle, deleteChan)
	}

	// create the message processing channel and start workers
	processChan := make(chan cacheMessage, cfg.WorkerQueueSize)
	for w := 1; w <= cfg.Workers; w++ {
		go worker(w, *cfg, rc, processChan, deleteChan)
	}

	batchTotal := 0
	overallTotal := 0

	showBacklog := false

	var batchStart time.Time

	for {
		if showBacklog == true {
			processBacklog := len(processChan)
			deleteBacklog := len(deleteChan)
			if processBacklog > 0 || deleteBacklog > 0 {
				log.Printf("[main] backlog: process = %d, delete = %d", len(processChan), len(deleteChan))
			}
			showBacklog = false
		}

		// wait for a batch of messages
		messages, err := aws.BatchMessageGet(inQueueHandle, awssqs.MAX_SQS_BLOCK_COUNT, time.Duration(cfg.PollTimeOut)*time.Second)
		if err != nil {
			log.Fatal(err)
		}

		received := time.Now()

		// did we receive any?
		sz := len(messages)
		if sz > 0 {
			//log.Printf("[main] received %d messages", sz)

			// tracking a new batch?  (groups of messages received close together)
			if batchTotal == 0 {
				batchStart = received
				log.Printf("[main] batch: tracking new batch")
			}

			for _, m := range messages {
				c := cacheMessage{
					message:  m,
					received: received,
				}

				processChan <- c

				batchTotal++
				overallTotal++

				// show batch totals periodically, along with overall timings
				if batchTotal%1000 == 0 {
					duration := time.Since(batchStart)
					log.Printf("[main] batch: queued %d messages (%0.2f mps)", batchTotal, float64(sz)/duration.Seconds())
				}

				// show overall totals periodically.  timings don't really make sense here
				if overallTotal%1000 == 0 {
					log.Printf("[main] overall: queued %d messages", overallTotal)
					showBacklog = true
				}
			}
		} else {
			// if the end of a batch, show totals and timing (if we haven't already)
			if batchTotal > 0 && batchTotal%1000 != 0 {
				duration := time.Since(batchStart)
				log.Printf("[main] batch: queued %d messages (%0.2f mps)", batchTotal, float64(sz)/duration.Seconds())
			}

			log.Printf("[main] no messages received")
			batchTotal = 0
			showBacklog = true
		}
	}
}

//
// end of file
//
