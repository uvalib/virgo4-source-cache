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

	log.Printf("===> %s service starting up <===", os.Args[0])

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
		Addr:     redisHost,
		DB:       cfg.RedisDB,
		Password: cfg.RedisPass,
	}

	rc := redis.NewClient(&redisOpts)

	// see if the connection is good
	_, err = rc.Ping().Result()
	if err != nil {
		log.Fatal(err)
	}

	// create the message processing channel
	processChan := make(chan awssqs.Message, cfg.WorkerQueueSize)

	// start workers here
	for w := 1; w <= cfg.Workers; w++ {
		go worker(w, *cfg, rc, processChan)
	}

	total := 0

	for {

		//log.Printf("Waiting for messages...")
		start := time.Now()

		// wait for a batch of messages
		messages, err := aws.BatchMessageGet(inQueueHandle, awssqs.MAX_SQS_BLOCK_COUNT, time.Duration(cfg.PollTimeOut)*time.Second)
		if err != nil {
			log.Fatal(err)
		}

		// did we receive any?
		sz := len(messages)
		if sz != 0 {

			log.Printf("Received %d messages", sz)

			for _, m := range messages {
				processChan <- m
			}

			// delete them all (might want to only delete ones that were stored successfully)
			opStatus, err := aws.BatchMessageDelete(inQueueHandle, messages)
			if err != nil {
				log.Fatal(err)
			}

			// check the operation results
			for ix, op := range opStatus {
				if op == false {
					log.Printf("WARNING: message %d failed to delete", ix)
				}
			}

			duration := time.Since(start)
			total = total + sz
			log.Printf("Cached %d records (%0.2f tps)  [total records: %d]", sz, float64(sz)/duration.Seconds(), total)
		} else {
			log.Printf("No messages received...")
		}
	}
}

//
// end of file
//
