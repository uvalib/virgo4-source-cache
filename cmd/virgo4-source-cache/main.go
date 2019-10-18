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

	log.Printf("===> %s service staring up (version: %s) <===", os.Args[0], Version())

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

	// create the message deletion channel
	// FIXME: determine a good buffer size for this channel, which will be determined by:
	// * how many delete workers
	// * how fast the delete workers can delete messages
	deleteChan := make(chan []awssqs.Message, cfg.WorkerQueueSize)

	// create the message processing channel
	processChan := make(chan awssqs.Message, cfg.WorkerQueueSize)

	// start workers here
	for w := 1; w <= cfg.Workers; w++ {
		go deleter(w, *cfg, aws, inQueueHandle, deleteChan)
		go worker(w, *cfg, rc, processChan, deleteChan)
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

			//log.Printf("Received %d messages", sz)

			for _, m := range messages {
				processChan <- m
			}

			total = total + sz
			if total%1000 == 0 {
				duration := time.Since(start)
				log.Printf("queued %d records (%0.2f tps)", total, float64(sz)/duration.Seconds())
			}
		} else {
			log.Printf("No messages received...")
		}
	}
}

//
// end of file
//
