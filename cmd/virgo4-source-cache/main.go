package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
)

type cacheMessage struct {
	message  awssqs.Message // the message
	received time.Time      // when it was read from the queue
	batchID  string         // label for this batch, for tracing purposes
}

type cacheService struct {
	handle *dynamodb.DynamoDB
	table  string
	size   int
}

//
// main entry point
//
func main() {

	log.Printf("===> %s service starting up (version: %s) <===", os.Args[0], Version())

	// Get config params and use them to init service context. Any issues are fatal
	cfg := LoadConfiguration()

	// load our AWS_SQS helper object
	v4sqs, err := awssqs.NewAwsSqs(awssqs.AwsSqsConfig{MessageBucketName: cfg.MessageBucketName})
	if err != nil {
		log.Fatal(err)
	}

	// get the queue handles from the queue name
	inQueueHandle, err := v4sqs.QueueHandle(cfg.InQueueName)
	if err != nil {
		log.Fatal(err)
	}

	// connect to dynamodb

	db := cacheService{
		handle: dynamodb.New(session.Must(session.NewSession())),
		table:  cfg.DynamoDBTable,
		size:   cfg.DynamoDBBatchSize,
	}

	// create the message deletion channel and start deleters
	deleteChan := make(chan []cacheMessage, cfg.DeleteQueueSize)
	for d := 1; d <= cfg.Deleters; d++ {
		go deleter(d, *cfg, v4sqs, inQueueHandle, deleteChan)
	}

	// create the message processing channel and start workers
	processChan := make(chan cacheMessage, cfg.WorkerQueueSize)
	for w := 1; w <= cfg.Workers; w++ {
		go worker(w, *cfg, &db, processChan, deleteChan)
	}

	batch := newRate()
	overall := newRate()

	var batchID string

	showBacklog := false

	pollTimeout := time.Duration(cfg.PollTimeOut) * time.Second

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
		messages, err := v4sqs.BatchMessageGet(inQueueHandle, awssqs.MAX_SQS_BLOCK_COUNT, pollTimeout)
		if err != nil {
			log.Fatal(err)
		}

		received := time.Now()

		// did we receive any?
		sz := len(messages)
		if sz > 0 {
			//log.Printf("[main] received %d messages", sz)

			// tracking a new batch?  (groups of messages received close together)
			if batch.count == 0 {
				batch.setStart(received)

				batchID = fmt.Sprintf("%d%02d%02d%02d%02d%02d",
					batch.start.Year(), batch.start.Month(), batch.start.Day(),
					batch.start.Hour(), batch.start.Minute(), batch.start.Second())

				log.Printf("[main] batch: [%s] tracking new batch", batchID)
			}

			for _, m := range messages {
				c := cacheMessage{
					message:  m,
					received: received,
					batchID:  batchID,
				}

				processChan <- c

				batch.incrementCount()
				overall.incrementCount()

				// show batch totals periodically, along with overall timings
				if batch.count%1000 == 0 {
					log.Printf("[main] batch: [%s] queued %d messages (%0.2f mps)", batchID, batch.count, batch.getCurrentRate())
				}

				// show overall totals periodically.  timings don't really make sense here
				if overall.count%1000 == 0 {
					log.Printf("[main] overall: queued %d messages", overall.count)
					showBacklog = true
				}
			}

			batch.setStopNow()
		} else {
			// if the end of a batch, show totals and timing (if we haven't already)
			if batch.count > 0 && batch.count%1000 != 0 {
				log.Printf("[main] batch: [%s] queued %d messages (%0.2f mps)", batchID, batch.count, batch.getRate())
			}

			log.Printf("[main] no messages received")
			batch = newRate()
			showBacklog = true
		}
	}
}

//
// end of file
//
