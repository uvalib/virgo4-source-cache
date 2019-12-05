package main

import (
	"fmt"
	"log"
	"os"
	"time"

	dbx "github.com/go-ozzo/ozzo-dbx"
	_ "github.com/lib/pq"
	"github.com/rs/xid"
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
)

type cacheMessage struct {
	message  awssqs.Message // the message
	received time.Time      // when it was read from the queue
	batchID  string         // label for this batch, for tracing purposes
}

type cacheService struct {
	handle *dbx.DB
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

	log.Printf("[main] initializing SQS...")
	// load our AWS_SQS helper object
	v4sqs, err := awssqs.NewAwsSqs(awssqs.AwsSqsConfig{MessageBucketName: cfg.MessageBucketName})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("[main] getting queue handle...")
	// get the queue handles from the queue name
	inQueueHandle, err := v4sqs.QueueHandle(cfg.InQueueName)
	if err != nil {
		log.Fatal(err)
	}

	// connect to database
	log.Printf("[main] connecting to postgres...")

	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%d connect_timeout=%d sslmode=disable",
		cfg.PostgresUser, cfg.PostgresPass, cfg.PostgresDatabase, cfg.PostgresHost, cfg.PostgresPort, 30)

	db, err := dbx.MustOpen("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	//db.LogFunc = log.Printf

	cache := cacheService{
		handle: db,
		table:  cfg.PostgresTable,
		size:   cfg.PostgresBatchSize,
	}

	log.Printf("[main] starting deleters...")
	// create the message deletion channel and start deleters
	deleteChan := make(chan []cacheMessage, cfg.DeleteQueueSize)
	for d := 1; d <= cfg.Deleters; d++ {
		go deleter(d, *cfg, v4sqs, inQueueHandle, deleteChan)
	}

	log.Printf("[main] starting workers...")
	// create the message processing channel and start workers
	processChan := make(chan cacheMessage, cfg.WorkerQueueSize)
	for w := 1; w <= cfg.Workers; w++ {
		go worker(w, *cfg, &cache, processChan, deleteChan)
	}

	batch := newRate()
	overall := newRate()

	var batchID string

	showBacklog := false

	pollTimeout := time.Duration(cfg.PollTimeOut) * time.Second

	log.Printf("[main] starting main polling loop...")

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

				guid := xid.New()
				batchID = guid.String()

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
			if batch.count > 0 {
				if batch.count%1000 != 0 {
					log.Printf("[main] batch: [%s] queued %d messages (%0.2f mps)", batchID, batch.count, batch.getRate())
				}
				log.Printf("[main] overall: queued %d messages", overall.count)
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
