package main

import (
	"log"
	"os"
	"time"

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
	aws, err := awssqs.NewAwsSqs(awssqs.AwsSqsConfig{})
	if err != nil {
		log.Fatal(err)
	}

	// get the queue handles from the queue name
	inQueueHandle, err := aws.QueueHandle(cfg.InQueueName)
	if err != nil {
		log.Fatal(err)
	}

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

			//log.Printf("Received %d messages", len( result.Messages ) )

			for _, m := range messages {
				cachePayload(m.Payload)
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
			log.Printf("Cached %d records (%0.2f tps)", sz, float64(sz)/duration.Seconds())

		} else {
			log.Printf("No messages received...")
		}
	}
}

func cachePayload(body []byte) error {
	// store it to redis

	return nil
}

//
// end of file
//
