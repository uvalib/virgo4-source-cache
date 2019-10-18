package main

import (
	"log"
	"time"

	"github.com/go-redis/redis"
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
)

func worker(id int, cfg ServiceConfig, rc *redis.Client, messages <-chan awssqs.Message) {
	rp := newPipeline(rc, id, cfg.RedisPipelineSize)

	count := uint(0)

	flushAfter := time.Duration(cfg.WorkerFlushTime) * time.Second
	start := time.Now()

	for {
		// process a message or wait...
		select {
		case msg, more := <-messages:
			if more == false {
				rp.flushRecords()

				// reset the start time when we have no more records
				start = time.Now()
				break
			}

			// new message to process; add it to pipeline

			// queue record; pipeline will self-flush if full
			rp.queueRecord(msg)

			count++

			if count % 1000 == 0 {
				duration := time.Since(start)
				log.Printf("worker %d processed %d records (%0.2f tps)", id, count, float64(count)/duration.Seconds())
			}
			break

		case <-time.After(flushAfter):
			rp.flushRecords()
			break
		}
	}

	// should never get here
}

//
// end of file
//
