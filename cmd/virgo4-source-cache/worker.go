package main

import (
	"log"
	"time"

	"github.com/go-redis/redis"
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
)

func worker(id int, cfg ServiceConfig, rc *redis.Client, messageChan <-chan cacheMessage, deleteChan chan<- []cacheMessage) {
	rp := newPipeline(rc, id, cfg.RedisPipelineSize, deleteChan)

	count := uint(0)

	flushAfter := time.Duration(cfg.WorkerFlushTime) * time.Second
	start := time.Now()

	for {
		// process a message or wait...
		select {
		case msg, ok := <-messageChan:
			if ok == false {
				// channel was closed
				log.Printf("[process] worker %d: channel closed; flushing pending cache writes", id)
				rp.flushRecords()
				return
			}

			// new message to process; add it to pipeline

			// queue record; pipeline will self-flush if full
			rp.queueRecord(msg)

			count++

			if count%1000 == 0 {
				duration := time.Since(start)
				log.Printf("[process] worker %d: pipelined %d records (%0.2f tps)", id, count, float64(count)/duration.Seconds())
			}
			break

		case <-time.After(flushAfter):
			rp.flushRecords()
			break
		}
	}

	// should never get here
}

func deleter(id int, cfg ServiceConfig, aws awssqs.AWS_SQS, queue awssqs.QueueHandle, messageChan <-chan []cacheMessage) {
	totalGroupCount := uint(0)
	totalMessageCount := uint(0)

	overallStart := time.Now()

	for {
		msgs, ok := <-messageChan

		if ok == false {
			// channel was closed
			log.Printf("[delete] deleter %d: channel closed", id)
			return
		}

		thisStart := time.Now()

		if err := batchDelete(id, aws, queue, msgs); err != nil {
			log.Fatal(err.Error())
		}

		overallDuration := time.Since(overallStart)
		thisDuration := time.Since(thisStart)

		messageCount := uint(len(msgs))
		totalGroupCount++
		totalMessageCount = totalMessageCount + messageCount

		log.Printf("[delete] deleter %d: deleted group of %d messages (%0.2f tps); overall: %d groups / %d messages (%0.2f tps)",
			id, messageCount, float64(messageCount)/thisDuration.Seconds(),
			totalGroupCount, totalMessageCount, float64(totalMessageCount)/overallDuration.Seconds())
	}

	// should never get here
}

func batchDelete(id int, aws awssqs.AWS_SQS, queue awssqs.QueueHandle, messages []cacheMessage) error {
	// ensure there is work to do
	count := uint(len(messages))
	if count == 0 {
		return nil
	}

	//log.Printf( "About to delete block of %d", count )

	start := time.Now()

	// we do delete in blocks of awssqs.MAX_SQS_BLOCK_COUNT
	fullBlocks := count / awssqs.MAX_SQS_BLOCK_COUNT
	remainder := count % awssqs.MAX_SQS_BLOCK_COUNT

	// go through the inbound messages a 'block' at a time
	for bix := uint(0); bix < fullBlocks; bix++ {

		// calculate slice range
		start := bix * awssqs.MAX_SQS_BLOCK_COUNT
		end := start + awssqs.MAX_SQS_BLOCK_COUNT

		//log.Printf( "Deleting slice [%d:%d]", start, end )

		// and delete them
		err := blockDelete(aws, queue, messages[start:end])
		if err != nil {
			return err
		}
	}

	// handle any remaining
	if remainder != 0 {

		// calculate slice range
		start := fullBlocks * awssqs.MAX_SQS_BLOCK_COUNT
		end := start + remainder

		//log.Printf( "Deleting slice [%d:%d]", start, end )

		// and delete them
		err := blockDelete(aws, queue, messages[start:end])
		if err != nil {
			return err
		}
	}

	duration := time.Since(start)
	log.Printf("[delete] deleter %d: batch delete of %d messages completed in %0.2f seconds", id, count, duration.Seconds())

	return nil
}

func blockDelete(aws awssqs.AWS_SQS, queue awssqs.QueueHandle, messages []cacheMessage) error {
	var msgs []awssqs.Message

	for _, msg := range messages {
		msgs = append(msgs, msg.message)

		duration := time.Since(msg.received).Seconds()

		if duration > 60 {
			msgID, _ := msg.message.GetAttribute(awssqs.AttributeKeyRecordId)
			log.Printf("[delete] WARNING: message %s being deleted after %0.2f seconds", msgID, duration)
		}
	}

	// delete the block
	opStatus, err := aws.BatchMessageDelete(queue, msgs)
	if err != nil {
		if err != awssqs.OneOrMoreOperationsUnsuccessfulError {
			return err
		}
	}

	// did we fail
	if err == awssqs.OneOrMoreOperationsUnsuccessfulError {
		for ix, op := range opStatus {
			if op == false {
				log.Printf("ERROR: message %d failed to delete", ix)
			}
		}
	}

	return nil
}

//
// end of file
//
