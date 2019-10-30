package main

import (
	"log"
	"time"

	"github.com/go-redis/redis"
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
)

// maximum number of commands to buffer before flushing
const pipelineCommands = 10

type redisPipeline struct {
	id         int
	pipe       redis.Pipeliner
	queued     int
	limit      int
	messages   []awssqs.Message
	deleteChan chan<- []awssqs.Message
}

func newPipeline(rc *redis.Client, workerID int, pipelineSize int, deleteChan chan<- []awssqs.Message) *redisPipeline {
	rp := redisPipeline{
		id:         workerID,
		pipe:       rc.TxPipeline(),
		queued:     0,
		limit:      pipelineSize,
		deleteChan: deleteChan,
	}

	return &rp
}

func (rp *redisPipeline) queueRecord(msg awssqs.Message) {
	// trust these values exist for now

	// key
	msgID, _ := msg.GetAttribute(awssqs.AttributeKeyRecordId)
	msgType, _ := msg.GetAttribute(awssqs.AttributeKeyRecordType)
	msgSource, _ := msg.GetAttribute(awssqs.AttributeKeyRecordSource)
	msgOperation, _ := msg.GetAttribute(awssqs.AttributeKeyRecordOperation)

	switch msgOperation {
	case awssqs.AttributeValueRecordOperationUpdate:

		var fieldMap = map[string]interface{}{
			"type":    msgType,
			"source":  msgSource,
			"payload": string(msg.Payload),
		}

		//log.Printf("[redis] queueing id [%s] for [%s] with type [%s] and source [%s]...", msgID, msgOperation, msgType, msgSource)

		rp.pipe.HMSet(msgID, fieldMap)

	case awssqs.AttributeValueRecordOperationDelete:

		//log.Printf("[redis] queueing id [%s] for [%s]...", msgID, msgOperation)

		rp.pipe.Del(msgID)

	default:
		// ignore?
		return
	}

	rp.queued++

	rp.messages = append(rp.messages, msg)

	if rp.queued >= rp.limit {
		rp.flushRecords()
	}
}

func (rp *redisPipeline) flushRecords() {
	if rp.queued == 0 {
		return
	}

	start := time.Now()
	_, err := rp.pipe.Exec()

	if err != nil {
		log.Fatal(err)
	}

	duration := time.Since(start)
	log.Printf("[redis] worker %d flushed %d records (%0.2f tps)", rp.id, rp.queued, float64(rp.queued)/duration.Seconds())

	rp.queued = 0

	rp.deleteChan <- rp.messages

	rp.messages = nil
}

//
// end of file
//
