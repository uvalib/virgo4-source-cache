package main

import (
	"log"

	"github.com/go-redis/redis"
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
)

// maximum number of commands to buffer before flushing
const pipelineCommands = 10

type redisPipeline struct {
	id int
	pipe redis.Pipeliner
	queued int
	limit int
}

func newPipeline (rc *redis.Client, workerID int, pipelineSize int) *redisPipeline {
	rp := redisPipeline{
		id: workerID,
		pipe: rc.TxPipeline(),
		queued: 0,
		limit: pipelineSize,
	}

	return &rp
}

func (rp *redisPipeline) queueRecord(msg awssqs.Message) {
	// trust these values exist for now

	// key
	msgID, _ := msg.GetAttribute("id")
	msgType, _ := msg.GetAttribute("type")
	msgSource, _ := msg.GetAttribute("source")

	// fields
	var fieldMap = map[string]interface{}{
		"type":    msgType,
		"source":  msgSource,
		"payload": string(msg.Payload),
	}

	//log.Printf("queueing id [%s] with type [%s] and source [%s]...", msgID, msgType, msgSource)

	rp.pipe.HMSet(msgID, fieldMap)

	rp.queued++

	if rp.queued >= rp.limit {
		rp.flushRecords()
	}
}

func (rp *redisPipeline) flushRecords() {
	if rp.queued == 0 {
		return
	}

	log.Printf("worker %d flushing %d records...", rp.id, rp.queued)

	_, err := rp.pipe.Exec()

	if err != nil {
		log.Fatal(err)
	}

	rp.queued = 0
}

//
// end of file
//
