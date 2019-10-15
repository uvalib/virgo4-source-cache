package main

import (
	"log"

	"github.com/go-redis/redis"
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
)

func worker(id int, config ServiceConfig, rc *redis.Client, messages <-chan awssqs.Message) {
	count := uint(0)

	for {
		msg, more := <-messages

		if more == false {
			return
		}

		// process this message
		cacheRecord(rc, msg)

		count++

		log.Printf("worker %d processed %d messages", id, count)
	}

	// should never get here
}

func cacheRecord(rc *redis.Client, m awssqs.Message) {
	// trust these values exist for now

	// key
	mID, _ := m.GetAttribute("id")
	mType, _ := m.GetAttribute("type")
	mSource, _ := m.GetAttribute("source")

	// fields
	var fieldMap = map[string]interface{}{
		"type":    mType,
		"source":  mSource,
		"payload": string(m.Payload),
	}

	//log.Printf("storing id [%s] with type [%s] and source [%s]...", mID, mType, mSource)

	err := rc.HMSet(mID, fieldMap).Err()

	if err != nil {
		log.Fatal(err)
	}
}

//
// end of file
//
