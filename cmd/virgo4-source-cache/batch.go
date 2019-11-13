package main

import (
	"log"
	"strings"

	dbx "github.com/go-ozzo/ozzo-dbx"
	_ "github.com/lib/pq"
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
)

const UPSERT = `
INSERT
INTO
	{:table}
		(id, type, source, payload, created_at, updated_at)
VALUES
	({:id}, {:type}, {:source}, {:payload}, now(), now())
ON CONFLICT
	(id)
DO
	UPDATE SET
		(type, source, payload, updated_at)
			= (EXCLUDED.type, EXCLUDED.source, EXCLUDED.payload, EXCLUDED.updated_at)
`

const DELETE = `
DELETE
FROM
	{:table}
WHERE
	id = {:id}
`

type batchTransaction struct {
	id          int
	cache       *cacheService
	keyMap      map[string]bool
	queued      int
	messages    []cacheMessage
	deleteChan  chan<- []cacheMessage
	upsertQuery string
	deleteQuery string
}

func newBatchTransaction(id int, cache *cacheService, deleteChan chan<- []cacheMessage) *batchTransaction {
	b := batchTransaction{
		id:          id,
		cache:       cache,
		keyMap:      make(map[string]bool),
		queued:      0,
		deleteChan:  deleteChan,
		upsertQuery: strings.Trim(strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(UPSERT, "{:table}", cache.table), "\n", " "), "\t", ""), " "),
		deleteQuery: strings.Trim(strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(DELETE, "{:table}", cache.table), "\n", " "), "\t", ""), " "),
	}

	return &b
}

func (b *batchTransaction) queueRecord(msg cacheMessage) {
	// check for duplicate keys, and flush current batch if found (BatchWriteItem doesn't allow duplicates)
	// FIXME: is this still needed outside of dynamodb?  assuming no, but let's log dups anyway

	msgID, _ := msg.message.GetAttribute(awssqs.AttributeKeyRecordId)

	if b.keyMap[msgID] == true {
		log.Printf("[cache] worker %d: WARNING: received duplicate key: [%s]", b.id, msgID)
		//b.flushRecords()
	}

	b.keyMap[msgID] = true

	b.queued++

	b.messages = append(b.messages, msg)

	if b.queued >= b.cache.size {
		b.flushRecords()
	}
}

func (b *batchTransaction) writeMessagesToCache() {
	// execute a transaction
	err := b.cache.handle.Transactional(func(tx *dbx.Tx) error {

		uq := tx.NewQuery(b.upsertQuery).Prepare()
		dq := tx.NewQuery(b.deleteQuery).Prepare()

		// execute statements within the transaction
		for _, msg := range b.messages {
			msgID, _ := msg.message.GetAttribute(awssqs.AttributeKeyRecordId)
			msgType, _ := msg.message.GetAttribute(awssqs.AttributeKeyRecordType)
			msgSource, _ := msg.message.GetAttribute(awssqs.AttributeKeyRecordSource)
			msgOperation, _ := msg.message.GetAttribute(awssqs.AttributeKeyRecordOperation)

			switch msgOperation {
			case awssqs.AttributeValueRecordOperationUpdate:
				// ozzo-dbx pgsql Upsert isn't selective on the "conflict update" clause, so we must specify it ourselves
				_, err := uq.Bind(dbx.Params{
					"id":      msgID,
					"type":    msgType,
					"source":  msgSource,
					"payload": msg.message.Payload,
				}).Execute()

				if err != nil {
					log.Printf("[cache] update execution failed: %s", err.Error())
					return err
				}

				//log.Printf("[cache] worker %d: putting id [%s] for [%s] with type [%s] and source [%s]...", b.id, msgID, msgOperation, msgType, msgSource)

			case awssqs.AttributeValueRecordOperationDelete:

				_, err := dq.Bind(dbx.Params{
					"id": msgID,
				}).Execute()

				if err != nil {
					log.Printf("[cache] delete execution failed: %s", err.Error())
					return err
				}

				//log.Printf("[cache] worker %d: queueing id [%s] for [%s]...", b.id, msgID, msgOperation)

			default:
				// ignore?
			}
		}

		return nil
	})

	if err != nil {
		log.Fatal(err.Error())
	}
}

func (b *batchTransaction) logBatchInfo() {
	log.Printf("[cache] worker %d: batch info: %d messages:", b.id, len(b.messages))

	for i, msg := range b.messages {
		msgID, _ := msg.message.GetAttribute(awssqs.AttributeKeyRecordId)
		msgType, _ := msg.message.GetAttribute(awssqs.AttributeKeyRecordType)
		msgSource, _ := msg.message.GetAttribute(awssqs.AttributeKeyRecordSource)
		msgOperation, _ := msg.message.GetAttribute(awssqs.AttributeKeyRecordOperation)

		log.Printf("[cache] worker %d: message %2d: id = [%s]  type = [%s]  source = [%s]  operation = [%s]  len(payload) = %d",
			b.id, i, msgID, msgType, msgSource, msgOperation, len(msg.message.Payload))
	}
}

func (b *batchTransaction) flushRecords() {
	if b.queued == 0 {
		return
	}

	flush := newRate()
	flush.setCount(int64(b.queued))

	b.writeMessagesToCache()

	flush.setStopNow()

	log.Printf("[cache] worker %d: flushed %d messages (%0.2f mps)", b.id, flush.count, flush.getRate())

	b.queued = 0

	b.deleteChan <- b.messages

	b.messages = nil

	b.keyMap = make(map[string]bool)
}

//
// end of file
//
