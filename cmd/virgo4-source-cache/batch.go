package main

import (
	"fmt"
	"log"
	"math"
	"sort"
	"strings"

	dbx "github.com/go-ozzo/ozzo-dbx"
	_ "github.com/lib/pq"
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
)

const UPSERT_QUERY = `
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

const DELETE_QUERY = `
DELETE
FROM
	{:table}
WHERE
	id = {:id}
`

var upsertQuery string
var deleteQuery string

type batchTransaction struct {
	id          int
	cache       *cacheService
	queued      int
	messages    []cacheMessage
	deleteChan  chan<- []cacheMessage
	upsertQuery string
	deleteQuery string
}

func cleanQuery(query string, table string) string {
	// converts a query to a more compact form
	// maybe it makes a difference to pq?

	q := query

	q = strings.ReplaceAll(q, "{:table}", table)
	q = strings.ReplaceAll(q, "\n", " ")
	q = strings.ReplaceAll(q, "\t", "")
	q = strings.Trim(q, " ")

	return q
}

func newBatchTransaction(id int, cache *cacheService, deleteChan chan<- []cacheMessage) *batchTransaction {
	b := batchTransaction{
		id:          id,
		cache:       cache,
		queued:      0,
		deleteChan:  deleteChan,
		upsertQuery: cleanQuery(UPSERT_QUERY, cache.table),
		deleteQuery: cleanQuery(DELETE_QUERY, cache.table),
	}

	return &b
}

func (b *batchTransaction) queueRecord(msg cacheMessage) {
	b.queued++

	b.messages = append(b.messages, msg)

	if b.queued >= b.cache.size {
		b.flushRecords()
	}
}

func (b *batchTransaction) sortMessages() {
	sort.SliceStable(b.messages, func(i, j int) bool {
		idi, _ := b.messages[i].message.GetAttribute(awssqs.AttributeKeyRecordId)
		idj, _ := b.messages[j].message.GetAttribute(awssqs.AttributeKeyRecordId)
		return idi < idj
	})
}

func (b *batchTransaction) writeMessagesToCache() {
	// sort messages by id in attempt to prevent deadlocks
	b.sortMessages()

	// execute a transaction inline
	// note: commits at the end automatically, or rolls back if error
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
					log.Printf("[cache] worker %d: update execution failed: %s", b.id, err.Error())
					return err
				}

			case awssqs.AttributeValueRecordOperationDelete:

				_, err := dq.Bind(dbx.Params{
					"id": msgID,
				}).Execute()

				if err != nil {
					log.Printf("[cache] worker %d: delete execution failed: %s", b.id, err.Error())
					return err
				}

			default:
				// ignore?
			}
		}

		return nil
	})

	if err != nil {
		log.Fatalf("[cache] worker %d: transaction failed: %s", b.id, err.Error())
	}
}

func countMapToString(countMap map[string]int) string {
	s := []string{}

	for k, v := range countMap {
		s = append(s, fmt.Sprintf("[%s]: %d", k, v))
	}

	sort.Strings(s)

	return strings.Join(s, ", ")
}

func (b *batchTransaction) logBatchSummary() {
	typeCounts := make(map[string]int)
	sourceCounts := make(map[string]int)
	operationCounts := make(map[string]int)

	minPayload := math.MaxInt32
	maxPayload := math.MinInt32

	for _, msg := range b.messages {
		msgType, _ := msg.message.GetAttribute(awssqs.AttributeKeyRecordType)
		msgSource, _ := msg.message.GetAttribute(awssqs.AttributeKeyRecordSource)
		msgOperation, _ := msg.message.GetAttribute(awssqs.AttributeKeyRecordOperation)

		typeCounts[msgType]++
		sourceCounts[msgSource]++
		operationCounts[msgOperation]++

		payloadSize := len(msg.message.Payload)
		if payloadSize > maxPayload {
			maxPayload = payloadSize
		}
		if payloadSize < minPayload {
			minPayload = payloadSize
		}
	}

	typeStr := countMapToString(typeCounts)
	sourceStr := countMapToString(sourceCounts)
	operationStr := countMapToString(operationCounts)

	log.Printf("[cache] worker %d: batch summary:", b.id)
	log.Printf("[cache] worker %d: messages: %d", b.id, len(b.messages))
	log.Printf("[cache] worker %d: payloads: min = %d bytes, max = %d bytes", b.id, minPayload, maxPayload)
	log.Printf("[cache] worker %d: operations: %s", b.id, operationStr)
	log.Printf("[cache] worker %d: types: %s", b.id, typeStr)
	log.Printf("[cache] worker %d: sources: %s", b.id, sourceStr)
}

func (b *batchTransaction) logBatchDetails() {
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

	b.logBatchSummary()

	b.queued = 0

	b.deleteChan <- b.messages

	b.messages = nil
}

//
// end of file
//
