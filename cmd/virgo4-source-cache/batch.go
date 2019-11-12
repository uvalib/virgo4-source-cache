package main

/*
	notes:

	* https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
		- A single call to BatchWriteItem can write up to 16 MB of data, which can comprise as many
		  as 25 put or delete requests. Individual items to be written can be as large as 400 KB.
		- BatchWriteItem cannot update items. To update items, use the UpdateItem action.

	* https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html
		- Your applications must encode binary values in base64-encoded format before sending them to DynamoDB.
		  Upon receipt of these values, DynamoDB decodes the data into an unsigned byte array and uses that as
		  the length of the binary attribute.

	* truncated table definition:

{
    "Table": {
        "AttributeDefinitions": [
            {
                "AttributeName": "id",
                "AttributeType": "S"
            }
        ],
        "KeySchema": [
            {
                "KeyType": "HASH",
                "AttributeName": "id"
            }
        ],
    }
}
*/

import (
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
)

type batchTransaction struct {
	id         int
	cache      *cacheService
	keyMap     map[string]bool
	queued     int
	messages   []cacheMessage
	deleteChan chan<- []cacheMessage
}

func newBatchTransaction(id int, cache *cacheService, deleteChan chan<- []cacheMessage) *batchTransaction {
	b := batchTransaction{
		id:         id,
		cache:      cache,
		keyMap:     make(map[string]bool),
		queued:     0,
		deleteChan: deleteChan,
	}

	return &b
}

func (b *batchTransaction) queueRecord(msg cacheMessage) {
	// check for duplicate keys, and flush current batch if found (BatchWriteItem doesn't allow duplicates)

	msgID, _ := msg.message.GetAttribute(awssqs.AttributeKeyRecordId)

	if b.keyMap[msgID] == true {
		log.Printf("[dynamodb] WARNING: received duplicate key: [%s]", msgID)
		b.flushRecords()
	}

	b.keyMap[msgID] = true

	b.queued++

	b.messages = append(b.messages, msg)

	if b.queued >= b.cache.size {
		b.flushRecords()
	}
}

func (b *batchTransaction) createItemRequest(msg cacheMessage) *dynamodb.WriteRequest {
	// trust these values exist for now

	// key
	msgID, _ := msg.message.GetAttribute(awssqs.AttributeKeyRecordId)
	msgType, _ := msg.message.GetAttribute(awssqs.AttributeKeyRecordType)
	msgSource, _ := msg.message.GetAttribute(awssqs.AttributeKeyRecordSource)
	msgOperation, _ := msg.message.GetAttribute(awssqs.AttributeKeyRecordOperation)

	var req *dynamodb.WriteRequest

	switch msgOperation {
	case awssqs.AttributeValueRecordOperationUpdate:

		//log.Printf("[dynamodb] putting id [%s] for [%s] with type [%s] and source [%s]...", msgID, msgOperation, msgType, msgSource)

		req = &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String(msgID),
					},
					"datatype": {
						S: aws.String(msgType),
					},
					"datasource": {
						S: aws.String(msgSource),
					},
					"payload": {
						B: msg.message.Payload,
					},
				},
			},
		}

	case awssqs.AttributeValueRecordOperationDelete:

		//log.Printf("[dynamodb] queueing id [%s] for [%s]...", msgID, msgOperation)

		req = &dynamodb.WriteRequest{
			DeleteRequest: &dynamodb.DeleteRequest{
				Key: map[string]*dynamodb.AttributeValue{
					"id": {
						S: aws.String(msgID),
					},
				},
			},
		}

	default:
		// ignore?
	}

	return req
}

func (b *batchTransaction) writeMessagesToCache() {
	var writes []*dynamodb.WriteRequest

	for _, msg := range b.messages {
		if write := b.createItemRequest(msg); write != nil {
			writes = append(writes, write)
		}
	}

	req := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			b.cache.table: writes,
		},
	}

	//log.Printf("BatchWriteItemInput: %s", req.GoString())

	_, err := b.cache.handle.BatchWriteItem(req)

	if err != nil {
		log.Fatal(err.Error())
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

	log.Printf("[dynamodb] worker %d flushed %d messages (%0.2f mps)", b.id, flush.count, flush.getRate())

	b.queued = 0

	b.deleteChan <- b.messages

	b.messages = nil

	b.keyMap = make(map[string]bool)
}

//
// end of file
//
