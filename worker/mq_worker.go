package worker

import (
	"encoding/json"
	"fmt"
	"github.com/sa7mon/s3scanner/bucket"
	"github.com/sa7mon/s3scanner/db"
	"github.com/sa7mon/s3scanner/mq"
	"github.com/sa7mon/s3scanner/provider"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"os"
	"sync"
)

func FailOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

// MQConfig groups the shared configuration used by every WorkMQ consumer.
type MQConfig struct {
	Conn        *amqp.Connection
	Provider    provider.StorageProvider
	Queue       string
	Threads     int
	DoEnumerate bool
	WriteToDB   bool
}

// msgOutcome tells the consumer loop how to proceed after handling a message.
type msgOutcome int

const (
	// nextMessage means the message is fully handled; continue with the next one.
	nextMessage msgOutcome = iota
	// reconnect means the channel is no longer usable; break out to re-establish it.
	reconnect
	// stopWorker means the worker should exit (used by the single-shot test mode).
	stopWorker
)

func WorkMQ(threadID int, wg *sync.WaitGroup, cfg MQConfig) {
	_, once := os.LookupEnv("TEST_MQ") // If we're being tested, exit after one bucket is scanned
	defer wg.Done()

	// Wrap the whole thing in a for (while) loop so if the mq server kills the channel, we start it up again
	for {
		ch, chErr := mq.Connect(cfg.Conn, cfg.Queue, cfg.Threads, threadID)
		if chErr != nil {
			FailOnError(chErr, "couldn't connect to message queue")
		}

		consumer := fmt.Sprintf("%s_%v", cfg.Queue, threadID)
		msgs, consumeErr := ch.Consume(cfg.Queue, consumer, false, false, false, false, nil)
		if consumeErr != nil {
			log.Error(fmt.Errorf("failed to register a consumer: %w", consumeErr))
			return
		}

		if consumeMessages(msgs, cfg, once) == stopWorker {
			return
		}
	}
}

// consumeMessages processes each delivery until the channel closes or a handler
// signals that the worker should reconnect or stop.
func consumeMessages(msgs <-chan amqp.Delivery, cfg MQConfig, once bool) msgOutcome {
	for j := range msgs {
		switch handleMessage(j, cfg, once) {
		case reconnect:
			return reconnect
		case stopWorker:
			return stopWorker
		}
	}
	return reconnect
}

// handleMessage scans a single bucket delivery and acknowledges or rejects it.
func handleMessage(j amqp.Delivery, cfg MQConfig, once bool) msgOutcome {
	bucketToScan := bucket.Bucket{}
	if unmarshalErr := json.Unmarshal(j.Body, &bucketToScan); unmarshalErr != nil {
		log.Error(unmarshalErr)
	}

	if !bucket.IsValidS3BucketName(bucketToScan.Name) {
		log.Info(fmt.Sprintf("invalid   | %s", bucketToScan.Name))
		FailOnError(j.Ack(false), "failed to ack")
		return nextMessage
	}

	b, existsErr := cfg.Provider.BucketExists(&bucketToScan)
	if existsErr != nil {
		log.WithFields(log.Fields{"bucket": b.Name, "step": "checkExists"}).Error(existsErr)
		FailOnError(j.Reject(false), "failed to reject")
	}
	if b.Exists == bucket.BucketNotExist {
		// ack the message and skip to the next
		log.Infof("not_exist | %s", b.Name)
		FailOnError(j.Ack(false), "failed to ack")
		return nextMessage
	}

	if scanErr := cfg.Provider.Scan(b, false); scanErr != nil {
		log.WithFields(log.Fields{"bucket": b}).Error(scanErr)
		FailOnError(j.Reject(false), "failed to reject")
		return nextMessage
	}

	if cfg.DoEnumerate && !enumerateBucket(j, b, cfg) {
		return nextMessage
	}

	PrintResult(&bucketToScan, false)
	if ackErr := j.Ack(false); ackErr != nil {
		// Acknowledge mq message. May fail if we've taken too long and the server has closed the channel.
		// If it has, we reconnect and start at the top of the outer for-loop again which re-establishes a
		// new channel.
		log.WithFields(log.Fields{"bucket": b}).Error(ackErr)
		return reconnect
	}

	storeBucket(&bucketToScan, cfg)
	if once {
		return stopWorker
	}
	return nextMessage
}

// enumerateBucket enumerates an existing bucket's objects when it is publicly
// readable. It reports whether the caller should continue with the rest of the
// scan pipeline for this delivery.
func enumerateBucket(j amqp.Delivery, b *bucket.Bucket, cfg MQConfig) bool {
	if b.PermAllUsersRead != bucket.PermissionAllowed {
		PrintResult(b, false)
		FailOnError(j.Ack(false), "failed to ack")
		storeBucket(b, cfg)
		return false
	}

	log.WithFields(log.Fields{"method": "main.mqwork()",
		"bucket_name": b.Name, "region": b.Region}).Debugf("enumerating objects...")

	if enumErr := cfg.Provider.Enumerate(b); enumErr != nil {
		log.Errorf("Error enumerating bucket '%s': %v\nEnumerated objects: %v", b.Name, enumErr, len(b.Objects))
		FailOnError(j.Reject(false), "failed to reject")
	}
	return true
}

// storeBucket persists a scanned bucket when database writes are enabled.
func storeBucket(b *bucket.Bucket, cfg MQConfig) {
	if !cfg.WriteToDB {
		return
	}
	if dbErr := db.StoreBucket(b); dbErr != nil {
		log.Error(dbErr)
	}
}
