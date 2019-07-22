package kafkaService

import (
	"context"
	"fmt"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

type KafkaHolder struct {
	Reader *kafka.Reader
}

type timeoutError struct {
}

func IsTimeoutError(t interface{}) bool {
	switch t.(type) {
	case *timeoutError:
		return true
	default:
		return false
	}
}

func (e *timeoutError) Error() string {
	return fmt.Sprintf("Kafka timeout exceed")
}

func (k *KafkaHolder) Connect() {
	k.Reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "billing2",
		GroupID:   "consumer-group-id-1",
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	log.Printf("Connected to Kafka!")
}

func (k *KafkaHolder) CommitMessage(message kafka.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	errCommit := k.Reader.CommitMessages(ctx, message)
	if ctx.Err() != nil {
		log.Printf("Timeout exceed")
		return &timeoutError{}
	}
	return errCommit
}

func (k *KafkaHolder) GetMessage() (kafka.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	message, readErr := k.Reader.ReadMessage(ctx)
	if ctx.Err() != nil {
		return kafka.Message{}, &timeoutError{}
	}
	if readErr != nil {
		return kafka.Message{}, readErr
	}
	return message, nil
}

func (k *KafkaHolder) StartPoll(interval time.Duration, handler func(kafka.Message)) {
	for {
		<-time.After(interval)
		log.Printf("Polling kafka")
		message, err := k.GetMessage()
		if err != nil {
			if IsTimeoutError(err) {
				// Timeout exceed
				continue
			} else {
				log.Printf("Kafka lib error: %+v", err)
				return
			}
		}
		handler(message)
		err = k.CommitMessage(message)
		if err != nil {
			if IsTimeoutError(err) {
				log.Printf("Timeout exceed")
			} else {
				log.Printf("Error in commit message: %+v", err)
				return
			}
		}
	}
}
