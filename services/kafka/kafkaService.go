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
	Writer *kafka.Writer
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

func (k *KafkaHolder) ConnectReader() {
	k.Reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "income",
		GroupID:   "consumer-group-id-1",
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	log.Printf("Connected to Kafka!")
}

func (k *KafkaHolder) ConnectWriter() {
	k.Writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "outcome",
		Balancer: &kafka.LeastBytes{},
	})
}

func (k *KafkaHolder) CommitMessage(message kafka.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	errCommit := k.Reader.CommitMessages(ctx, message)
	if ctx.Err() != nil {
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

func (k *KafkaHolder) WriteMessage(key []byte, message []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	kafkaMessage := kafka.Message{
		Key:     key,
		Value:   message,
		Headers: []kafka.Header{{Key: "my-header", Value: []byte("my-value")}},
	}
	writeErr := k.Writer.WriteMessages(ctx, kafkaMessage)
	if ctx.Err() != nil {
		return &timeoutError{}
	}
	if writeErr != nil {
		return writeErr
	}
	return nil
}

func (k *KafkaHolder) StartPoll(interval time.Duration, handler func(key []byte, message []byte) ([]byte, error), exitChannel chan bool) {
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
				break
			}
		}

		response, err := handler(message.Key, message.Value)
		if err != nil {
			log.Printf("Error in handler: %+v", err)
			break
		}

		// use to key of income message as key of answer because we sending to another topic
		err = k.WriteMessage(message.Key, response)
		if err != nil {
			if IsTimeoutError(err) {
				log.Printf("Write timeout exceed")
			} else {
				log.Printf("Error in write message: %+v", err)
				break
			}
		}

		err = k.CommitMessage(message)
		if err != nil {
			if IsTimeoutError(err) {
				log.Printf("Commit timeout exceed")
			} else {
				log.Printf("Error in commit message: %+v", err)
				break
			}
		}
	}
	exitChannel <- true
}
