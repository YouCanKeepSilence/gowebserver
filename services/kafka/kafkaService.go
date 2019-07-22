package kafkaService

import (
	"log"

	kafka "github.com/segmentio/kafka-go"
)

func Connect() *kafka.Reader {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "my-topic",
		GroupID:   "consumer-group-id-1",
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	log.Printf("Connected to Kafka!")
	return r
}
