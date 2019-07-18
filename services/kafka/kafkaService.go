package kafkaService

import (
	kafka "github.com/segmentio/kafka-go"
)

func Connect() *kafka.Reader {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "TODOme",
		// GroupID:   "consumer-group-id-3",
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	return r
}
