package mongoService

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func Connect(timeout float32) *mongo.Client {
	if timeout == 0.0 {
		timeout = 5.0
	}
	// Create client
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://127.0.0.1:27017"))
	if err != nil {
		log.Fatal(err)
	}

	// Create connect
	ctx, cancel := context.WithTimeout(context.Background(), (time.Duration)(timeout)*time.Second)
	defer cancel()
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Check the connection
	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to MongoDB!")
	return client
}

func Disconnect(client *mongo.Client, timeout float32) {
	if timeout == 0.0 {
		timeout = 5.0
	}
	ctx, cancel := context.WithTimeout(context.Background(), (time.Duration)(timeout)*time.Second)
	defer cancel()
	err := client.Disconnect(ctx)

	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connection to MongoDB closed.")
}
