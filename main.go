package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"reflect"
	"time"

	"webserver/services/kafka"
	"webserver/services/mongo"

	"github.com/gorilla/mux"
	kafka "github.com/segmentio/kafka-go"
)

type textField struct {
	Text string `json:"message"`
}

type infoMessage struct {
	ID      string `json:"id"`
	Message string `json:"message"`
	Count   int32  `json:"count"`
}

func processMessage(message kafka.Message) {
	log.Printf("Message %+v", string(message.Value))
	var m infoMessage
	json.Unmarshal(message.Value, &m)
	log.Printf("Parsed %+v", m)
}

func pollKafka(reader *kafka.Reader, interval time.Duration, messageHandler func(kafka.Message)) {
	for {
		<-time.After(interval)
		log.Printf("polling kafka")
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		message, readErr := reader.ReadMessage(ctx)
		if ctx.Err() != nil {
			log.Printf("Timeout exceed")
			continue
		}
		if readErr != nil {
			log.Printf("Error in kafka %v. Type: %+v", readErr, reflect.TypeOf(readErr))
			return
			// return
		}
		messageHandler(message)
		errCommit := reader.CommitMessages(context.TODO(), message)
		if errCommit != nil {
			log.Printf("Error in commit message: %+v", errCommit)
		}
	}
}

func main() {
	client := mongoService.Connect(0.0)
	defer mongoService.Disconnect(client, 0.0)
	reader := kafkaService.Connect()
	go pollKafka(reader, 2*time.Second, processMessage)
	//
	// log.Printf("GOMAXPROCS: %v", runtime.NumCPU())
	// collection := client.Database("test").Collection("trainers")
	// collection.InsertOne(context.TODO(), textField{"Здарова"})
	r := mux.NewRouter()
	// r.HandleFunc("/", helloWorld).Methods("GET")
	// r.HandleFunc("/info/{billing_id}", checkBillingInfo).Methods("GET")
	// r.HandleFunc("/blocking", blocking).Methods("GET")
	log.Fatal(http.ListenAndServe(":8000", r))
}

func checkBillingInfo(w http.ResponseWriter, r *http.Request) {
	pathParams := mux.Vars(r)["billing_id"]
	log.Printf("Getting info about %v", pathParams)
	json.NewEncoder(w).Encode(textField{"Your info is " + pathParams})
}

func blocking(w http.ResponseWriter, r *http.Request) {
	log.Printf("before sleeping")
	time.Sleep(5000 * time.Millisecond)
	log.Printf("%v msecs later", 5000)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	json.NewEncoder(w).Encode(textField{"Yup"})
}

func nonBlocking(w http.ResponseWriter, r *http.Request) {
	done := make(chan bool)
	log.Printf("before sleeping")
	// make some asyncronys shit
	go func() {
		time.Sleep(5000 * time.Millisecond)
		log.Printf("%v msecs later", 5000)
		done <- true
	}()
	// sync due flag (this will block our goroutine)
	log.Printf("done is %v", <-done)
	json.NewEncoder(w).Encode(textField{"Yup"})
}

func helloWorld(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(textField{"Привет, мир"})
	// Make json from object?
	// json.Marshal(v)
}
