package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"time"

	"webserver/services"

	"github.com/gorilla/mux"
	"go.mongodb.org/mongo-driver/mongo"
)

type textField struct {
	Text string `json:"message"`
}

func disconnect(client *mongo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := client.Disconnect(ctx)

	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connection to MongoDB closed.")
}

func main() {
	client := mongoService.Connect(0.0)
	defer mongoService.Disconnect(client, 0.0)
	log.Printf("GOMAXPROCS: %v", runtime.NumCPU())
	collection := client.Database("test").Collection("trainers")
	collection.InsertOne(context.TODO(), textField{"Здарова"})
	r := mux.NewRouter()
	r.HandleFunc("/", helloWorld).Methods("GET")
	r.HandleFunc("/info/{billing_id}", checkBillingInfo).Methods("GET")
	r.HandleFunc("/blocking", blocking).Methods("GET")
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
