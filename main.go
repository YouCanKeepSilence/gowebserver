package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"webserver/services/kafka"
	"webserver/services/mongo"

	"github.com/gorilla/mux"
	uuid "github.com/satori/go.uuid"
)

type textField struct {
	Text string `json:"message"`
}

type infoMessage struct {
	ID      string `json:"id"`
	Message string `json:"message"`
	Count   int32  `json:"count"`
}

type itemStructure struct {
	ID    string  `json:"id"`
	Type  float64 `json:"type"`
	Count float64 `json:"count"`
}

type firstMessageRequest struct {
	ID          string          `json:"id"`
	Items       [][]interface{} `json:"items"`
	ParsedItems []itemStructure `json:"parsedItems"`
}

type firstMessageResponse struct {
	ID     string `json:"id"`
	Status bool   `json:"ok"`
}

func processFirstMessage(key []byte, message []byte) ([]byte, error) {
	var m firstMessageRequest
	err := json.Unmarshal(message, &m)
	if err != nil {
		log.Printf("json parse error %#v", err)
		return []byte{}, err
	}
	for _, inter := range m.Items {
		m.ParsedItems = append(m.ParsedItems, itemStructure{inter[0].(string), inter[1].(float64), inter[2].(float64)})
	}

	log.Printf("Parsed %#v", m)
	ans := firstMessageResponse{ID: m.ID, Status: true}
	answer, err := json.Marshal(ans)
	if err != nil {
		log.Printf("json parse error %#v", err)
		return []byte{}, err
	}
	return answer, nil
}

func processMessage(key []byte, message []byte) ([]byte, error) {
	log.Printf("Message %#v", string(message))
	var m infoMessage
	err := json.Unmarshal(message, &m)
	if err != nil {
		log.Printf("json parse error %#v", err)
		return []byte{}, err
	}
	log.Printf("Parsed %#v", m)
	answerStr := fmt.Sprintf("Answer to kafka_id: %v, message_id: %v. Count was: %d", string(key), m.ID, m.Count)
	ID, err := uuid.NewV4()
	if err != nil {
		log.Printf("Error in generate uuid: %#v", err)
		return []byte{}, err
	}
	ans := infoMessage{ID: ID.String(), Message: answerStr}
	answer, err := json.Marshal(ans)
	if err != nil {
		log.Printf("json parse error %#v", err)
		return []byte{}, err
	}
	return answer, nil
}

func main() {
	client := mongoService.Connect(0.0)
	defer mongoService.Disconnect(client, 0.0)
	holder := kafkaService.KafkaHolder{}
	holder.ConnectReader()
	holder.ConnectWriter()
	defer holder.Reader.Close()
	defer holder.Writer.Close()
	exitChannel := make(chan bool)
	go holder.StartPoll(1*time.Second, processFirstMessage, exitChannel)
	// waiting while true come to exit channel (lock main function)
	<-exitChannel
	// r := mux.NewRouter()
	// r.HandleFunc("/", helloWorld).Methods("GET")
	// r.HandleFunc("/info/{billing_id}", checkBillingInfo).Methods("GET")
	// r.HandleFunc("/blocking", blocking).Methods("GET")
	// log.Fatal(http.ListenAndServe(":8000", r))
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
}
