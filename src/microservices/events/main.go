package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/IBM/sarama"
)

type Event struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Timestamp time.Time   `json:"timestamp"`
	Payload   interface{} `json:"payload"`
}

type MovieEvent struct {
	MovieID   int     `json:"movie_id"`
	Title     string  `json:"title"`
	Action    string  `json:"action"`
	UserID    int     `json:"user_id,omitempty"`
	Rating    float64 `json:"rating,omitempty"`
	Genres    []string `json:"genres,omitempty"`
	Description string  `json:"description,omitempty"`
}

type UserEvent struct {
	UserID    int    `json:"user_id"`
	Username  string `json:"username,omitempty"`
	Email     string `json:"email,omitempty"`
	Action    string `json:"action"`
	Timestamp time.Time `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentID int     `json:"payment_id"`
	UserID    int     `json:"user_id"`
	Amount    float64 `json:"amount"`
	Status    string  `json:"status"`
	Timestamp time.Time `json:"timestamp"`
	MethodType string  `json:"method_type,omitempty"`
}

type EventResponse struct {
	Status    string `json:"status"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
}

type Error struct {
	Error string `json:"error"`
}

func eventHandler(w http.ResponseWriter, r *http.Request, producer sarama.SyncProducer, topic string, eventType string, eventID int, event interface{}) {
//     if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
//         w.WriteHeader(http.StatusBadRequest)
//         json.NewEncoder(w).Encode(Error{Error: "Invalid request"})
//         return
//     }

    eventData, err := json.Marshal(event)
    if err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        json.NewEncoder(w).Encode(Error{Error: "Internal Server Error"})
        return
    }

    log.Printf("Received event: %s", eventData)

    msg := &sarama.ProducerMessage{
        Topic: topic,
        Value: sarama.StringEncoder(eventData),
    }

    partition, offset, err := producer.SendMessage(msg)
    if err != nil {
        w.WriteHeader(http.StatusInternalServerError)
        json.NewEncoder(w).Encode(Error{Error: "Internal Server Error"})
        return
    }

    w.WriteHeader(http.StatusCreated)
    json.NewEncoder(w).Encode(EventResponse{
        Status:    "success",
        Partition: partition,
        Offset:    offset,
        Event:     Event{ID: fmt.Sprintf("%d", eventID), Type: eventType, Timestamp: time.Now(), Payload: event},
    })
}

func movieEventHandler(w http.ResponseWriter, r *http.Request, producer sarama.SyncProducer, topic string) {
    var event MovieEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
    eventHandler(w, r, producer, topic, "movie", event.MovieID, event)
}

func userEventHandler(w http.ResponseWriter, r *http.Request, producer sarama.SyncProducer, topic string) {
    var event UserEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
    eventHandler(w, r, producer, topic, "user", event.UserID, event)
}

func paymentEventHandler(w http.ResponseWriter, r *http.Request, producer sarama.SyncProducer, topic string) {
    var event PaymentEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
    eventHandler(w, r, producer, topic, "payment", event.PaymentID, event)
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
}

func main() {
    port := getEnv("PORT", "8082")
	broker := getEnv("KAFKA_BROKERS", "kafka:9092")
	movie_topic := getEnv("MOVIE_TOPIC", "movie-events")
	user_topic := getEnv("USER_TOPIC", "user-events")
	payment_topic := getEnv("PAYMENT_TOPIC", "payment-events")

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{broker}, config)
	if err != nil {
		log.Fatalf("Failed to start producer: %s", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalf("Failed to close producer: %s", err)
		}
	}()

	http.HandleFunc("/api/events/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]bool{"status": true})
	})
	http.HandleFunc("/api/events/movie", func(w http.ResponseWriter, r *http.Request) {
        movieEventHandler(w, r, producer, movie_topic)
    })
	http.HandleFunc("/api/events/user", func(w http.ResponseWriter, r *http.Request) {
        userEventHandler(w, r, producer, user_topic)
    })
	http.HandleFunc("/api/events/payment", func(w http.ResponseWriter, r *http.Request) {
        paymentEventHandler(w, r, producer, payment_topic)
    })

	consumer, err := sarama.NewConsumer([]string{broker}, config)
	if err != nil {
		log.Fatal("Failed to start consumer: %s", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatal("Failed to close consumer: %s", err)
		}
	}()

	moviePartitionConsumer, err := consumer.ConsumePartition(movie_topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatal("Failed to start movie partition consumer: %s", err)
	}
	userPartitionConsumer, err := consumer.ConsumePartition(user_topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatal("Failed to start user partition consumer: %s", err)
	}
	paymentPartitionConsumer, err := consumer.ConsumePartition(payment_topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatal("Failed to start payment partition consumer: %s", err)
	}
	defer func() {
		if err := moviePartitionConsumer.Close(); err != nil {
			log.Fatal("Failed to close movie partition consumer: %s", err)
		}
		if err := userPartitionConsumer.Close(); err != nil {
			log.Fatal("Failed to close user partition consumer: %s", err)
		}
		if err := paymentPartitionConsumer.Close(); err != nil {
			log.Fatal("Failed to close payment partition consumer: %s", err)
		}
	}()

	go func() {
		for msg := range moviePartitionConsumer.Messages() {
			log.Printf("Movie event consumed: %s", string(msg.Value))
		}
	}()
	go func() {
		for msg := range userPartitionConsumer.Messages() {
			log.Printf("User event consumed: %s", string(msg.Value))
		}
	}()
	go func() {
		for msg := range paymentPartitionConsumer.Messages() {
			log.Printf("Payment event consumed: %s", string(msg.Value))
		}
	}()

	log.Fatal(http.ListenAndServe(":"+port, nil))
}
