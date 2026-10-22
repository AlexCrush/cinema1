package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type storedEvent struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Timestamp string          `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

type eventResponse struct {
	Status    string      `json:"status"`
	Partition int         `json:"partition"`
	Offset    int64       `json:"offset"`
	Event     storedEvent `json:"event"`
}

func main() {
	brokers := splitBrokers(getenv("KAFKA_BROKERS", "localhost:9092"))
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Timeout: 10 * time.Second}
	topics := map[string]string{
		"movie":   "movie-events",
		"user":    "user-events",
		"payment": "payment-events",
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/events/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]bool{"status": true})
	})
	for kind, topic := range topics {
		go consume(brokers, topic)
		mux.HandleFunc("/api/events/"+kind, publish(client, kind, topic))
	}

	port := getenv("PORT", "8082")
	log.Printf("events service on :%s brokers=%s", port, strings.Join(brokers, ","))
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

func publish(client *kafka.Client, kind, topic string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil || !json.Valid(body) {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}

		ev := storedEvent{
			ID:        fmt.Sprintf("%s-%d", kind, time.Now().UnixNano()),
			Type:      kind,
			Timestamp: time.Now().UTC().Format(time.RFC3339),
			Payload:   json.RawMessage(body),
		}
		raw, err := json.Marshal(ev)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		resp, err := client.Produce(ctx, &kafka.ProduceRequest{
			Addr:         client.Addr,
			Topic:        topic,
			Partition:    0,
			RequiredAcks: kafka.RequireOne,
			Records:      kafka.NewRecordReader(kafka.Record{Key: kafka.NewBytes([]byte(ev.ID)), Value: kafka.NewBytes(raw)}),
		})
		if err != nil {
			log.Printf("produce %s: %v", kind, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if resp == nil || resp.Error != nil {
			err = fmt.Errorf("empty produce response")
			if resp != nil {
				err = resp.Error
			}
			log.Printf("produce %s: %v", kind, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		log.Printf("produced %s partition=0 offset=%d", kind, resp.BaseOffset)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(eventResponse{
			Status:    "success",
			Partition: 0,
			Offset:    resp.BaseOffset,
			Event:     ev,
		})
	}
}

func consume(brokers []string, topic string) {
	for {
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:     brokers,
			Topic:       topic,
			GroupID:     "events-service-" + topic,
			StartOffset: kafka.LastOffset,
			MaxWait:     time.Second,
		})
		log.Printf("consuming %s", topic)
		err := readLoop(reader, topic)
		_ = reader.Close()
		if err == nil {
			return
		}
		log.Printf("consumer %s: %v", topic, err)
		time.Sleep(2 * time.Second)
	}
}

func readLoop(reader *kafka.Reader, topic string) error {
	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			return err
		}
		log.Printf("consumed topic=%s partition=%d offset=%d body=%s", topic, msg.Partition, msg.Offset, msg.Value)
	}
}

func splitBrokers(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
