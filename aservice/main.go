package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   "topic-a-to-b",
	})
	defer writer.Close()

	for {
		err := writer.WriteMessages(context.Background(),
			kafka.Message{
				Value: []byte("Hello from Service A !"),
			})
		if err != nil {
			log.Printf("Error sending message: %v", err)
		} else {
			log.Println("Message sent to B")
		}
		time.Sleep(5 * time.Second)
	}
}
