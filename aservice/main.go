package main

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"kafka:9092"},
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
