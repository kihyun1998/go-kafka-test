package main

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

func processMessage(msg []byte) []byte {
	return append([]byte("Processed: "), msg...)
}
func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"kafka:9092"},
		Topic:   "topic-a-to-b",
		GroupID: "group-b",
	})
	defer reader.Close()

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"kafka:9092"},
		Topic:   "topic-b-to-c",
	})
	defer writer.Close()

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v", err)
			continue
		}

		log.Printf("Received from A: %s", string(msg.Value))
		processMsg := processMessage(msg.Value)

		err = writer.WriteMessages(context.Background(),
			kafka.Message{
				Value: processMsg,
			})
		if err != nil {
			log.Printf("Error sending message to C: %v", err)
		} else {
			log.Println("Message sent to C")
		}

	}
}
