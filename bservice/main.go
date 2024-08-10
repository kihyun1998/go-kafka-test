package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

type Candle struct {
	Open   float64   `json:"open"`
	High   float64   `json:"high"`
	Low    float64   `json:"low"`
	Close  float64   `json:"close"`
	Volume int       `json:"volume"`
	Time   time.Time `json:"time"`
}

func processMessage() []byte {
	return append([]byte("Processed: "))
}
func main() {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "localhost:9092"
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic:   "topic-a-to-b",
		GroupID: "group-b",
	})
	defer reader.Close()

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   "topic-b-to-c",
	})
	defer writer.Close()

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v", err)
			continue
		}

		var candle Candle
		err = json.Unmarshal(msg.Value, &candle)
		if err != nil {
			log.Printf("Error unmarshaling JSON: %v", err)
			continue
		}
		log.Printf("Received candle data: Open=%.2f, High=%.2f, Low=%.2f, Close=%.2f, Volume=%d, Time=%v",
			candle.Open, candle.High, candle.Low, candle.Close, candle.Volume, candle.Time)

		log.Printf("Received from A: %s", string(msg.Value))
		processMsg := processMessage()

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
