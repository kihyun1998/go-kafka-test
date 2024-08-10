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
		candle := Candle{
			Open:   100.0,
			High:   105.0,
			Low:    98.0,
			Close:  103.5,
			Volume: 1000000,
			Time:   time.Now(),
		}

		jsonData, err := json.Marshal(candle)
		if err != nil {
			log.Printf("Error marshaling JSON: %v", err)
			continue
		}

		err = writer.WriteMessages(context.Background(),
			kafka.Message{
				Value: jsonData,
			})
		if err != nil {
			log.Printf("Error sending message: %v", err)
		} else {
			log.Println("Message sent to B")
		}
		time.Sleep(5 * time.Second)
	}
}
