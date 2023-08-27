package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/iamseki/pulsar-key-shared/core"
)

func main() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "persistent://public/default/orders",
	})
	if err != nil {
		log.Fatal(err)
	}

	defer producer.Close()

	log.Println("Starting producer")

	order := &core.Order{
		ID:                 "1",
		Status:             "PAID",
		DestinationAddress: "Rua dos bobos",
		Value:              23.5,
		CreatedAt:          time.Now().String(),
	}

	payload, err := json.Marshal(order)
	if err != nil {
		log.Fatal(err)
	}

	producer.Send(context.TODO(), &pulsar.ProducerMessage{
		Key:     order.ID,
		Payload: payload,
	})

	order.Status = "READY_FOR_DISPATCHED"
	payload, err = json.Marshal(order)
	if err != nil {
		log.Fatal(err)
	}

	producer.Send(context.TODO(), &pulsar.ProducerMessage{
		Key:     order.ID,
		Payload: payload,
	})
	log.Printf("Sent order notification: %v", order)

	order.Status = "DISPATCHED"
	payload, err = json.Marshal(order)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Sent order notification: %v", order)

	producer.Send(context.TODO(), &pulsar.ProducerMessage{
		Key:     order.ID,
		Payload: payload,
	})
	log.Printf("Sent order notification: %v", order)

	order.Status = "DELIVERED"
	payload, err = json.Marshal(order)
	if err != nil {
		log.Fatal(err)
	}

	producer.Send(context.TODO(), &pulsar.ProducerMessage{
		Key:     order.ID,
		Payload: payload,
	})
	log.Printf("Sent order notification: %v", order)

	order.Status = "PAID"
	order.ID = "2"
	payload, err = json.Marshal(order)
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(5 * time.Second)
	producer.Send(context.TODO(), &pulsar.ProducerMessage{
		Key:     order.ID,
		Payload: payload,
	})
	log.Printf("Sent order notification: %v", order)

	order.Status = "DELIVERED"
	payload, err = json.Marshal(order)
	if err != nil {
		log.Fatal(err)
	}
	producer.Send(context.TODO(), &pulsar.ProducerMessage{
		Key:     order.ID,
		Payload: payload,
	})
	log.Printf("Sent order notification: %v", order)

	order.Status = "PAID"
	order.ID = "3"
	payload, err = json.Marshal(order)
	if err != nil {
		log.Fatal(err)
	}
	producer.Send(context.TODO(), &pulsar.ProducerMessage{
		Key:     order.ID,
		Payload: payload,
	})
	log.Printf("Sent order notification: %v", order)

}
