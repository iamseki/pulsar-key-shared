package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/iamseki/pulsar-key-shared/core"
)

func initListeners(client pulsar.Client) {
	channel := make(chan pulsar.ConsumerMessage, 1000)

	for i := 0; i < 5; i++ {
		consumer, err := client.Subscribe(pulsar.ConsumerOptions{
			Topic:            "persistent://public/default/orders",
			SubscriptionName: "my-sub",
			Type:             pulsar.KeyShared,
			MessageChannel:   channel,
			Name:             fmt.Sprintf("Consumer-%v", i),
		})
		if err != nil {
			log.Fatal(err)
		}

		defer consumer.Close()
	}

	log.Println("Starting listener")
	for cm := range channel {
		consumer := cm.Consumer
		msg := cm.Message
		log.Printf("Consuming message name: %v, messageId: %v, subscription: %v, payload: %v, key: %v", consumer.Name(), msg.ID().String(), consumer.Subscription(), string(msg.Payload()), msg.Key())

		order := &core.Order{}

		json.Unmarshal(msg.Payload(), &order)
		log.Printf("message, payload: %v", order)

		// consumer.Ack(msg)

		if order.Status == "DISPATCHED" {
			log.Printf("Nack, msgId: %v", msg.ID())
			consumer.Nack(msg)
			consumer.Seek(msg.ID())
			time.Sleep(1 * time.Second)
		} else {
			consumer.Ack(msg)
		}
	}

}

func main() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	block := make(chan int, 1)

	const MAX_CONSUMERS int = 5
	for i := 0; i < MAX_CONSUMERS; i++ {
		c, err := client.Subscribe(pulsar.ConsumerOptions{
			Topic:            "persistent://public/default/orders",
			SubscriptionName: "my-sub",
			Type:             pulsar.KeyShared,
			Name:             fmt.Sprintf("Consumer-%v", i),
		})
		if err != nil {
			log.Fatal(err)
		}
		defer c.Close()

		go func(consumer pulsar.Consumer) {
			retry := 0
			for msg := range c.Chan() {
				order := &core.Order{}
				json.Unmarshal(msg.Payload(), &order)

				// log.Printf("Consuming message name: %v, messageId: %v, subscription: %v, payload: %v, key: %v", consumer.Name(), msg.ID().String(), consumer.Subscription(), string(msg.Payload()), msg.Key())
				log.Printf("Consuming message key: %v, status: %v, consumer: %v", msg.Key(), order.Status, consumer.Name())

				if order.Status == "DISPATCHED" && retry < 3 {
					log.Printf("Nack, msgId: %v", msg.ID())
					consumer.Nack(msg)
					err := consumer.Seek(msg.ID())
					if err != nil {
						log.Fatalf("err on seek: %v", err)
					}
					// For example purposes the consumer which is reading for specific message will retry at least 3 times
					// Until everything were back to normal and every order will be correctly processed with "order guarantee" in order besides any error that can occur on consumer...
					retry++
					time.Sleep(20 * time.Second)
				} else {
					err := consumer.Ack(msg)
					if err != nil {
						log.Fatalf("Err on ACK msg %v", err)
					}
					log.Printf("Ack => key: %v, status: %v, consumer: %v", msg.Key(), order.Status, consumer.Name())
				}
			}
		}(c)

		time.Sleep(100 * time.Millisecond)
	}

	<-block
}
