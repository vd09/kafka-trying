package main

import (
	"context"
	"fmt"
	"log"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	// Kafka configuration
	brokers := "localhost:9092"
	topic := "my-topic"

	// Create Admin client
	adminClient, err := ckafka.NewAdminClient(&ckafka.ConfigMap{"bootstrap.servers": brokers})
	if err != nil {
		log.Fatalf("Failed to create Admin client: %s", err)
	}
	defer adminClient.Close()

	// Create topic with 10 partitions
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = adminClient.CreateTopics(
		ctx,
		[]ckafka.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     10,
			ReplicationFactor: 1,
		}},
	)
	if err != nil {
		log.Printf("Failed to create topic: %s", err)
	} else {
		log.Printf("Topic %s created successfully", topic)
	}

	// Create a Producer instance
	producer, err := ckafka.NewProducer(&ckafka.ConfigMap{"bootstrap.servers": brokers})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer producer.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *ckafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					log.Printf("Message delivered to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Publish messages at ~50 messages per second
	ticker := time.NewTicker(15 * time.Millisecond) // 1000ms / 20ms = 50 messages per second (5ms publish latency)
	defer ticker.Stop()

	i := 0
	for {
		<-ticker.C
		message := fmt.Sprintf("Message number %d at %s", i, time.Now().Format(time.RFC3339Nano))
		err = producer.Produce(&ckafka.Message{
			TopicPartition: ckafka.TopicPartition{Topic: &topic, Partition: ckafka.PartitionAny},
			Value:          []byte(message),
			Key:            []byte(fmt.Sprintf("Key-%d", i)),
		}, nil)
		if err != nil {
			log.Printf("Failed to produce message: %s\n", err)
		} else {
			log.Printf("Produced message: %s\n", message)
		}
		i++
	}
}
