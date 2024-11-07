package main

import (
	"fmt"
	"log"
	"sync"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	// Kafka configuration
	brokers := "localhost:9092"
	topic := "my-topic"

	// Number of partitions/consumers
	numConsumers := 10

	// WaitGroup to wait for all consumers to finish
	var wg sync.WaitGroup
	wg.Add(numConsumers)

	for i := 0; i < numConsumers; i++ {
		go func(partition int) {
			defer wg.Done()
			consumePartition(brokers, topic, partition)
		}(i)
	}

	// Wait for all consumers (goroutines) to finish
	wg.Wait()
}

func consumePartition(brokers, topic string, partition int) {
	// Create a new consumer for a specific partition
	consumer, err := ckafka.NewConsumer(&ckafka.ConfigMap{
		"bootstrap.servers":    brokers,
		"group.id":             fmt.Sprintf("consumer-group-%d", partition),
		"auto.offset.reset":    "earliest",
		"enable.partition.eof": true,
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer consumer.Close()

	// Assign specific partition
	topicPartition := []ckafka.TopicPartition{
		ckafka.TopicPartition{Topic: &topic, Partition: int32(partition), Offset: ckafka.OffsetBeginning},
	}
	err = consumer.Assign(topicPartition)
	if err != nil {
		log.Fatalf("Failed to assign partition %d: %s", partition, err)
	}

	log.Printf("Consumer for partition %d started", partition)

	// Consume messages
	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			if e, ok := err.(ckafka.Error); ok && e.Code() == ckafka.ErrPartitionEOF {
				log.Printf("Reached end of partition %d", partition)
				break
			} else {
				log.Printf("Consumer error: %v (%v)\n", err, msg)
				continue
			}
		}
		log.Printf("Partition %d: Message on %s: %s\n", partition, msg.TopicPartition, string(msg.Value))
	}

	log.Printf("Consumer for partition %d exiting", partition)
}
