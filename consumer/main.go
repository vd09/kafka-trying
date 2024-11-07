package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	// Kafka configuration
	brokers := "localhost:9092"
	topic := "my-topic"

	// Number of partitions/consumers
	numPartitions := 10
	numWorkersPerPartition := 5

	var wg sync.WaitGroup
	wg.Add(numPartitions)

	for partition := 0; partition < numPartitions; partition++ {
		go func(partition int) {
			defer wg.Done()
			consumePartition(brokers, topic, partition, numWorkersPerPartition)
		}(partition)
	}

	wg.Wait()
}

func consumePartition(brokers, topic string, partition, numWorkers int) {
	// Create a new consumer for a specific partition
	consumer, err := ckafka.NewConsumer(&ckafka.ConfigMap{
		"bootstrap.servers":      brokers,
		"group.id":               fmt.Sprintf("consumer-group-%d", partition),
		"auto.offset.reset":      "earliest",
		"enable.auto.commit":     false,
		"go.events.channel.size": 100000,
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer consumer.Close()

	// Assign specific partition
	topicPartition := []ckafka.TopicPartition{
		ckafka.TopicPartition{Topic: &topic, Partition: int32(partition)},
	}
	err = consumer.Assign(topicPartition)
	if err != nil {
		log.Fatalf("Failed to assign partition %d: %s", partition, err)
	}

	log.Printf("Consumer for partition %d started", partition)

	// Channel to distribute messages to workers
	messageChan := make(chan *ckafka.Message, 1000)

	// WaitGroup for workers
	var workerWg sync.WaitGroup
	workerWg.Add(numWorkers)

	// Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer workerWg.Done()
			processMessages(partition, workerID, messageChan)
		}(i)
	}

	// Consume messages and send to messageChan
	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			if e, ok := err.(ckafka.Error); ok && e.Code() == ckafka.ErrAllBrokersDown {
				log.Printf("All brokers down: %v", err)
				break
			} else {
				log.Printf("Consumer error: %v (%v)\n", err, msg)
				continue
			}
		}
		messageChan <- msg
	}

	// Close the message channel after consumption is done
	close(messageChan)

	// Wait for workers to finish processing
	workerWg.Wait()

	log.Printf("Consumer for partition %d exiting", partition)
}

func processMessages(partition, workerID int, messages <-chan *ckafka.Message) {
	for msg := range messages {
		// Simulate message processing
		log.Printf("Partition %d - Worker %d processing message: %s\n", partition, workerID, string(msg.Value))

		// Simulate processing time
		time.Sleep(10 * time.Millisecond)
	}
}
