package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

const (
	brokers          = "localhost:9092"
	topic            = "my-topic"
	numPods          = 3 // Number of pods (consumer instances)
	numWorkersPerPod = 5 // Number of worker threads per pod
	processingTime   = 10 * time.Millisecond
	consumerGroupID  = "my-consumer-group" // Use the same group ID for all pods
)

func main() {
	// Sarama logger
	sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)

	var wg sync.WaitGroup
	wg.Add(numPods)

	for i := 0; i < numPods; i++ {
		go func(podID int) {
			defer wg.Done()
			consumeNewPod(podID)
		}(i)
	}

	// Wait for all pods to finish (they won't in this example)
	wg.Wait()
}

func consumeNewPod(podID int) {
	// Consumer group configuration
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Group.Session.Timeout = 10 * time.Second
	config.Consumer.MaxProcessingTime = 500 * time.Millisecond

	// Manual offset management
	config.Consumer.Offsets.AutoCommit.Enable = false

	// All pods use the same consumer group ID
	groupID := consumerGroupID

	// Create a new consumer group
	consumerGroup, err := sarama.NewConsumerGroup([]string{brokers}, groupID, config)
	if err != nil {
		log.Fatalf("[Pod %d] Error creating consumer group client: %v", podID, err)
	}
	defer consumerGroup.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Trap SIGINT and SIGTERM to trigger a shutdown.
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, os.Interrupt, syscall.SIGTERM)

	// Consumer group handler
	handler := &ConsumerGroupHandler{
		podID:        podID,
		numWorkers:   numWorkersPerPod,
		processingWG: &sync.WaitGroup{},
	}

	// Run the consumer group in a goroutine
	go func() {
		for {
			err := consumerGroup.Consume(ctx, []string{topic}, handler)
			if err != nil {
				log.Printf("[Pod %d] Error from consumer: %v", podID, err)
				time.Sleep(time.Second)
			}

			if ctx.Err() != nil {
				return
			}
		}
	}()

	// Wait for a termination signal
	select {
	case <-sigchan:
		log.Printf("[Pod %d] Received shutdown signal", podID)
		cancel()
	case <-ctx.Done():
	}

	// Wait for all message processing to finish
	handler.processingWG.Wait()
	log.Printf("[Pod %d] Consumer group exited", podID)
}

// ConsumerGroupHandler represents a Sarama consumer group consumer
type ConsumerGroupHandler struct {
	podID        int
	numWorkers   int
	processingWG *sync.WaitGroup
	messageChan  chan *sarama.ConsumerMessage
	session      sarama.ConsumerGroupSession
}

// Setup is run before the consumer group starts consuming
func (h *ConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	log.Printf("[Pod %d] Consumer group handler setup", h.podID)

	// Store the session for use in workers
	h.session = session

	// Initialize the message channel and worker pool
	h.messageChan = make(chan *sarama.ConsumerMessage, 1000)
	h.processingWG.Add(h.numWorkers)

	for i := 0; i < h.numWorkers; i++ {
		go h.worker(i)
	}

	return nil
}

// Cleanup is run after the consumer group stops consuming
func (h *ConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	log.Printf("[Pod %d] Consumer group handler cleanup", h.podID)
	close(h.messageChan)
	h.processingWG.Wait()
	return nil
}

// ConsumeClaim processes messages from Kafka
func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log.Printf("[Pod %d] Consuming partition %d", h.podID, claim.Partition())

	for msg := range claim.Messages() {
		h.messageChan <- msg
	}

	return nil
}

func (h *ConsumerGroupHandler) worker(workerID int) {
	defer h.processingWG.Done()
	for msg := range h.messageChan {
		err := processMessage(h.podID, workerID, msg)
		if err == nil {
			// Mark message as processed after successful processing
			h.session.MarkMessage(msg, "")
		} else {
			log.Printf("[Pod %d] Thread %d failed to process message offset %d: %v", h.podID, workerID, msg.Offset, err)
			// Handle retries or send to DLQ as needed
		}
	}
}

func processMessage(podID, workerID int, msg *sarama.ConsumerMessage) error {
	// Simulate message processing
	log.Printf("[Pod %d] Thread %d processing message offset %d: %s", podID, workerID, msg.Offset, string(msg.Value))

	// Simulate processing time
	time.Sleep(processingTime)

	// Return nil if processing is successful, or an error if it fails
	return nil
}
