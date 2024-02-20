package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type consumer struct {
	topic       string
	consumerCfg *kafka.ConfigMap
}

func newConsumer(cfg *Config) *consumer {
	return &consumer{
		topic: cfg.TopicName,
		consumerCfg: &kafka.ConfigMap{
			"bootstrap.servers":  cfg.BootstrapServers,
			"sasl.mechanisms":    cfg.SASLMechanism,
			"security.protocol":  cfg.SecurityProtocol,
			"sasl.username":      cfg.SASLUser,
			"sasl.password":      cfg.SASLPassword,
			"session.timeout.ms": 6000,
			"group.id":           cfg.GroupId,
			"auto.offset.reset":  "earliest",
		},
	}
}

func (c *consumer) run() error {
	consumer, err := kafka.NewConsumer(c.consumerCfg)
	fmt.Printf("starting consumer for %s", c.topic)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %s", err)
	}

	topics := []string{c.topic}
	err = consumer.SubscribeTopics(topics, nil)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %s", err)
	}
	defer consumer.Close()

	for {
		message, err := consumer.ReadMessage(3000 * time.Millisecond)
		if err == nil {

			consumer.CommitMessage(message)
			received := User{}
			err := json.Unmarshal(message.Value, &received)
			if err != nil {
				fmt.Printf("failed to deserialize payload: %s\n", err)
			} else {
				fmt.Printf("consumed from topic %s [%d] at offset %v: %+v\n",
					*message.TopicPartition.Topic,
					message.TopicPartition.Partition, message.TopicPartition.Offset,
					received)
			}
		} else if !strings.Contains(err.Error(), "Timed out") {
			fmt.Printf("%+v", err)
		}
	}
}
