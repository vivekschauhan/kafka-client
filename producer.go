package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type payloadGeneratorFactory func() ([]byte, error)
type producer struct {
	topic            string
	producerCfg      *kafka.ConfigMap
	payloadGenerator payloadGeneratorFactory
}

func newProducer(cfg *Config, factory payloadGeneratorFactory) *producer {
	return &producer{
		topic:            cfg.TopicName,
		payloadGenerator: factory,
		producerCfg: &kafka.ConfigMap{
			"bootstrap.servers": cfg.BootstrapServers,
			"sasl.mechanisms":   cfg.SASLMechanism,
			"security.protocol": cfg.SecurityProtocol,
			"sasl.username":     cfg.SASLUser,
			"sasl.password":     cfg.SASLPassword},
	}
}

func (p *producer) run() error {
	fmt.Printf("starting producer for %s", p.topic)

	producer, err := kafka.NewProducer(p.producerCfg)
	if err != nil {
		return fmt.Errorf("failed to create producer: %s", err)
	}

	for i := 0; i < 10; i++ {
		buf, _ := p.payloadGenerator()

		producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &p.topic,
				Partition: kafka.PartitionAny},
			Value: buf}, nil)

		e := <-producer.Events()

		message := e.(*kafka.Message)
		if message.TopicPartition.Error != nil {
			return fmt.Errorf("failed to deliver message: %v. %s",
				message.TopicPartition, message.TopicPartition.Error)
		} else {
			fmt.Printf("delivered to topic %s [%d] at offset %v\n",
				*message.TopicPartition.Topic,
				message.TopicPartition.Partition,
				message.TopicPartition.Offset)
		}
	}

	producer.Close()
	return nil
}
