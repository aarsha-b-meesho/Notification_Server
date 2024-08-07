package kafka

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Producer = kafka.Producer
type Consumer = kafka.Consumer

func New_Consumer(brokers, groupID string) (*Consumer, error) {
	log.Println("returning new consumer of kafka")
	return kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	})
}

func New_Producer(brokers string) (*Producer, error) {
	log.Println("returning new producer of kafka")
	return kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
}
