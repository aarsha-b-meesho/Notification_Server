package kafka

import (
	"log"
	"notifications/configurations"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Producer = kafka.Producer
type Consumer = kafka.Consumer

func NewConsumer(brokers, groupID string) (*Consumer, error) {
	log.Println("returning new consumer of kafka")
	return kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
	})
}

func NewProducer(brokers string) (*Producer, error) {
	log.Println("returning new producer of kafka")
	return kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
}
func GetKafkaProducer()(*kafka.Producer,error){
	// Initialize Kafka producer
	producer, err := NewProducer(config.KafkaAddr)
	if err != nil {
		return nil, err
	}
	return producer,nil
}
func GetKafkaConsumer()(*kafka.Consumer,error){
	// Initialize Kafka producer
	consumer, err := NewConsumer(config.KafkaAddr,config.KafkaTopic)
	if err != nil {
		return nil, err
	}
	return consumer,nil
}
