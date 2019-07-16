package kafka_go

import (
	"github.com/shopify/sarama"
	"log"
)

var Address = []string{"localhost:9092"}

type AsyncProducer struct {
	producer sarama.AsyncProducer
}

func (ap *AsyncProducer) InitProducer(brokers []string) {
	config := sarama.NewConfig()
	p, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		log.Println("new async producer failed:", err)
		return
	}
	ap.producer = p
}

func (ap *AsyncProducer) ProduceMessage(topic string, encoder sarama.Encoder) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: encoder,
	}
	log.Printf("message: %v\n", *msg)
	ap.producer.Input() <- msg

	select {
	case suc := <-ap.producer.Successes():
		log.Printf("success offset=%d, timestamp=%s\n", suc.Offset, suc.Timestamp)
		log.Printf("success partition=%v, topic=%v\n", suc.Partition, suc.Topic)
	case fail := <-ap.producer.Errors():
		log.Printf("failed err=%v\n", fail.Err.Error())
	}
	return nil
}
