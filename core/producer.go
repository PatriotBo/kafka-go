package core

import (
	"github.com/shopify/sarama"
	"log"
)

var Address = []string{"localhost:9092"}

type AsyncProducer struct {
	Producer sarama.AsyncProducer
}

func (ap *AsyncProducer) InitProducer(brokers []string) {
	config := sarama.NewConfig()
	p, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		log.Println("new async producer failed:", err)
		return
	}
	ap.Producer = p
}

func (ap *AsyncProducer) ProduceMessage(topic string, encoder sarama.Encoder) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: encoder,
	}
	//log.Printf("message: %v\n", *msg)
	ap.Producer.Input() <- msg

	select {
	case suc := <-ap.Producer.Successes():
		log.Printf("success offset=%d, timestamp=%s\n", suc.Offset, suc.Timestamp)
		log.Printf("success partition=%v, topic=%v\n", suc.Partition, suc.Topic)
	case fail := <-ap.Producer.Errors():
		log.Printf("failed err=%v\n", fail.Err.Error())
	}
	return nil
}
