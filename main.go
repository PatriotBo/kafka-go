package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"sync"
)

var brokers = []string{"127.0.0.1:9092"}
var topic = "test"

func main(){
	log.Println("开始生产--------")
	go product()

	log.Println("开始消费-------")
	go consume()
}

func product(){
	var ap AsyncProducer
	ap.InitProducer(brokers)
	defer ap.producer.Close()
	var wg sync.WaitGroup
	for i := 0;i < 1000;i++ {
		wg.Add(1)
		message := fmt.Sprintf("生产： 这是第[%d]条消息",i)
		go func(msg string) {
			ap.ProduceMessage(topic,sarama.ByteEncoder(msg))
			wg.Done()
		}(message)
	}
	wg.Wait()
}

func consume(){
	ac, err := InitConsumer(brokers)
	defer ac.ConsumerGroup.Close()
	if err != nil {
		log.Println("init consumer failed:",err)
		return
	}

	partitions,err := ac.ConsumerGroup.Partitions(topic)
	if err != nil || len(partitions) <= 0{
		log.Println("get partition of this topic failed: ",err, partitions)
		return
	}
	partitionConsumer,err := ac.GetPartitionConsumer(topic,partitions[0],sarama.OffsetNewest)
	defer partitionConsumer.Close()
	if err != nil {
		log.Println(err)
		return
	}
	ac.ConsumeMessage(partitionConsumer)
}