package main

import (
	"github.com/Shopify/sarama"
	"log"
)

type asyncConsumer struct {
	ConsumerGroup sarama.Consumer
}

// NewConsumer 返回的实际上是一个consumer group； 需要调用NewPartitionConsumer来获得一个指定partition的普通consumer。
// sarama中的consumer必须手动关闭，否则会造成内存泄露
func InitConsumer(brokers []string)(asyncConsumer,error){
	var ac asyncConsumer
	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer(brokers,config)
	if err != nil {
		log.Println("new consumer failedL ",err)
		return ac,err
	}
	ac.ConsumerGroup = consumer
	return ac,nil
}

//指定topic,partition和offset的consumer
func (ac *asyncConsumer)GetPartitionConsumer(topic string,partition int32,offset int64)(sarama.PartitionConsumer,error){
	log.Printf("create partition consumer topic=%s partition=%d offset=%d\n",topic,partition,offset)
	parConsumer, err := ac.ConsumerGroup.ConsumePartition(topic,partition,offset)
	if err != nil {
		log.Println("create consumer partition failed:",err)
		return parConsumer,err
	}
	return parConsumer,nil
}

func (ac *asyncConsumer)ConsumeMessage(parConsumer sarama.PartitionConsumer){
	defer parConsumer.Close()
	ch := make(chan struct{})

	go func(){
		for {
			select {
			case msg := <- parConsumer.Messages():
				log.Println("consumer message: ",msg)
			case err := <- parConsumer.Errors():
				log.Println("consumer error:",err)
				ch <- struct{}{}
			}
		}
	}()
	<- ch
}
