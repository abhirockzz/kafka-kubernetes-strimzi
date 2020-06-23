package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	bootstrapServers, caLocation, topic, userCertLocation, userKeyLocation, userKeyPassword string
	kp                                                                                      *kafka.Producer
	kc                                                                                      *kafka.Consumer
)

var active = true

func init() {
	bootstrapServers = os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	caLocation = os.Getenv("CA_CERT_LOCATION")
	topic = os.Getenv("KAFKA_TOPIC")

	userCertLocation = os.Getenv("USER_CERT_LOCATION")
	userKeyLocation = os.Getenv("USER_KEY_LOCATION")
	userKeyPassword = os.Getenv("USER_KEY_PASSWORD")

	producerConfig := &kafka.ConfigMap{"bootstrap.servers": bootstrapServers, "security.protocol": "SSL", "ssl.ca.location": caLocation, "ssl.certificate.location": userCertLocation, "ssl.key.location": userKeyLocation, "ssl.key.password": userKeyPassword}

	var err error
	kp, err = kafka.NewProducer(producerConfig)

	if err != nil {
		log.Fatal("failed to create producer - ", err)
	}

	consumerConfig := &kafka.ConfigMap{"bootstrap.servers": bootstrapServers, "security.protocol": "SSL", "ssl.ca.location": caLocation, "ssl.certificate.location": userCertLocation, "ssl.key.location": userKeyLocation, "ssl.key.password": userKeyPassword, "group.id": "strimzi-tls-test-consumer-group"}

	kc, err = kafka.NewConsumer(consumerConfig)

	if err != nil {
		log.Fatal("failed to create consumer - ", err)
	}
}

func main() {

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)

	// producer goroutine
	go func() {
		fmt.Println("started producer goroutine")

		for active == true {
			select {
			case <-exit:
				active = false
			default:
				err := kp.Produce(&kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny}, Key: []byte("key-" + time.Now().String()), Value: []byte("value-" + time.Now().String())}, nil)
				if err != nil {
					fmt.Println("failed to produce message", err)
				}

				time.Sleep(2 * time.Second)
			}
		}
	}()

	// track producer delivery
	go func() {
		fmt.Println("started producer delivery goroutine")

		for active == true {
			select {
			case <-exit:
				active = false
			case e := <-kp.Events():
				if e == nil {
					continue
				}
				m := e.(*kafka.Message)
				if m.TopicPartition.Error != nil {
					fmt.Println("delivery failed ", m.TopicPartition.Error)
				}
				fmt.Println("delivered messaged", e)
			}
		}
	}()

	// consumer loop
	fmt.Println("started consumer")

	err := kc.Subscribe(topic, nil)
	if err != nil {
		log.Fatalf("unable to subscribe to topic %s - %v", topic, err)
	}
	for active == true {
		select {
		case <-exit:
			active = false
		default:
			ke := kc.Poll(1000)
			if ke == nil {
				continue
			}

			switch e := ke.(type) {
			case *kafka.Message:
				fmt.Printf("received message from %s: %s\n",
					e.TopicPartition, string(e.Value))

			case kafka.Error:
				fmt.Fprintf(os.Stderr, "Error: %v: %v\n", e.Code(), e)
			}
		}
	}

	kp.Close()
	fmt.Println("closed producer")

	err = kc.Close()
	if err != nil {
		fmt.Println("failed to close consumer ", err)
		return
	}
	fmt.Println("closed consumer")
}
