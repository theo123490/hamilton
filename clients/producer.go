package clients

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/spf13/viper"
)

func Producer(cfgFile string) {
	fmt.Printf("Hi this should start a producer %s\n", cfgFile)

	producerConfig := readProducerConfig(cfgFile)
	runProducer(producerConfig)
}

type producerConfig struct {
	brokers              string `default:""`
	version              string `default:""`
	topic                string `default:""`
	verbose              bool
	producerDelayMs      int
	producerStringLength int
	producerStringBlub   bool
}

func readProducerConfig(cfgFile string) producerConfig {
	fmt.Println(viper.GetInt("KAFKA_PROUCER_STRING_LENGTH"))
	producerConfig := producerConfig{
		brokers:              viper.GetString("KAFKA_BROKERS"),
		version:              viper.GetString("KAFKA_VERSION"),
		topic:                viper.GetString("KAFKA_TOPIC"),
		verbose:              viper.GetBool("KAFKA_VERBOSE"),
		producerDelayMs:      viper.GetInt("KAFKA_PROUCER_DELAY_MS"),
		producerStringLength: viper.GetInt("KAFKA_PROUCER_STRING_LENGTH"),
		producerStringBlub:   viper.GetBool("KAFKA_PROUCER_DELAY_MS"),
	}

	return producerConfig
}

func runProducer(cfg producerConfig) {
	keepRunning := true
	producerConfig := sarama.NewConfig()
	brokers := strings.Split(cfg.brokers, ",")
	producerConfig.Producer.Return.Successes = true

	config := sarama.NewConfig()
	if cfg.verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}
	if cfg.version != "" {
		version, err := sarama.ParseKafkaVersion(cfg.version)
		config.Version = version
		if err != nil {
			log.Panicf("Error parsing Kafka version: %v", err)
		}
	}

	config.Producer.Retry.Backoff = 0
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Return.Successes = true
	config.ClientID = "hamilton-" + uuid.New().String()

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		fmt.Println("Could not create producer: ", err)
	}
	var msgString string = "blubBlub"
	for keepRunning {
		time.Sleep(time.Duration(cfg.producerDelayMs) * time.Millisecond)

		if cfg.producerStringBlub {
			msgString = "blub"
		} else {
			msgString = randSeq(cfg.producerStringLength)
		}

		msg := &sarama.ProducerMessage{Topic: cfg.topic, Value: sarama.StringEncoder(msgString)}
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			fmt.Println("Message not sent: ", err)
		} else {
			log.Printf("Sent to partion %v and the offset is %v", partition, offset)
		}
	}
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
