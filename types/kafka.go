/**
 * @Description:
 * @Version: 1.0.0
 * @Author: liteng
 * @Date: 2019-11-27 17:04
 */

package types

import (
	"fmt"
	"github.com/Shopify/sarama"
	"karmq/config"
	"karmq/errors"
	"net"
	"strconv"
)

const MQ_KAFKA = "kafka_mq"

type Kafka struct {
	Config            *config.KafkaConfig
	Client            sarama.Client
	AsyncProducer     sarama.AsyncProducer
	Consumer          sarama.Consumer
	PartitionConsumer sarama.PartitionConsumer
}

func NewKafka() *Kafka {
	return &Kafka{}
}

func (k *Kafka) InitConfig(config *config.Configuration) {
	k.Config = &config.KafkaConfig
}

func (k *Kafka) Connect(url string) error {
	if url == "" {
		url = net.JoinHostPort(k.Config.Host, strconv.Itoa(k.Config.Port))
	}
	fmt.Println("kafka url: ", url)
	cfg := k.genConfig()
	client, err := sarama.NewClient([]string{url}, cfg)
	if err != nil {
		return errors.ErrConnection.ToError(err)
	}

	k.Client = client

	return nil

}

func (k *Kafka) CreateProducer() error {
	asyncProducer, err := sarama.NewAsyncProducerFromClient(k.Client)
	if err != nil {
		return errors.ErrNewProducerFromClient.ToError(err)
	}
	k.AsyncProducer = asyncProducer

	return nil
}

func (k *Kafka) CreateConsumer() error {
	consumer, err := sarama.NewConsumerFromClient(k.Client)
	if err != nil {
		return errors.ErrNewConsumerFromClient.ToError(err)
	}

	k.Consumer = consumer

	partitionConsumer, err := k.Consumer.ConsumePartition("kafka", 0, sarama.OffsetNewest)
	if err != nil {
		return errors.ErrReceive.ToError(err)
	}

	k.PartitionConsumer = partitionConsumer

	return nil
}

func (k *Kafka) Send(msg []byte) error {

	proMsg := &sarama.ProducerMessage{
		Topic: "kafka",
		Key:   sarama.StringEncoder("key"),
	}

	proMsg.Value = sarama.ByteEncoder(msg)

	k.AsyncProducer.Input() <- proMsg
	//_, _, err := k.AsyncProducer.SendMessage(proMsg)
	//if err != nil {
	//	return errors.ErrSend.ToError(err)
	//}

	//fmt.Println("partition: ", partition, "offset: ", offset)

	return nil
}

func (k *Kafka) Receive() ([]byte, error) {

	msg := <-k.PartitionConsumer.Messages()
	return msg.Value, nil
}

func (k *Kafka) Disconnect() error {

	if k.AsyncProducer != nil {
		err := k.AsyncProducer.Close()
		if err != nil {
			return errors.ErrAsyncProducerClose.ToError(err)
		}
	}

	if k.PartitionConsumer != nil {
		err := k.PartitionConsumer.Close()
		if err != nil {
			return errors.ErrPartitionConsumerClose.ToError(err)
		}
	}

	if k.Consumer != nil {
		err := k.Consumer.Close()
		if err != nil {
			return errors.ErrConsumerClose.ToError(err)
		}
	}

	err := k.Client.Close()
	if err != nil {
		return errors.ErrDisconnection.ToError(err)
	}

	return nil
}

func (k *Kafka) genConfig() *sarama.Config {

	cfg := sarama.NewConfig()

	//cfg.Net.MaxOpenRequests = 10

	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Partitioner = sarama.NewRandomPartitioner
	cfg.Producer.Return.Successes = true

	cfg.Consumer.Return.Errors = true
	cfg.Version = sarama.V0_11_0_2

	return cfg
}
