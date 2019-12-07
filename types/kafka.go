/**
 * @Description:
 * @Version: 1.0.0
 * @Author: liteng
 * @Date: 2019-11-27 17:04
 */

package types

import (
	"github.com/Shopify/sarama"
	"karmq/common"
	"karmq/config"
	"karmq/design"
	"karmq/errors"
	"strconv"
)

const MQ_KAFKA = "kafka_mq"

type Kafka struct {
	design.Base
	Client            sarama.Client
	Url               string
	Config            *config.KafkaConfig
	AsyncProducer     sarama.AsyncProducer
	Consumer          sarama.Consumer
	PartitionConsumer sarama.PartitionConsumer
}

func NewKafka() *Kafka {
	return &Kafka{}
}

func (k *Kafka) InitConfig(config *config.Configuration) {
	k.Config = config.KafkaConfig
}

func (k *Kafka) Connect(url string) error {
	if url == "" {
		url = common.JoinHostPort(k.Config.Host, strconv.Itoa(k.Config.Port))
	}
	k.Url = url

	cfg := k.genConfig()

	client, err := sarama.NewClient([]string{url}, cfg)
	if err != nil {
		return errors.ErrConnection.JoinError(err)
	}

	k.Client = client
	return nil

}

func (k *Kafka) CreateProducer(name string) error {

	k.ProducerName = name
	asyncProducer, err := sarama.NewAsyncProducerFromClient(k.Client)
	if err != nil {
		return errors.ErrNewProducerFromClient.JoinError(err)
	}
	k.AsyncProducer = asyncProducer

	return nil
}

func (k *Kafka) CreateConsumer(name string) error {

	k.ConsumerName = name

	consumer, err := sarama.NewConsumerFromClient(k.Client)
	if err != nil {
		return errors.ErrConnection.JoinError(err)
	}

	k.Consumer = consumer
	partitionConsumer, err := k.Consumer.ConsumePartition(k.ConsumerName, 0, sarama.OffsetOldest)
	if err != nil {
		return errors.ErrReceive.JoinError(err)
	}

	k.PartitionConsumer = partitionConsumer

	return nil
}

func (k *Kafka) Send(msg []byte) error {

	proMsg := &sarama.ProducerMessage{
		Topic: k.ProducerName,
		Key:   sarama.StringEncoder("key"),
	}

	proMsg.Value = sarama.ByteEncoder(msg)

	k.AsyncProducer.Input() <- proMsg

	//select {
	//case <-k.AsyncProducer.Successes():
	//	fmt.Println("send msg on success")
	//case err := <-k.AsyncProducer.Errors():
	//	fmt.Println("send msg on error", err)
	//}

	//_, _, err := k.AsyncProducer.SendMessage(proMsg)
	//if err != nil {
	//	return errors.ErrSend.JoinError(err)
	//}

	//fmt.Println("partition: ", partition, "offset: ", offset)

	return nil
}

func (k *Kafka) Receive() ([]byte, error) {

	msg := <-k.PartitionConsumer.Messages()
	//err := <-k.PartitionConsumer.Errors()
	return msg.Value, nil
}

func (k *Kafka) Disconnect() error {

	err := k.Client.Close()
	if err != nil {
		return errors.ErrDisconnection.JoinError(err)
	}

	if k.AsyncProducer != nil {
		err := k.AsyncProducer.Close()
		if err != nil {
			return errors.ErrAsyncProducerClose.JoinError(err)
		}
	}

	if k.PartitionConsumer != nil {
		err := k.PartitionConsumer.Close()
		if err != nil {
			return errors.ErrPartitionConsumerClose.JoinError(err)
		}
	}

	if k.Consumer != nil {
		err := k.Consumer.Close()
		if err != nil {
			return errors.ErrConsumerClose.JoinError(err)
		}
	}

	return nil
}

func (k *Kafka) genConfig() *sarama.Config {

	cfg := sarama.NewConfig()

	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Partitioner = sarama.NewRandomPartitioner
	//cfg.Producer.Return.Successes = true
	//cfg.Producer.Return.Errors = true
	cfg.Consumer.Return.Errors = true
	cfg.Version = sarama.V0_11_0_2

	return cfg
}
