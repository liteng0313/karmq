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



type Kafka struct {
	Config *config.KafkaConfig
	Client sarama.Client
	SyncProducer sarama.SyncProducer
	AsyncProducer sarama.AsyncProducer
	Consumer sarama.Consumer
}

func (k *Kafka) InitConfig(config *config.Configuration){
	k.Config = &config.KafkaConfig
}

func (k *Kafka) Connect() error {
	url := net.JoinHostPort(k.Config.Host, strconv.Itoa(k.Config.Port))
	cfg := k.genConfig()
	client, err := sarama.NewClient([]string{url}, cfg)
	if err != nil {
		return errors.ErrConnection.ToError(err)
	}

	k.Client = client

	return nil

}

func (k *Kafka) Send(msg []byte) error {
	syncProducer, err := sarama.NewSyncProducerFromClient(k.Client)
	if err != nil {
		return errors.ErrNewProducerFromClient.ToError(err)
	}
	defer syncProducer.Close()

	proMsg := &sarama.ProducerMessage{
		Topic:"kafka",
		Key: sarama.StringEncoder("key"),
	}

	proMsg.Value = sarama.ByteEncoder(msg)

	partition, offset, err := syncProducer.SendMessage(proMsg)
	if err != nil {
		return errors.ErrSend.ToError(err)
	}

	fmt.Println("partition: ", partition, "offset: ", offset)

	return nil
}

func (k *Kafka) Receive() ([]byte, error) {
	consumer, err := sarama.NewConsumerFromClient(k.Client)
	if err != nil {
		return nil, errors.ErrNewConsumerFromClient.ToError(err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition("kafka", 0, sarama.OffsetOldest)
	if err != nil {
		return nil, errors.ErrReceive.ToError(err)
	}

	defer partitionConsumer.Close()

	msg := <- partitionConsumer.Messages()

	return msg.Value, nil
}

func (k *Kafka) Disconnect() error {
	err := k.Client.Close()
	if err != nil {
		return errors.ErrDisconnection.ToError(err)
	}

	return nil
}

func (k* Kafka) genConfig() *sarama.Config {

	cfg := new(sarama.Config)
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Partitioner = sarama.NewRandomPartitioner
	cfg.Producer.Return.Successes = true

	cfg.Consumer.Return.Errors = true
	cfg.Version = sarama.V0_11_0_2

	return cfg
}

