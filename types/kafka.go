/**
 * @Description: 
 * @Version: 1.0.0
 * @Author: liteng
 * @Date: 2019-11-27 17:04
 */

package types

import (
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
	panic("implement me")
}

func (k *Kafka) Receive() error {
	panic("implement me")
}

func (k *Kafka) Disconnect() error {
	err := k.Client.Close()
	if err != nil {
		return errors.ErrDisconnection.ToError(err)
	}

	return nil
}

func (k* Kafka) genConfig() *sarama.Config {
	return &sarama.Config{}
}

