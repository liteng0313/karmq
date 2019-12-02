/**
 * @Description:
 * @Version: 1.0.0
 * @Author: liteng
 * @Date: 2019-11-27 17:07
 */

package types

import (
	"fmt"
	"github.com/streadway/amqp"
	"karmq/config"
	"karmq/design"
	"karmq/errors"
	"strconv"
)

const MQ_RABBIT = "rabbit_mq"

type RabbitMQ struct {
	design.Base
	Config          *config.RabbitConfig
	Connection      *amqp.Connection
	ProducerChannel *amqp.Channel
	ConsumerChannel *amqp.Channel
	Delivery        <-chan amqp.Delivery
}

func NewRabbitMQ() *RabbitMQ {
	return &RabbitMQ{}
}

func (rm *RabbitMQ) InitConfig(config *config.Configuration) {
	rm.Config = &config.RabbitConfig
}

func (rm *RabbitMQ) Connect(url string) error {
	if url == "" {
		fmt.Printf("%s:%d\n", rm.Config.Host, rm.Config.Port)
		//url = net.JoinHostPort(rm.Config.Host, strconv.Itoa(rm.Config.Port))
		url = rm.Config.Host + ":" + strconv.Itoa(rm.Config.Port)
	}
	fmt.Println("url: ", url)
	connection, err := amqp.Dial(url)
	if err != nil {
		return errors.ErrConnection.ToError(err)
	}

	rm.Connection = connection

	return nil
}

func (rm *RabbitMQ) CreateProducer(name string) error {
	rm.ProducerName = name

	channel, err := rm.Connection.Channel()
	if err != nil {
		return errors.ErrRabbitChannel.ToError(err)
	}

	_, err = channel.QueueDeclare(
		rm.ProducerName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return errors.ErrRabbitQueueDeclare.ToError(err)
	}
	rm.ProducerChannel = channel

	return nil
	//return rm.createChannel(true)
}

func (rm *RabbitMQ) CreateConsumer(name string) error {
	rm.ConsumerName = name

	channel, err := rm.Connection.Channel()
	if err != nil {
		return errors.ErrRabbitChannel.ToError(err)
	}

	_, err = channel.QueueDeclare(
		rm.ConsumerName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return errors.ErrRabbitQueueDeclare.ToError(err)
	}

	rm.ConsumerChannel = channel
	msgs, err := rm.ConsumerChannel.Consume(
		rm.ConsumerName,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	rm.Delivery = msgs

	if err != nil {
		return errors.ErrReceive.ToError(err)
	}

	return nil
}

func (rm *RabbitMQ) Send(msg []byte) error {

	err := rm.ProducerChannel.Publish(
		"",
		rm.ProducerName,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        msg,
		})

	if err != nil {
		return errors.ErrSend.ToError(err)
	}

	return nil

}

func (rm *RabbitMQ) Receive() ([]byte, error) {

	data := <-rm.Delivery
	return data.Body, nil
}

func (rm *RabbitMQ) Disconnect() error {
	err := rm.Connection.Close()
	if err != nil {
		return errors.ErrDisconnection.ToError(err)
	}

	return nil
}
