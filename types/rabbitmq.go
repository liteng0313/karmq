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
	"karmq/errors"
	"strconv"
)

const MQ_RABBIT = "rabbit_mq"

type RabbitMQ struct {
	Config          *config.RabbitConfig
	Connection      *amqp.Connection
	Queue           *amqp.Queue
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
	fmt.Println(rm.Config)
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

func (rm *RabbitMQ) CreateProducer() error {
	return rm.createChannel(true)
}

func (rm *RabbitMQ) CreateConsumer() error {
	err := rm.createChannel(false)
	if err != nil {
		return err
	}

	msgs, err := rm.ConsumerChannel.Consume(
		rm.Queue.Name,
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
		rm.Queue.Name,
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

func (rm *RabbitMQ) createChannel(isProducer bool) error {
	channel, err := rm.Connection.Channel()
	if err != nil {
		return errors.ErrRabbitChannel.ToError(err)
	}

	queue, err := channel.QueueDeclare(
		"rabbit",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return errors.ErrRabbitQueueDeclare.ToError(err)
	}

	rm.Queue = &queue

	if isProducer {
		rm.ProducerChannel = channel
	} else {
		rm.ConsumerChannel = channel
	}

	return nil
}
