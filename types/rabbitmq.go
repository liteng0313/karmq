/**
 * @Description: 
 * @Version: 1.0.0
 * @Author: liteng
 * @Date: 2019-11-27 17:07
 */

package types

import (
	"github.com/streadway/amqp"
	"karmq/config"
	"karmq/errors"
	"net"
	"strconv"
)

type RabbitMQ struct {
	Config     *config.RabbitConfig
	Connection*amqp.Connection
 	Channel    *amqp.Channel
}

func NewRabbitMQ() *RabbitMQ {
	return &RabbitMQ{}
}

func (rm *RabbitMQ) InitConfig(config *config.Configuration) {
	rm.Config = &config.RabbitConfig
}

func (rm *RabbitMQ) Connect() error {

	url := net.JoinHostPort(rm.Config.Host, strconv.Itoa(rm.Config.Port))
	connection, err := amqp.Dial(url)
	if err != nil {
		return errors.ErrConnection.ToError(err)
	}

	channe, err := connection.Channel()
	if err != nil {
		return errors.ErrRabbitChannel.ToError(err)
	}

	rm.Connection = connection
	rm.Channel = channe

	return nil
}

func (rm *RabbitMQ) Send(msg []byte) error {
	queue, err := rm.Channel.QueueDeclare(
		"rabbit",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return errors.ErrSend.ToError(err)
	}

	err = rm.Channel.Publish(
		"",
		queue.Name,
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

func (rm *RabbitMQ) Receive()([]byte, error) {
	queue, err := rm.Channel.QueueDeclare(
		"rabbit",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, errors.ErrRabbitQueueDeclare.ToError(err)
	}

	msgs, err := rm.Channel.Consume(
		queue.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
		)

	if err != nil {
		return nil, errors.ErrReceive.ToError(err)
	}

	data := <-msgs

	return data.Body, nil
}

func (rm *RabbitMQ) Disconnect() error {
	err := rm.Connection.Close()
	if err != nil {
		return errors.ErrDisconnection.ToError(err)
	}

	return nil
}


