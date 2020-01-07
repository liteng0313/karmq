/**
 * @Description:
 * @Version: 1.0.0
 * @Author: liteng
 * @Date: 2020-01-06 14:02
 */

package types

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"time"
)

const MqUrl = "amqp://guest:guest@localhost:5672"

type RabbitMQ2 struct {
	conn    *amqp.Connection
	channel *amqp.Channel

	QueueName string
	Exchange  string
	Key       string
	MqUrl     string
}

func NewRabbitMQ2(queuename, exchange, key string) *RabbitMQ2 {
	rm2 := &RabbitMQ2{QueueName: queuename, Exchange: exchange, Key: key, MqUrl: MqUrl}
	var err error
	rm2.conn, err = amqp.Dial(rm2.MqUrl)
	rm2.failOnErr(err, "Dial error")
	rm2.channel, err = rm2.conn.Channel()
	rm2.failOnErr(err, "Channel error")

	return rm2
}

func NewSimpleMode(queueName string) *RabbitMQ2 {

	return NewRabbitMQ2(queueName, "", "")
}

// 断开 channel 和 connection
func (r *RabbitMQ2) Close() {
	if r.channel != nil {
		err := r.channel.Close()
		if err != nil {
			log.Println("channel close error: ", err)
		}
	}

	if r.conn != nil {
		err := r.conn.Close()
		if err != nil {
			log.Println("conn close error: ", err)
		}
	}
}

func (r *RabbitMQ2) failOnErr(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Errorf("%s: %s", msg, err))
	}
}

func (r *RabbitMQ2) PublishSimple(msgBytes []byte) {
	// persist
	_, err := r.channel.QueueDeclare(
		r.QueueName,
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Println("queue declare error: ", err)
	}

	err = r.channel.Publish(
		r.Exchange,
		r.QueueName,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         msgBytes,
		},
	)

	if err != nil {
		log.Println("chanel publish error: ", err)
	}

	fmt.Println("send msg on success: ", string(msgBytes))

}

func (r *RabbitMQ2) ConsumeSimple() {

	// persist
	_, err := r.channel.QueueDeclare(
		r.QueueName,
		true,
		false,
		false,
		false, //阻塞
		nil,
	)

	if err != nil {
		log.Println("queue declare error: ", err)
	}

	msgs, err := r.channel.Consume(
		r.QueueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		log.Println("consume error: ", err)
	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Println("received msg: ", string(d.Body))
			err := d.Ack(false)
			time.Sleep(500 * time.Millisecond)
			if err != nil {
				log.Println("d.ack(false) error: ", err)
			}
		}
	}()
	log.Println("[*] Waiting for message, to exit press Ctrl+C")
	<-forever
}
