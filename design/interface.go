/**
 * @Description:
 * @Version: 1.0.0
 * @Author: liteng
 * @Date: 2019-11-27 16:33
 */

package design

import "karmq/config"

type Middleware interface {
	InitConfig(config *config.Configuration)
	Connect(url string) error
	Disconnect() error
	CreateProducer(name string) error
	CreateConsumer(name string) error
	Producer
	Consumer
}

type Producer interface {
	Send(msg []byte) error
}

type Consumer interface {
	Receive() ([]byte, error)
}

type Base struct {
	ProducerName string
	ConsumerName string
}
