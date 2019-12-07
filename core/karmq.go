/**
 * @Description:
 * @Version: 1.0.0
 * @Author: liteng
 * @Date: 2019-11-28 10:47
 */

package core

import (
	"fmt"
	"karmq/config"
	"karmq/design"
	ke "karmq/errors"
	"karmq/types"
)

type Karmq struct {
	Type       string
	Config     *config.Configuration
	Middleware design.Middleware
}

func NewEmptyKarmq() *Karmq {
	return &Karmq{}
}

func NewKarmq(category string) (*Karmq, error) {

	if category != types.MQ_RABBIT && category != types.MQ_ACTIVE && category != types.MQ_KAFKA {
		return nil, ke.ErrInvalidType.JoinError(fmt.Errorf(
			"Please choose one of them as the input parameter: \"%s\" \"%s\" \"%s\"\n",
			types.MQ_RABBIT, types.MQ_KAFKA, types.MQ_ACTIVE))
	}
	k := NewEmptyKarmq()
	k.Type = category

	configuration, err := config.LoadConfig(config.CONFIG_FILE_PATH)
	if err != nil {
		return nil, ke.ErrConfiguration.JoinError(err)
	}

	k.Config = configuration
	k.parseType(category)

	return k, nil
}

func (k *Karmq) parseType(category string) {

	switch category {
	case types.MQ_RABBIT:
		k.Middleware = types.NewRabbitMQ()
	case types.MQ_ACTIVE:
		k.Middleware = types.NewActiveMQ()
	case types.MQ_KAFKA:
		k.Middleware = types.NewKafka()
	}

	k.Middleware.InitConfig(k.Config)
}

func (k *Karmq) Connect(url string) error {

	return k.Middleware.Connect(url)
}

func (k *Karmq) GenerateProducer(name string) (design.Producer, error) {

	if name == "" {
		return nil, ke.ErrProducerNameEmpty
	}

	err := k.Middleware.CreateProducer(name)
	if err != nil {
		return nil, err
	}

	return k.Middleware, nil
}

func (k *Karmq) GenerateConsumer(name string) (design.Consumer, error) {

	if name == "" {
		return nil, ke.ErrConsumerNameEmpty
	}

	err := k.Middleware.CreateConsumer(name)
	if err != nil {
		return nil, err
	}

	return k.Middleware, nil
}

func (k *Karmq) DisConnect() error {
	return k.Middleware.Disconnect()
}
