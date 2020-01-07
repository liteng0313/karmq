/**
 * @Description:
 * @Version: 1.0.0
 * @Author: liteng
 * @Date: 2019-11-28 10:47
 */

package core

import (
	"karmq/config"
	"karmq/design"
	ke "karmq/errors"
	"karmq/types"
)

var CATEGORIES = []string{
	types.MQ_ROCKET,
	types.MQ_RABBIT,
	types.MQ_ACTIVE,
	types.MQ_KAFKA,
}

type Karmq struct {
	Type       string
	Config     *config.Configuration
	Middleware design.Middleware
}

func NewEmptyKarmq() *Karmq {
	return &Karmq{}
}

func NewKarmq(category string) (*Karmq, error) {

	if !IsValid(category) {
		return nil, ke.ErrInvalidType
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
	case types.MQ_ROCKET:
		k.Middleware = types.NewRocketMQ()
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

func IsValid(category string) bool {

	for _, c := range CATEGORIES {
		if category == c {
			return true
		}
	}
	return false
}
