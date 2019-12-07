/**
 * @Description:
 * @Version: 1.0.0
 * @Author: liteng
 * @Date: 2019-12-07 10:59
 */

package types

import (
	"fmt"
	"github.com/apache/rocketmq-client-go/core"
	"karmq/common"
	"karmq/config"
	"karmq/design"
	"karmq/errors"
	"strconv"
	"sync/atomic"
)

const MQ_ROCKET = "rocket_mq"

type RocketMQ struct {
	design.Base
	URL          string
	Config       *config.RocketConfig
	Producer     rocketmq.Producer
	PushConsumer rocketmq.PushConsumer
	PullConsumer rocketmq.PullConsumer
	Msgs         chan string
}

type messageQueueSelector struct {
}

func (m *messageQueueSelector) Select(size int, msg *rocketmq.Message, arg interface{}) int {
	return 0
}

func NewRocketMQ() *RocketMQ {
	return &RocketMQ{}
}

func (rm *RocketMQ) InitConfig(config *config.Configuration) {
	rm.Config = config.RocketConfig
}

func (rm *RocketMQ) Connect(url string) error {
	if url == "" {
		url = common.JoinHostPort(rm.Config.Host, strconv.Itoa(rm.Config.Port))
	}

	fmt.Println("rocket mq url:", url)
	rm.URL = url

	return nil
}

func (rm *RocketMQ) Disconnect() error {
	if rm.Producer != nil {
		return rm.Producer.Shutdown()
	}

	if rm.PushConsumer != nil {
		return rm.PushConsumer.Shutdown()
	}

	if rm.PullConsumer != nil {
		return rm.PullConsumer.Shutdown()
	}

	return nil
}

func (rm *RocketMQ) CreateProducer(name string) error {
	rm.ProducerName = name

	pConfig := &rocketmq.ProducerConfig{}
	pConfig.GroupID = rm.ProducerName + "01"
	pConfig.NameServer = rm.URL
	pConfig.ProducerModel = rocketmq.OrderlyProducer

	producer, err := rocketmq.NewProducer(pConfig)
	if err != nil {
		return errors.ErrNewProducer.JoinError(err)
	}

	rm.Producer = producer

	return rm.Producer.Start()
}

func (rm *RocketMQ) CreateConsumer(name string) error {
	rm.ConsumerName = name

	cConfig := &rocketmq.PushConsumerConfig{
		ClientConfig: rocketmq.ClientConfig{
			GroupID:    rm.ConsumerName + "01",
			NameServer: rm.URL,
			Credentials: &rocketmq.SessionCredentials{
				AccessKey: "YK",
				SecretKey: "SK",
				Channel:   "Cloud",
			},
		},
		Model:         rocketmq.Clustering,
		ConsumerModel: rocketmq.CoCurrently,
	}

	consumer, err := rocketmq.NewPushConsumer(cConfig)
	if err != nil {
		return errors.ErrNewPushConsumer.JoinError(err)
	}

	rm.PushConsumer = consumer

	strings, err := rm.Subscribe(rm.ConsumerName)
	if err != nil {
		return err
	}

	rm.Msgs = strings

	return rm.PushConsumer.Start()

}

func (rm *RocketMQ) Subscribe(topic string) (chan string, error) {
	if rm.PushConsumer == nil {
		return nil, fmt.Errorf("PushConsumer is nil")
	}

	ch := make(chan interface{})
	var count = int64(100)
	msgs := make(chan string, 100)

	err := rm.PushConsumer.Subscribe(topic, "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
		msgs <- msg.Body
		if atomic.AddInt64(&count, -1) <= 0 {
			ch <- "quit"
		}

		return rocketmq.ConsumeSuccess
	})

	if err != nil {
		close(ch)
		return nil, errors.ErrConsumerScribe.JoinError(err)
	}

	<-ch

	return msgs, nil

}

func (rm *RocketMQ) Send(msg []byte) error {
	msgs := &rocketmq.Message{
		Topic: rm.ProducerName,
		Tags:  "tags",
		Keys:  "key",
		Body:  string(msg),
	}

	s := &messageQueueSelector{}
	result, err := rm.Producer.SendMessageOrderly(msgs, s, nil, 1)
	if err != nil {
		return errors.ErrSend.JoinError(err)
	}

	fmt.Println("send result: ", result)
	return nil
}

func (rm *RocketMQ) Receive() ([]byte, error) {
	str := <-rm.Msgs
	return []byte(str), nil
}
