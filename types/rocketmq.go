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

	fmt.Println("rocketmq url:", url)
	rm.URL = url

	return nil
}

func (rm *RocketMQ) Disconnect() error {

	return nil
}

func (rm *RocketMQ) CreateProducer(name string) error {
	rm.ProducerName = name

	pConfig := &rocketmq.ProducerConfig{}
	pConfig.GroupID = rm.ProducerName + "01"
	pConfig.NameServer = rm.URL
	pConfig.ProducerModel = rocketmq.CommonProducer
	pConfig.InstanceName = "testProducer"
	pConfig.LogC = &rocketmq.LogConfig{
		Path:     "/rocketmq/log",
		FileNum:  16,
		FileSize: 1 << 20,
		Level:    rocketmq.LogLevelDebug}

	pConfig.CompressLevel = 4
	pConfig.SendMsgTimeout = 5
	pConfig.MaxMessageSize = 1024

	producer, err := rocketmq.NewProducer(pConfig)
	if err != nil {
		return errors.ErrNewProducer.JoinError(err)
	}

	rm.Producer = producer

	return nil
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

	return nil

}

func (rm *RocketMQ) Subscribe(topic string) (chan string, error) {
	if rm.PushConsumer == nil {
		return nil, fmt.Errorf("PushConsumer is nil")
	}

	ch := make(chan interface{})
	var count = int64(10)
	msgs := make(chan string, 10)

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

	//result, err := rm.Producer.SendMessageOrderlyByShardingKey(msgs, "hello")
	err := rm.Producer.SendMessageOneway(msgs)
	if err != nil {
		return errors.ErrSend.JoinError(err)
	}

	//fmt.Println(result)

	return nil
}

func (rm *RocketMQ) Receive() ([]byte, error) {
	str := <-rm.Msgs
	return []byte(str), nil
}

func (rm *RocketMQ) Start() error {
	if rm.Producer != nil {
		return rm.Producer.Start()
	}

	if rm.PushConsumer != nil {
		return rm.PushConsumer.Start()
	}

	if rm.PullConsumer != nil {
		return rm.PullConsumer.Start()
	}

	return nil
}

func (rm *RocketMQ) Shutdown() error {
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
