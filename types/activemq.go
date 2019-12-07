/**
 * @Description:
 * @Version: 1.0.0
 * @Author: liteng
 * @Date: 2019-11-27 17:08
 */

package types

import (
	"fmt"
	"github.com/go-stomp/stomp"
	"karmq/common"
	"karmq/config"
	"karmq/design"
	"karmq/errors"
	"strconv"
)

const MQ_ACTIVE = "active_mq"

type ActiveMQ struct {
	design.Base
	Config       *config.ActiveConfig
	Connection   *stomp.Conn
	Subscription *stomp.Subscription
}

func NewActiveMQ() *ActiveMQ {
	return &ActiveMQ{}
}

func (am *ActiveMQ) InitConfig(config *config.Configuration) {
	am.Config = config.ActiveConfig
}

func (am *ActiveMQ) Connect(url string) error {
	if url == "" {
		url = common.JoinHostPort(am.Config.Host, strconv.Itoa(am.Config.Port))
	}

	fmt.Println("active mq url: ", url)

	conn, err := stomp.Dial("tcp", url)
	if err != nil {
		return errors.ErrConnection.JoinError(err)
	}

	am.Connection = conn

	return nil
}

func (am *ActiveMQ) CreateProducer(name string) error {
	am.ProducerName = name
	return nil
}

func (am *ActiveMQ) CreateConsumer(name string) error {
	am.ConsumerName = name
	sub, err := am.Connection.Subscribe(am.ConsumerName, stomp.AckAuto)
	if err != nil {
		return errors.ErrReceive.JoinError(err)
	}

	am.Subscription = sub

	return nil
}

func (am *ActiveMQ) Send(msg []byte) error {
	err := am.Connection.Send(am.ProducerName, "text/plain", msg)
	if err != nil {
		return errors.ErrReceive.JoinError(err)
	}

	return nil
}

func (am *ActiveMQ) Receive() ([]byte, error) {

	data := <-am.Subscription.C

	return data.Body, nil
}

func (am *ActiveMQ) Disconnect() error {
	err := am.Connection.Disconnect()
	if err != nil {
		return errors.ErrDisconnection.JoinError(err)
	}

	return nil
}
