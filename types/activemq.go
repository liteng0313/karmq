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
	"karmq/config"
	"karmq/errors"
	"net"
	"strconv"
)

const MQ_ACTIVE = "active_mq"

type ActiveMQ struct {
	Config       *config.ActiveConfig
	Connection   *stomp.Conn
	Subscription *stomp.Subscription
}

func NewActiveMQ() *ActiveMQ {
	return &ActiveMQ{}
}

func (am *ActiveMQ) InitConfig(config *config.Configuration) {
	am.Config = &config.ActiveConfig
}

func (am *ActiveMQ) Connect(url string) error {
	if url == "" {
		url = net.JoinHostPort(am.Config.Host, strconv.Itoa(am.Config.Port))
	}

	fmt.Println("active mq url: ", url)

	conn, err := stomp.Dial("tcp", url)
	if err != nil {
		return errors.ErrConnection.ToError(err)
	}

	am.Connection = conn

	return nil
}

func (am *ActiveMQ) CreateProducer() error {
	return nil
}

func (am *ActiveMQ) CreateConsumer() error {
	sub, err := am.Connection.Subscribe("active", stomp.AckAuto)
	if err != nil {
		return errors.ErrReceive.ToError(err)
	}

	am.Subscription = sub

	return nil
}

func (am *ActiveMQ) Send(msg []byte) error {
	err := am.Connection.Send("active", "text/plain", msg)
	if err != nil {
		return errors.ErrReceive.ToError(err)
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
		return errors.ErrDisconnection.ToError(err)
	}

	return nil
}
