/**
 * @Description: 
 * @Version: 1.0.0
 * @Author: liteng
 * @Date: 2019-11-27 17:08
 */

package types

import (
	"github.com/go-stomp/stomp"
	"karmq/config"
	"karmq/errors"
	"net"
	"strconv"
)

type ActiveMQ struct {
	Config *config.ActiveConfig
	Connection *stomp.Conn

}

func (am *ActiveMQ) InitConfig(config *config.Configuration){
	am.Config = &config.ActiveConfig
}

func (am *ActiveMQ) Connect() error {
	url := net.JoinHostPort(am.Config.Host, strconv.Itoa(am.Config.Port))
	conn, err := stomp.Dial("tcp", url)
	if err != nil {
		return errors.ErrConnection.ToError(err)
	}

	am.Connection = conn

	return nil
}

func (am *ActiveMQ) Send(msg []byte) error {
	err := am.Connection.Send("active", "text/plain", msg)
	if err != nil {
		return errors.ErrReceive.ToError(err)
	}

	return nil
}

func (am *ActiveMQ) Receive() ([]byte, error){
	sub, err := am.Connection.Subscribe("active", stomp.AckAuto)
	if err != nil {
		return nil, errors.ErrReceive.ToError(err)
	}

	data := <- sub.C

	return data.Body, nil
}

func (am *ActiveMQ) Disconnect() error {
	err := am.Connection.Disconnect()
	if err != nil {
		return errors.ErrDisconnection.ToError(err)
	}

	return nil
}



