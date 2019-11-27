/**
 * @Description: 
 * @Version: 1.0.0
 * @Author: liteng
 * @Date: 2019-11-27 16:33
 */

package _interface

import "karmq/config"

type Middleware interface {
	InitConfig(config *config.Configuration)
	Connect() error
	Send(msg []byte) error
	Receive() ([]byte, error)
	Disconnect() error
}