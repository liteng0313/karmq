/**
 * @Description: 
 * @Version: 1.0.0
 * @Author: liteng
 * @Date: 2019-11-27 20:10
 */

package errors

import "fmt"

type ErrCode int

const (
	Error						ErrCode = -1
	Success						ErrCode = 0
	ErrConnection				ErrCode = 41001
	ErrSend						ErrCode = 41002
	ErrReceive					ErrCode = 41003
	ErrDisconnection			ErrCode = 41004

	// rabbit mq
	ErrRabbitChannel  			ErrCode = 42001
	ErrRabbitQueueDeclare		ErrCode = 42002

	// kafka
	ErrNewProducerFromClient	ErrCode = 43001
	ErrNewConsumerFromClient	ErrCode = 43002
)

var ErrMap = map[ErrCode]string{
	Error:						"Error",
	Success:					"Success",
	ErrConnection:				"Connection Error",
	ErrSend:					"Send Msg Error",
	ErrReceive:					"Receive Msg Error",
	ErrDisconnection:			"Disconnection Error",

	ErrRabbitChannel:			"Rabbit Channel Error",
	ErrRabbitQueueDeclare: 		"Rabbit Queue Declare Error",
	ErrNewProducerFromClient:	"New Producer From Client Error",
	ErrNewConsumerFromClient:	"New Consumer From Client Error",
}

func (code ErrCode) Error() string {
	return ErrMap[code]
}

func (code ErrCode) ToError(detail error) error {
	return fmt.Errorf("%s: %s\n", code.Error(), detail.Error())
}

