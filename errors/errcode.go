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
	Error            ErrCode = -1
	Success          ErrCode = 0
	ErrConnection    ErrCode = 41001
	ErrSend          ErrCode = 41002
	ErrReceive       ErrCode = 41003
	ErrDisconnection ErrCode = 41004
	ErrInvalidType   ErrCode = 41005
	ErrConfiguration ErrCode = 41006

	// rabbit mq
	ErrRabbitChannel      ErrCode = 42001
	ErrRabbitQueueDeclare ErrCode = 42002

	// kafka
	ErrNewProducerFromClient  ErrCode = 43001
	ErrNewConsumerFromClient  ErrCode = 43002
	ErrSyncProducerClose      ErrCode = 43003
	ErrAsyncProducerClose     ErrCode = 43004
	ErrConsumerClose          ErrCode = 43005
	ErrPartitionConsumerClose ErrCode = 43006

	// active mq
	ErrSubscribe ErrCode = 44001
)

var ErrMap = map[ErrCode]string{
	Error:            "Error",
	Success:          "Success",
	ErrConnection:    "Connection Error",
	ErrSend:          "Send Msg Error",
	ErrReceive:       "Receive Msg Error",
	ErrDisconnection: "Disconnection Error",
	ErrInvalidType:   "Invalid Type",
	ErrConfiguration: "Configuration Error",

	ErrRabbitChannel:      "RabbitMQ Channel Error",
	ErrRabbitQueueDeclare: "RabbitMQ Queue Declare Error",

	ErrNewProducerFromClient:  "Kafka New Producer From Client Error",
	ErrNewConsumerFromClient:  "Kafka New Consumer From Client Error",
	ErrSyncProducerClose:      "Kafka Sync Producer Close Error",
	ErrAsyncProducerClose:     "Kafka Async Producer Close Error",
	ErrConsumerClose:          "Kafka Consumer Close Error",
	ErrPartitionConsumerClose: "Kafka Partition Consumer Error",

	ErrSubscribe: "ActiveMQ Subscribe Error",
}

func (code ErrCode) Error() string {
	return ErrMap[code]
}

func (code ErrCode) ToError(detail error) error {
	return fmt.Errorf("%s: %s\n", code.Error(), detail.Error())
}
