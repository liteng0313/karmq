/**
 * @Description:
 * @Version: 1.0.0
 * @Author: liteng
 * @Date: 2020-01-07 17:48
 */

package types

import (
	"strconv"
	"testing"
)

const QueueName = "world1"

func TestRabbitMQ2_Simple(t *testing.T) {

	rm := NewSimpleMode(QueueName)
	defer rm.Close()

	i := 0
	for {

		if i == 100 {
			break
		}
		msg := "hello world " + strconv.Itoa(i)
		rm.PublishSimple([]byte(msg))
		i += 1
	}
}

func TestRabbitMQ2_ConsumeSimple(t *testing.T) {
	rm := NewSimpleMode(QueueName)
	defer rm.Close()

	rm.ConsumeSimple()
}

func TestRabbitMQ2_ConsumeSimple2(t *testing.T) {
	rm := NewSimpleMode(QueueName)
	defer rm.Close()

	rm.ConsumeSimple()
}
