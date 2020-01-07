/**
 * @Description:
 * @Version: 1.0.0
 * @Author: liteng
 * @Date: 2019-11-28 11:47
 */

package core

import (
	"fmt"
	"karmq/types"
	"strconv"
	"testing"
	"time"
)

const URL_RABBIT = "amqp://guest:guest@localhost:5672"
const URL_RABBIT2 = "amqp://admin:gep..GEP@39.106.59.251:5672/gep_vhost"
const URL_ACTIVE = "localhost:60000"
const URL_KAFKA = "localhost:9092"
const URL_ROCKET = "localhost:9876"

const PC_NAME = "hello1"
const PC_NAME2 = "hello2"
const TestName1 = "topic"

var category = types.MQ_RABBIT
var url = URL_RABBIT2

func TestNewKarmq(t *testing.T) {

	categories := []string{
		"hello",
		types.MQ_ACTIVE,
		types.MQ_KAFKA,
		types.MQ_RABBIT,
		types.MQ_ROCKET,
	}

	for _, category := range categories {

		karmq, err := NewKarmq(category)

		if category == "hello" {
			t.Log(err)
		} else {
			fmt.Println(karmq)
			fmt.Printf("%s:%d\n", karmq.Config.RabbitConfig.Host, karmq.Config.RabbitConfig.Port)
		}
	}
}

func TestKarmq_Connect(t *testing.T) {

	karmq, _ := NewKarmq(category)
	err := karmq.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
}

func TestKarmq_DisConnect(t *testing.T) {

	karmq, err := NewKarmq(category)
	if err != nil {
		t.Fatal(err)
	}
	err = karmq.Connect(url)
	if err != nil {
		t.Fatal(err)
	}

	err = karmq.DisConnect()
	if err != nil {
		t.Fatal(err)
	}

}

func TestKarmq_GenerateProducer(t *testing.T) {

	karmq, _ := NewKarmq(category)
	err := karmq.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer karmq.DisConnect()

	producer, err := karmq.GenerateProducer(PC_NAME)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(producer)
}

func TestKarmq_GenerateConsumer(t *testing.T) {

	karmq, _ := NewKarmq(category)
	err := karmq.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer karmq.DisConnect()

	consumer, err := karmq.GenerateConsumer(PC_NAME)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(consumer)
}

func TestKarmq_Send(t *testing.T) {
	karmq, _ := NewKarmq(category)
	err := karmq.Connect(url)
	if err != nil {
		t.Fatal(err)
	}

	defer karmq.DisConnect()

	producer, err := karmq.GenerateProducer(PC_NAME)
	if err != nil {
		t.Fatal("generate producer error: ", err)
	}

	//fmt.Println("producer:", producer)
	//
	//actualProducer, ok := producer.(*types.RocketMQ)
	//if !ok {
	//	t.Fatal("not get rocket mq producer")
	//}
	//
	//err = actualProducer.Producer.Start()
	//if err != nil {
	//	t.Fatal("actual producer start error")
	//}

	err = producer.Send([]byte("hello world 1"))
	err = producer.Send([]byte("hello world 2"))
	err = producer.Send([]byte("hello world 3"))
	err = producer.Send([]byte("hello world 4"))
	err = producer.Send([]byte("hello world 5"))

	if err != nil {
		t.Fatal(err)
	}

	//err = actualProducer.Producer.Shutdown()
	//if err != nil {
	//	t.Fatal("actual producer shutdown error:", err)
	//}
}

func TestKarmq_Receive(t *testing.T) {
	karmq, _ := NewKarmq(category)
	err := karmq.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer karmq.DisConnect()

	consumer, err := karmq.GenerateConsumer(PC_NAME)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(consumer)

	bytes, err := consumer.Receive()
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("receive: ", string(bytes))
}

func TestKarmq_Async_Receive(t *testing.T) {

	karmq, _ := NewKarmq(category)
	err := karmq.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer karmq.DisConnect()

	consumer, err := karmq.GenerateConsumer(PC_NAME)
	if err != nil {
		t.Fatal(err)
	}

	for {
		bytes, err := consumer.Receive()
		if err != nil {
			t.Fatal(err)
			break
		}
		if string(bytes) == "quit" {
			break
		}
		fmt.Println("recv: ", string(bytes))
		time.Sleep(500 * time.Millisecond)
	}

}

func TestKarmq_Send_Receive(t *testing.T) {

	karmq, _ := NewKarmq(category)
	err := karmq.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer karmq.DisConnect()

	producer, err := karmq.GenerateProducer(PC_NAME)
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := karmq.GenerateConsumer(PC_NAME)
	if err != nil {
		t.Fatal(err)
	}

	finish := make(chan bool)

	go func() {
		for {
			msg, err := consumer.Receive()
			if err != nil {
				t.Fatal(err)
			}
			fmt.Println("receive: ", string(msg))
			if string(msg) == "quit" {
				fmt.Println("msg: ", string(msg))
				finish <- true
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
	}()

	for i := 0; i < 10; i++ {
		err := producer.Send([]byte("hello, how are you? " + strconv.Itoa(i)))
		if err != nil {
			t.Fatal(err)
		}
	}
	err = producer.Send([]byte("quit"))

	<-finish

}
