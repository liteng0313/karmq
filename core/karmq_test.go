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
const URL_ACTIVE = "localhost:60000"
const URL_KAFKA = "localhost:9092"

var category = types.MQ_RABBIT
var url = URL_RABBIT

func TestNewKarmq(t *testing.T) {

	categories := []string{
		"hello",
		types.MQ_ACTIVE,
		types.MQ_KAFKA,
		types.MQ_RABBIT,
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

	producer, err := karmq.GenerateProducer()
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

	consumer, err := karmq.GenerateConsumer()
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

	producer, err := karmq.GenerateProducer()
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(producer)

	err = producer.Send([]byte("hello world 1"))
	err = producer.Send([]byte("hello world 2"))
	err = producer.Send([]byte("hello world 3"))
	err = producer.Send([]byte("quit"))
	if err != nil {
		t.Fatal(err)
	}
}

func TestKarmq_Receive(t *testing.T) {
	karmq, _ := NewKarmq(category)
	err := karmq.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer karmq.DisConnect()

	consumer, err := karmq.GenerateConsumer()
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

	consumer, err := karmq.GenerateConsumer()
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

	producer, err := karmq.GenerateProducer()
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := karmq.GenerateConsumer()
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
