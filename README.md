# karmq
General message queue middleware encapsulation, like kafka, rabbitmq, activemq and the others .

# Example

    category := types.MQ_RABBIT                             // types.MQ_KAFKA, types.MQ_ACTIVE
    url := "amqp://guest:guest@localhost:5672"              // 相应的URL, 要开启对应中间件服务器
    
    karmq, _ := NewKarmq(category)
	err := karmq.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer karmq.DisConnect()

	// producer name 要和 consumer name 保持一致，不然接收不到相应的数据
	producer, err := karmq.GenerateProducer("test1")
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := karmq.GenerateConsumer("test2")
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