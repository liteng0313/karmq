/**
 * @Description: 
 * @Version: 1.0.0
 * @Author: liteng
 * @Date: 2019-11-27 16:32
 */

package config

import (
	"encoding/json"
	"io/ioutil"
)

const (
	MQ_KAFKA = "kafka_mq"
	MQ_RABBIT = "rabbit_mq"
	MQ_ACTIVE = "active_mq"

	CONFIG_FILE_PATH = "../config.json"

)

type CommonConfig struct {
	Host string `json:"host"`
	Port int `json:"port"`
}

type KafkaConfig struct {
	CommonConfig
}

type RabbitConfig struct {
	CommonConfig
}

type ActiveConfig struct {
	CommonConfig
}

type Configuration struct {
	KafkaConfig KafkaConfig `json:"kafka_mq"`
	RabbitConfig RabbitConfig `json:"rabbit_mq"`
	ActiveConfig ActiveConfig `json:"active_mq"`
}

func LoadConfig(path string) (*Configuration, error) {

	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	config := new(Configuration)
	err = json.Unmarshal(bytes, config)
	if err != nil {
		return nil, err
	}

	return config, err
}




