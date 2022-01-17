package kafsar

import (
	"github.com/paashzj/kafka_go/pkg/kafka"
	"github.com/sirupsen/logrus"
)

type Config struct {
	KafkaConfig  kafka.ServerConfig
	PulsarConfig PulsarConfig
}

type PulsarConfig struct {
	Host     string
	HttpPort int
	TcpPort  int
}

type Broker struct {
}

func Run(config *Config, impl Server) (*Broker, error) {
	logrus.Info("kafsar started")
	k := &KafkaImpl{server: impl, pulsarConfig: config.PulsarConfig}
	err := k.ConnPulsar()
	if err != nil {
		return nil, err
	}
	_, err = kafka.Run(&config.KafkaConfig, k)
	if err != nil {
		return nil, err
	}
	broker := &Broker{}
	return broker, nil
}
