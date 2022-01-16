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

func Run(config *Config, impl Server) error {
	logrus.Info("kafsar started")
	k := &KafkaImpl{server: impl, pulsarConfig: config.PulsarConfig}
	err := k.ConnPulsar()
	if err != nil {
		return err
	}
	_, err = kafka.Run(&config.KafkaConfig, k)
	if err != nil {
		return err
	}
	return nil
}
