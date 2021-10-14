package kafsar

import "github.com/paashzj/kafka_go/pkg/kafka"

type Config struct {
	LogLevel     string
	KafkaConfig  kafka.ServerConfig
	PulsarConfig PulsarConfig
}

type PulsarConfig struct {
	Host     string
	HttpPort int
	TcpPort  int
}

func Run(config *Config, impl Server) error {
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
