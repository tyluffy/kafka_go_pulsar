package test

import (
	"github.com/paashzj/kafka_go/pkg/kafka"
	"github.com/paashzj/kafka_go_pulsar/pkg/kafsar"
)

func setupKafsar() (*kafsar.Broker, int) {
	port, err := AcquireUnusedPort()
	if err != nil {
		panic(err)
	}
	broker, err := setupKafsarInternal(port)
	if err != nil {
		panic(err)
	}
	return broker, port
}

func setupKafsarInternal(port int) (*kafsar.Broker, error) {
	config := &kafsar.Config{}
	config.KafkaConfig = kafka.ServerConfig{}
	config.KafkaConfig.ListenHost = "localhost"
	config.KafkaConfig.ListenPort = port
	config.KafkaConfig.AdvertiseHost = "localhost"
	config.KafkaConfig.AdvertisePort = port
	config.PulsarConfig = kafsar.PulsarConfig{}
	config.PulsarConfig.Host = "localhost"
	config.PulsarConfig.HttpPort = 8080
	config.PulsarConfig.TcpPort = 6650
	kafsarImpl := &KafsarImpl{}
	return kafsar.Run(config, kafsarImpl)
}
