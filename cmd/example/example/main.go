package main

import (
	"flag"
	"github.com/paashzj/kafka_go_pulsar/pkg/kafsar"
)

var logLevel = flag.String("v", "5", "log level")

var listenAddr = flag.String("kafka_listen_addr", "0.0.0.0", "kafka listen addr")
var multiCore = flag.Bool("kafka_multi_core", false, "multi core")
var needSasl = flag.Bool("kafka_need_sasl", false, "need sasl")
var maxConn = flag.Int("kafka_max_conn", 500, "need sasl")

var clusterId = flag.String("kafka_cluster_id", "shoothzj", "kafka cluster id")
var advertiseListenAddr = flag.String("kafka_advertise_addr", "localhost", "kafka advertise addr")
var advertiseListenPort = flag.Int("kafka_advertise_port", 9092, "kafka advertise port")

var pulsarHost = flag.String("pulsar_host", "localhost", "pulsar host")
var pulsarHttpPort = flag.Int("pulsar_http_port", 8080, "pulsar http port")
var pulsarTcpPort = flag.Int("pulsar_tcp_port", 6650, "pulsar tcp port")

func main() {
	flag.Parse()
	config := &kafsar.Config{}
	config.LogLevel = *logLevel
	config.KafkaConfig.ListenAddr = *listenAddr
	config.KafkaConfig.MultiCore = *multiCore
	config.KafkaConfig.NeedSasl = *needSasl
	config.KafkaConfig.ClusterId = *clusterId
	config.KafkaConfig.AdvertiseHost = *advertiseListenAddr
	config.KafkaConfig.AdvertisePort = *advertiseListenPort
	config.KafkaConfig.MaxConn = int32(*maxConn)
	config.PulsarConfig.Host = *pulsarHost
	config.PulsarConfig.HttpPort = *pulsarHttpPort
	config.PulsarConfig.TcpPort = *pulsarTcpPort
	e := &ExampleKafsarImpl{}
	err := kafsar.Run(config, e)
	if err != nil {
		panic(err)
	}
}
