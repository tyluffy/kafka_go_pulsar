// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"flag"
	"github.com/paashzj/kafka_go_pulsar/pkg/kafsar"
	"os"
	"os/signal"
)

var listenHost = flag.String("kafka_listen_host", "0.0.0.0", "kafka listen host")
var listenPort = flag.Int("kafka_listen_port", 9092, "kafka listen port")
var eventLoopNum = flag.Int("event_loop_num", 100, "multi core")
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
	config.PulsarConfig.Host = *pulsarHost
	config.PulsarConfig.HttpPort = *pulsarHttpPort
	config.PulsarConfig.TcpPort = *pulsarTcpPort
	config.KafsarConfig.GnetConfig.ListenHost = *listenHost
	config.KafsarConfig.GnetConfig.ListenPort = *listenPort
	config.KafsarConfig.GnetConfig.EventLoopNum = *eventLoopNum
	config.KafsarConfig.NeedSasl = *needSasl
	config.KafsarConfig.ClusterId = *clusterId
	config.KafsarConfig.AdvertiseHost = *advertiseListenAddr
	config.KafsarConfig.AdvertisePort = *advertiseListenPort
	config.KafsarConfig.MaxConn = int32(*maxConn)
	config.KafsarConfig.MaxConsumersPerGroup = 1
	config.KafsarConfig.GroupMaxSessionTimeoutMs = 60000
	config.KafsarConfig.GroupMinSessionTimeoutMs = 0
	config.KafsarConfig.MaxFetchRecord = 100
	config.KafsarConfig.MinFetchWaitMs = 10
	config.KafsarConfig.MaxFetchWaitMs = 200
	config.KafsarConfig.ContinuousOffset = false
	config.KafsarConfig.PulsarTenant = "public"
	config.KafsarConfig.PulsarNamespace = "default"
	config.KafsarConfig.OffsetTopic = "kafka_offset"
	config.KafsarConfig.GroupCoordinatorType = kafsar.Standalone
	config.KafsarConfig.InitialDelayedJoinMs = 3000
	config.KafsarConfig.RebalanceTickMs = 100
	e := &ExampleKafsarImpl{}
	server := kafsar.NewKafsarServer(config, e)
	err := server.Run()
	if err != nil {
		panic(err)
	}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	for {
		<-interrupt
	}
}
