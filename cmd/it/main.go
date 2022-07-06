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

func main() {
	flag.Parse()
	config := &kafsar.Config{}
	config.KafkaConfig.ListenHost = "0.0.0.0"
	config.KafkaConfig.ListenPort = 9092
	config.KafkaConfig.EventLoopNum = 100
	config.KafkaConfig.NeedSasl = false
	config.KafkaConfig.ClusterId = "it_kafsar"
	config.KafkaConfig.AdvertiseHost = "localhost"
	config.KafkaConfig.AdvertisePort = 9092
	config.KafkaConfig.MaxConn = int32(500)
	config.PulsarConfig.Host = "localhost"
	config.PulsarConfig.HttpPort = 8080
	config.PulsarConfig.TcpPort = 6650
	config.KafsarConfig.MaxConsumersPerGroup = 1
	config.KafsarConfig.GroupMaxSessionTimeoutMs = 60000
	config.KafsarConfig.GroupMinSessionTimeoutMs = 0
	config.KafsarConfig.MaxFetchRecord = 100
	config.KafsarConfig.MinFetchWaitMs = 10
	config.KafsarConfig.MaxFetchWaitMs = 200
	config.KafsarConfig.ContinuousOffset = true
	config.KafsarConfig.PulsarTenant = "public"
	config.KafsarConfig.PulsarNamespace = "default"
	config.KafsarConfig.OffsetTopic = "kafka_offset"
	config.KafsarConfig.GroupCoordinatorType = kafsar.Standalone
	config.KafsarConfig.InitialDelayedJoinMs = 3000
	config.KafsarConfig.RebalanceTickMs = 100
	config.TraceConfig.DisableTracing = true
	e := &ItKafsaImpl{}
	_, err := kafsar.Run(config, e)
	if err != nil {
		panic(err)
	}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	for {
		<-interrupt
	}
}
