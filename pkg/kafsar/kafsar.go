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

package kafsar

import (
	"fmt"
	"github.com/paashzj/kafka_go/pkg/kafka"
	"github.com/sirupsen/logrus"
)

type Config struct {
	KafkaConfig  kafka.ServerConfig
	PulsarConfig PulsarConfig
	KafsarConfig KafsarConfig
}

type PulsarConfig struct {
	Host     string
	HttpPort int
	TcpPort  int
}

type KafsarConfig struct {
	MaxConsumersPerGroup     int
	GroupMinSessionTimeoutMs int
	GroupMaxSessionTimeoutMs int
	ConsumerReceiveQueueSize int
	MaxFetchRecord           int
	MaxFetchWaitMs           int
	ContinuousOffset         bool
	// PulsarTenant use for kafsar internal
	PulsarTenant string
	// PulsarNamespace use for kafsar internal
	PulsarNamespace string
	// OffsetTopic use to store kafka offset
	OffsetTopic string
}

type Broker struct {
	impl *KafkaImpl
}

func (b *Broker) Close() {
	b.impl.Close()
}

func Run(config *Config, impl Server) (*Broker, error) {
	logrus.Info("kafsar started")
	k, err := NewKafsar(impl, config)
	if err != nil {
		return nil, err
	}
	_, err = kafka.Run(&config.KafkaConfig, k)
	if err != nil {
		return nil, err
	}
	broker := &Broker{impl: k}
	return broker, nil
}

func getOffsetTopic(config KafsarConfig) string {
	return fmt.Sprintf("persistent://%s/%s/%s", config.PulsarTenant, config.PulsarNamespace, config.OffsetTopic)
}
