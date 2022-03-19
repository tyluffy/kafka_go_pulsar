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
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	"github.com/paashzj/kafka_go/pkg/kafka"
	"github.com/paashzj/kafka_go_pulsar/pkg/constant"
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
	OffsetManager            OffsetManager
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
	if config.KafsarConfig.OffsetManager == nil {
		client := GetPulsarClient(config)
		consumer, err := GetOffsetConsumer(client, config)
		if err != nil {
			return nil, err
		}
		producer, err := GetOffsetProducer(client, config)
		if err != nil {
			return nil, err
		}
		config.KafsarConfig.OffsetManager = NewOffsetManager(producer, consumer)
	}
	logrus.Info("kafsar started")
	k, err := NewKafsar(impl, config)
	if err != nil {
		return nil, err
	}
	err = k.InitGroupCoordinator()
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

func GetPulsarClient(config *Config) pulsar.Client {
	pulsarUrl := fmt.Sprintf("pulsar://%s:%d", config.PulsarConfig.Host, config.PulsarConfig.TcpPort)
	pulsarClient, err := pulsar.NewClient(pulsar.ClientOptions{URL: pulsarUrl})
	if err != nil {
		logrus.Errorf("init pulsar client failed. err: %s", err)
		return nil
	}
	return pulsarClient
}

func GetOffsetConsumer(client pulsar.Client, config *Config) (pulsar.Consumer, error) {
	newUUID, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            getOffsetTopic(config),
		Type:             pulsar.Failover,
		SubscriptionName: newUUID.String(),
	})
	if err != nil {
		logrus.Errorf("subscribe consumer failed. topic: %s, err: %s", getOffsetTopic(config), err)
		return nil, err
	}
	return consumer, nil
}

func GetOffsetProducer(client pulsar.Client, config *Config) (pulsar.Producer, error) {
	options := pulsar.ProducerOptions{}
	options.Topic = getOffsetTopic(config)
	options.SendTimeout = constant.DefaultProducerSendTimeout
	options.MaxPendingMessages = constant.DefaultMaxPendingMs
	options.DisableBlockIfQueueFull = true
	producer, err := client.CreateProducer(options)
	if err != nil {
		logrus.Errorf("create producer failed. topic: %s, err: %s", options.Topic, err)
		return nil, err
	}
	return producer, nil
}

func getOffsetTopic(config *Config) string {
	return fmt.Sprintf("persistent://%s/%s/%s", config.KafsarConfig.PulsarTenant, config.KafsarConfig.PulsarNamespace, config.KafsarConfig.OffsetTopic)
}
