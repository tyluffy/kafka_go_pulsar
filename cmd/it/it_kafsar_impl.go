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

import "github.com/apache/pulsar-client-go/pulsar"

var pulsarClient, _ = pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})

type ItKafsarImpl struct {
}

func (e ItKafsarImpl) Auth(username string, password string, clientId string) (bool, error) {
	return true, nil
}

func (e ItKafsarImpl) AuthTopic(username string, password, clientId, topic, permissionType string) (bool, error) {
	return true, nil
}

func (e ItKafsarImpl) AuthTopicGroup(username string, password, clientId, consumerGroup string) (bool, error) {
	return true, nil
}

func (e ItKafsarImpl) AuthGroupTopic(topic, groupId string) bool {
	return true
}

func (e ItKafsarImpl) SubscriptionName(groupId string) (string, error) {
	return groupId, nil
}

func (e ItKafsarImpl) PulsarTopic(username, topic string) (string, error) {
	return "persistent://public/default/" + topic, nil
}

func (e ItKafsarImpl) PartitionNum(username, topic string) (int, error) {
	pulsarTopic, err := e.PulsarTopic(username, topic)
	if err != nil {
		return 0, err
	}
	partitions, err := pulsarClient.TopicPartitions(pulsarTopic)
	if err != nil {
		return 0, err
	}
	return len(partitions), nil
}

func (e ItKafsarImpl) ListTopic(username string) ([]string, error) {
	return nil, nil
}

func (e ItKafsarImpl) HasFlowQuota(username, topic string) bool {
	return true
}
