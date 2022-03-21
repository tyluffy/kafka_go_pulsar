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

package test

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	"github.com/paashzj/kafka_go_pulsar/pkg/kafsar"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
)

func TestOffsetManager(t *testing.T) {
	topic := uuid.New().String()
	groupId := uuid.New().String()
	pulsarTopic := defaultTopicType + topicPrefix + topic
	setupPulsar()
	manager, err := kafsar.NewOffsetManager(pulsarClient, kafsar.KafsarConfig{
		PulsarTenant:    "public",
		PulsarNamespace: "default",
		OffsetTopic:     "kafka_offset",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	// wait for manager start
	time.Sleep(3 * time.Second)

	producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: pulsarTopic})
	if err != nil {
		t.Fatal(err)
	}
	message := pulsar.ProducerMessage{Value: testContent}
	messageId, err := producer.Send(context.TODO(), &message)
	if err != nil {
		t.Fatal(err)
	}
	logrus.Infof("send msg to pulsar %s", messageId)
	rand.Seed(time.Now().Unix())
	offset := rand.Int63()
	messagePair := kafsar.MessageIdPair{
		MessageId: messageId,
		Offset:    offset,
	}
	err = manager.CommitOffset(username, topic, groupId, partition, messagePair)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Second)
	acquireOffset, flag := manager.AcquireOffset(username, topic, groupId, partition)
	if !flag {
		t.Fatal("acquire offset not exists")
	}
	assert.Equal(t, acquireOffset.Offset, offset)
	flag = manager.RemoveOffset(username, topic, groupId, partition)
	if !flag {
		t.Fatal("remove offset not exist")
	}
	time.Sleep(3 * time.Second)
	acquireOffset, flag = manager.AcquireOffset(username, topic, groupId, partition)
	if flag {
		t.Fatal("acquire offset exists")
	}
	assert.Nil(t, acquireOffset.MessageId)
}
