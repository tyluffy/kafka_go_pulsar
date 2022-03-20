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
	"github.com/paashzj/kafka_go_pulsar/pkg/utils"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReadEarliestMsg(t *testing.T) {
	topic := uuid.New().String()
	partitionedTopic := utils.PartitionedTopic(defaultTopicType+topicPrefix+topic, partition)
	setupPulsar()
	producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: partitionedTopic})
	assert.Nil(t, err)
	message := pulsar.ProducerMessage{Value: testContent}
	messageId, err := producer.Send(context.TODO(), &message)
	logrus.Infof("send msg to pulsar %s", messageId)
	assert.Nil(t, err)
	readTopic := utils.PartitionedTopic(defaultTopicType+topicPrefix+topic, partition)
	msg := utils.ReadEarliestMsg(readTopic, maxFetchWaitMs, pulsarClient)
	if msg == nil {
		t.Fatal("msg is nil")
	}
	assert.Equal(t, msg.ID().LedgerID(), messageId.LedgerID())
	assert.Equal(t, msg.ID().EntryID(), messageId.EntryID())
	assert.Equal(t, msg.ID().PartitionIdx(), messageId.PartitionIdx())
}

func TestReadLatestMsg(t *testing.T) {
	topic := uuid.New().String()
	partitionedTopic := utils.PartitionedTopic(defaultTopicType+topicPrefix+topic, partition)
	setupPulsar()
	producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: partitionedTopic})
	assert.Nil(t, err)
	message := pulsar.ProducerMessage{Value: testContent}
	messageId, err := producer.Send(context.TODO(), &message)
	logrus.Infof("send msg to pulsar %s", messageId)
	assert.Nil(t, err)
	message = pulsar.ProducerMessage{Value: testContent}
	messageId, err = producer.Send(context.TODO(), &message)
	logrus.Infof("send msg to pulsar %s", messageId)
	assert.Nil(t, err)
	message = pulsar.ProducerMessage{Value: testContent}
	messageId, err = producer.Send(context.TODO(), &message)
	logrus.Infof("send msg to pulsar %s", messageId)
	assert.Nil(t, err)
	message = pulsar.ProducerMessage{Value: testContent}
	messageId, err = producer.Send(context.TODO(), &message)
	logrus.Infof("send msg to pulsar %s", messageId)
	assert.Nil(t, err)
	readPartitionedTopic := utils.PartitionedTopic(defaultTopicType+topicPrefix+topic, partition)
	msg, err := utils.GetLatestMsgId(topicPrefix+topic, readPartitionedTopic, partition, pulsarHttpUrl)
	logrus.Infof("msgId : %s", string(msg))
	assert.Nil(t, err)
	assert.NotNil(t, msg)
	lastedMsg := utils.ReadLastedMsg(readPartitionedTopic, maxFetchWaitMs, msg, pulsarClient)
	assert.NotNil(t, lastedMsg)
	assert.Equal(t, lastedMsg.ID().LedgerID(), messageId.LedgerID())
	assert.Equal(t, lastedMsg.ID().EntryID(), messageId.EntryID())
	assert.Equal(t, lastedMsg.ID().PartitionIdx(), messageId.PartitionIdx())
}
