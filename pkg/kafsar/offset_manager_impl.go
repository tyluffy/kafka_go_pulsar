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
	"context"
	"encoding/json"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/paashzj/kafka_go_pulsar/pkg/model"
	"github.com/sirupsen/logrus"
	"strconv"
	"sync"
)

type OffsetManagerImpl struct {
	producer  pulsar.Producer
	consumer  pulsar.Consumer
	offsetMap map[string]MessageIdPair
	mutex     sync.RWMutex
}

func NewOffsetManager(producer pulsar.Producer, consumer pulsar.Consumer) OffsetManager {
	impl := OffsetManagerImpl{
		producer:  producer,
		consumer:  consumer,
		offsetMap: make(map[string]MessageIdPair),
	}
	impl.startOffsetConsumer()
	return &impl
}

func (o *OffsetManagerImpl) startOffsetConsumer() {
	go func() {
		for receive := range o.consumer.Chan() {
			logrus.Infof("receive key: %s, msg: %s", receive.Key(), string(receive.Payload()))
			payload := receive.Payload()
			if len(payload) == 0 {
				logrus.Errorf("payload length is 0. key: %s", receive.Key())
				o.mutex.Lock()
				delete(o.offsetMap, receive.Key())
				o.mutex.Unlock()
				continue
			}
			var msgIdData model.MessageIdData
			err := json.Unmarshal(payload, &msgIdData)
			if err != nil {
				logrus.Errorf("unmarshal failed. key: %s, topic: %s", receive.Key(), receive.Topic())
				continue
			}
			idData := msgIdData.MessageId
			msgId, err := pulsar.DeserializeMessageID(idData)
			if err != nil {
				logrus.Errorf("deserialize message id failed. key: %s, err: %s", receive.Key(), err)
				continue
			}
			logrus.Infof("received key %s offset %d", receive.Key(), msgIdData.Offset)
			pair := MessageIdPair{
				MessageId: msgId,
				Offset:    msgIdData.Offset,
			}
			o.mutex.Lock()
			o.offsetMap[receive.Key()] = pair
			o.mutex.Unlock()
		}
	}()
}

func (o *OffsetManagerImpl) CommitOffset(username, kafkaTopic, groupId string, partition int, pair MessageIdPair) error {
	key := o.generateKey(username, kafkaTopic, groupId, partition)
	data := model.MessageIdData{}
	data.MessageId = pair.MessageId.Serialize()
	data.Offset = pair.Offset
	marshal, err := json.Marshal(data)
	if err != nil {
		logrus.Errorf("convert msg to bytes failed. kafkaTopic: %s, err: %s", kafkaTopic, err)
		return err
	}
	message := pulsar.ProducerMessage{}
	message.Payload = marshal
	message.Key = key
	_, err = o.producer.Send(context.TODO(), &message)
	if err != nil {
		logrus.Errorf("commit offset failed. kafkaTopic: %s, offset: %d, err: %s", kafkaTopic, pair.Offset, err)
		return err
	}
	logrus.Infof("kafkaTopic: %s commit offset %d success", kafkaTopic, pair.Offset)
	return nil
}

func (o *OffsetManagerImpl) AcquireOffset(username, kafkaTopic, groupId string, partition int) (MessageIdPair, bool) {
	key := o.generateKey(username, kafkaTopic, groupId, partition)
	o.mutex.RLock()
	pair, exist := o.offsetMap[key]
	o.mutex.RUnlock()
	return pair, exist
}

func (o *OffsetManagerImpl) RemoveOffset(username, kafkaTopic, groupId string, partition int) (MessageIdPair, bool) {
	key := o.generateKey(username, kafkaTopic, groupId, partition)
	logrus.Infof("begin remove offset key: %s", key)
	message := pulsar.ProducerMessage{}
	message.Key = key
	_, err := o.producer.Send(context.TODO(), &message)
	if err != nil {
		logrus.Errorf("send msg failed. kafkaTopic: %s, err: %s", kafkaTopic, err)
		return MessageIdPair{}, false
	}
	logrus.Infof("kafkaTopic: %s remove offset success", kafkaTopic)
	return MessageIdPair{}, true
}

func (o *OffsetManagerImpl) Close() {
	o.producer.Close()
	o.consumer.Close()
}

func (o *OffsetManagerImpl) generateKey(username, kafkaTopic, groupId string, partition int) string {
	return username + kafkaTopic + groupId + strconv.Itoa(partition)
}
