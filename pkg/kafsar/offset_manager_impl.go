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
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	"github.com/paashzj/kafka_go_pulsar/pkg/constant"
	"github.com/paashzj/kafka_go_pulsar/pkg/model"
	"github.com/paashzj/kafka_go_pulsar/pkg/utils"
	"github.com/sirupsen/logrus"
	"strconv"
	"sync"
	"time"
)

type OffsetManagerImpl struct {
	producer       pulsar.Producer
	consumer       pulsar.Consumer
	offsetMap      map[string]MessageIdPair
	mutex          sync.RWMutex
	client         pulsar.Client
	offsetTopic    string
	pulsarHttpAddr string
	startFlag      bool
}

func NewOffsetManager(client pulsar.Client, config KafsarConfig, pulsarHttpAddr string) (OffsetManager, error) {
	consumer, err := getOffsetConsumer(client, config)
	if err != nil {
		return nil, err
	}
	producer, err := getOffsetProducer(client, config)
	if err != nil {
		consumer.Close()
		return nil, err
	}
	impl := OffsetManagerImpl{
		producer:       producer,
		consumer:       consumer,
		client:         client,
		offsetTopic:    getOffsetTopic(config),
		pulsarHttpAddr: pulsarHttpAddr,
		offsetMap:      make(map[string]MessageIdPair),
	}
	return &impl, nil
}

func (o *OffsetManagerImpl) Start() chan bool {
	offsetChannel := make(chan bool)
	o.startOffsetConsumer(offsetChannel)
	return offsetChannel
}

func (o *OffsetManagerImpl) startOffsetConsumer(c chan bool) {
	go func() {
		var msg pulsar.Message
		for {
			var err error
			msg, err = o.getCurrentLatestMsg()
			if err != nil {
				continue
			}
			break
		}
		if msg == nil {
			o.startFlag = true
			c <- true
		}
		for receive := range o.consumer.Chan() {
			logrus.Infof("receive key: %s, msg: %s", receive.Key(), string(receive.Payload()))
			payload := receive.Payload()
			publishTime := receive.PublishTime()
			// At present, the three abnormal scenarios can be directly ack
			if err := receive.Ack(receive.Message); err != nil {
				logrus.Errorf("ack message failed. key: %s, topic: %s, error: %s",
					receive.Key(), receive.Topic(), err.Error())
			}
			if len(payload) == 0 {
				o.checkTime(msg, publishTime, c)
				logrus.Errorf("payload length is 0. key: %s", receive.Key())
				o.mutex.Lock()
				delete(o.offsetMap, receive.Key())
				o.mutex.Unlock()
				continue
			}
			var msgIdData model.MessageIdData
			err := json.Unmarshal(payload, &msgIdData)
			if err != nil {
				o.checkTime(msg, publishTime, c)
				logrus.Errorf("unmarshal failed. key: %s, topic: %s", receive.Key(), receive.Topic())
				continue
			}
			idData := msgIdData.MessageId
			msgId, err := pulsar.DeserializeMessageID(idData)
			if err != nil {
				o.checkTime(msg, publishTime, c)
				logrus.Errorf("deserialize message id failed. key: %s, err: %s", receive.Key(), err)
				continue
			}
			pair := MessageIdPair{
				MessageId: msgId,
				Offset:    msgIdData.Offset,
			}
			o.mutex.Lock()
			o.offsetMap[receive.Key()] = pair
			o.mutex.Unlock()
			o.checkTime(msg, publishTime, c)
		}
	}()
}

func (o *OffsetManagerImpl) getCurrentLatestMsg() (pulsar.Message, error) {
	partitionedTopic := o.offsetTopic + fmt.Sprintf(constant.PartitionSuffixFormat, 0)
	msgByte, err := utils.GetLatestMsgId(partitionedTopic, o.pulsarHttpAddr)
	if err != nil {
		logrus.Errorf("get lasted msgId failed. err: %s", err)
		return nil, err
	}
	msg, err := utils.ReadLastedMsg(partitionedTopic, 500, msgByte, o.client)
	if err != nil {
		logrus.Errorf("read Lasted Msg failed. err: %s", err)
		return nil, err
	}
	return msg, nil
}

func (o *OffsetManagerImpl) checkTime(lastMsg pulsar.Message, currentTime time.Time, c chan bool) {
	if lastMsg != nil && (currentTime.Equal(lastMsg.PublishTime()) || currentTime.After(lastMsg.PublishTime())) && !o.startFlag {
		o.startFlag = true
		c <- true
	}
}

func (o *OffsetManagerImpl) CommitOffset(username, kafkaTopic, groupId string, partition int, pair MessageIdPair) error {
	key := o.GenerateKey(username, kafkaTopic, groupId, partition)
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
	key := o.GenerateKey(username, kafkaTopic, groupId, partition)
	o.mutex.RLock()
	pair, exist := o.offsetMap[key]
	o.mutex.RUnlock()
	return pair, exist
}

func (o *OffsetManagerImpl) RemoveOffset(username, kafkaTopic, groupId string, partition int) bool {
	key := o.GenerateKey(username, kafkaTopic, groupId, partition)
	logrus.Infof("begin remove offset key: %s", key)
	message := pulsar.ProducerMessage{}
	message.Key = key
	_, err := o.producer.Send(context.TODO(), &message)
	if err != nil {
		logrus.Errorf("send msg failed. kafkaTopic: %s, err: %s", kafkaTopic, err)
		return false
	}
	logrus.Infof("kafkaTopic: %s remove offset success", kafkaTopic)
	return true
}

func (o *OffsetManagerImpl) Close() {
	o.producer.Close()
	o.consumer.Close()
}

func (o *OffsetManagerImpl) GenerateKey(username, kafkaTopic, groupId string, partition int) string {
	return username + kafkaTopic + groupId + strconv.Itoa(partition)
}

func getOffsetConsumer(client pulsar.Client, config KafsarConfig) (pulsar.Consumer, error) {
	subscribeName := uuid.New().String()
	logrus.Infof("start offset consume subscribe name %s", subscribeName)
	offsetTopic := getOffsetTopic(config)
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       offsetTopic,
		Type:                        pulsar.Failover,
		SubscriptionName:            subscribeName,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		ReadCompacted:               true,
	})
	if err != nil {
		logrus.Errorf("subscribe reader failed. topic: %s, err: %s", offsetTopic, err)
		return nil, err
	}
	return consumer, nil
}

func getOffsetProducer(client pulsar.Client, config KafsarConfig) (pulsar.Producer, error) {
	options := pulsar.ProducerOptions{}
	options.Topic = getOffsetTopic(config)
	options.SendTimeout = constant.DefaultProducerSendTimeout
	options.MaxPendingMessages = constant.DefaultMaxPendingMsg
	options.DisableBlockIfQueueFull = true
	producer, err := client.CreateProducer(options)
	if err != nil {
		logrus.Errorf("create producer failed. topic: %s, err: %s", options.Topic, err)
		return nil, err
	}
	return producer, nil
}

func getOffsetTopic(config KafsarConfig) string {
	return fmt.Sprintf("persistent://%s/%s/%s", config.PulsarTenant, config.PulsarNamespace, config.OffsetTopic)
}
