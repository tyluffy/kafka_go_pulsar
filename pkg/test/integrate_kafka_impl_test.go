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
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	"github.com/paashzj/kafka_go/pkg/service"
	"github.com/paashzj/kafka_go_pulsar/pkg/constant"
	"github.com/paashzj/kafka_go_pulsar/pkg/kafsar"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
)

var (
	username         = "username"
	password         = "password"
	clientId         = "consumer-a1e12365-ddfa-43fc-826e-9661fb54c274-1"
	sessionTimeoutMs = 30000
	protocolType     = "consumer"
	protocol         = service.GroupProtocol{
		ProtocolName:     "range",
		ProtocolMetadata: "000100000001000474657374ffffffff00000000",
	}
	groupProtocol   []*service.GroupProtocol
	protocols       = append(groupProtocol, &protocol)
	partition       = 0
	testClientId    = "consumer-test-client-id"
	testContent     = "test-content"
	addr            = net.IPNet{IP: net.ParseIP("::1")}
	maxFetchWaitMs  = 2000
	maxFetchRecord  = 1
	pulsarHttpUrl   = "http://localhost:8080"
	pulsarClient, _ = pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
	config          = &kafsar.Config{
		KafsarConfig: kafsar.KafsarConfig{
			MaxConsumersPerGroup:     1,
			GroupMinSessionTimeoutMs: 0,
			GroupMaxSessionTimeoutMs: 30000,
			MaxFetchWaitMs:           maxFetchWaitMs,
			MaxFetchRecord:           maxFetchRecord,
			ContinuousOffset:         false,
			PulsarTenant:             "public",
			PulsarNamespace:          "default",
			OffsetTopic:              "kafka_offset",
		},
		PulsarConfig: kafsar.PulsarConfig{
			Host:     "localhost",
			HttpPort: 8080,
			TcpPort:  6650,
		},
	}
	kafsarServer = KafsarImpl{}
)

func TestFetchPartitionNoMessage(t *testing.T) {
	topic := uuid.New().String()
	groupId := uuid.New().String()
	pulsarTopic := topicPrefix + topic
	setupPulsar()
	k, err := kafsar.NewKafsar(kafsarServer, config)
	assert.Nil(t, err)
	defer k.Close()

	// sasl auth
	saslReq := service.SaslReq{
		Username: username,
		Password: password,
		ClientId: clientId,
	}
	auth, errorCode := k.SaslAuth(&addr, saslReq)
	assert.Equal(t, service.NONE, errorCode)
	assert.True(t, true, auth)

	// join group
	joinGroupReq := service.JoinGroupReq{
		ClientId:       clientId,
		GroupId:        groupId,
		SessionTimeout: sessionTimeoutMs,
		ProtocolType:   protocolType,
		GroupProtocols: protocols,
	}
	joinGroupResp, err := k.GroupJoin(&addr, &joinGroupReq)
	assert.Nil(t, err)
	assert.Equal(t, service.NONE, joinGroupResp.ErrorCode)

	// offset fetch
	offsetFetchReq := service.OffsetFetchPartitionReq{
		GroupId:     groupId,
		ClientId:    testClientId,
		PartitionId: partition,
	}
	offset, err := k.OffsetFetch(&addr, topic, &offsetFetchReq)
	assert.Nil(t, err)
	fetchPartitionReq := service.FetchPartitionReq{
		PartitionId: partition,
		FetchOffset: offset.Offset,
		ClientId:    testClientId,
	}
	_, err = k.FetchPartition(&addr, topic, &fetchPartitionReq)
	assert.Nil(t, err)

	url := "http://localhost:8080/admin/v2/persistent/public/default/" + pulsarTopic + fmt.Sprintf(constant.PartitionSuffixFormat, partition) + "/subscriptions"
	request, err := HttpGetRequest(url)
	assert.Nil(t, err)
	assert.Contains(t, string(request), subscriptionPrefix)
}

func TestFetchAndCommitOffset(t *testing.T) {
	topic := uuid.New().String()
	groupId := uuid.New().String()
	pulsarTopic := defaultTopicType + topicPrefix + topic
	setupPulsar()
	k, err := kafsar.NewKafsar(kafsarServer, config)
	if err != nil {
		t.Fatal(err)
	}
	producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: pulsarTopic})
	assert.Nil(t, err)
	message := pulsar.ProducerMessage{Value: testContent}
	messageId, err := producer.Send(context.TODO(), &message)
	logrus.Infof("send msg to pulsar %s", messageId)
	assert.Nil(t, err)

	// sasl auth
	saslReq := service.SaslReq{
		Username: username,
		Password: password,
		ClientId: clientId,
	}
	auth, errorCode := k.SaslAuth(&addr, saslReq)
	assert.Equal(t, service.NONE, errorCode)
	assert.True(t, true, auth)

	// join group
	joinGroupReq := service.JoinGroupReq{
		ClientId:       clientId,
		GroupId:        groupId,
		SessionTimeout: sessionTimeoutMs,
		ProtocolType:   protocolType,
		GroupProtocols: protocols,
	}
	joinGroupResp, err := k.GroupJoin(&addr, &joinGroupReq)
	assert.Nil(t, err)
	assert.Equal(t, service.NONE, joinGroupResp.ErrorCode)

	// offset fetch
	offsetFetchReq := service.OffsetFetchPartitionReq{
		GroupId:     groupId,
		ClientId:    testClientId,
		PartitionId: partition,
	}
	offsetFetchPartitionResp, err := k.OffsetFetch(&addr, topic, &offsetFetchReq)
	assert.Nil(t, err)
	assert.Equal(t, int16(service.NONE), offsetFetchPartitionResp.ErrorCode)

	// fetch partition
	fetchPartitionReq := service.FetchPartitionReq{
		PartitionId: partition,
		FetchOffset: offsetFetchPartitionResp.Offset,
		ClientId:    testClientId,
	}
	fetchPartitionResp, err := k.FetchPartition(&addr, topic, &fetchPartitionReq)
	assert.Nil(t, err)
	assert.Equal(t, service.NONE, fetchPartitionResp.ErrorCode)
	assert.Equal(t, maxFetchRecord, len(fetchPartitionResp.RecordBatch.Records))
	offset := int64(fetchPartitionResp.RecordBatch.Records[0].RelativeOffset) + fetchPartitionResp.RecordBatch.Offset
	assert.Equal(t, string(message.Payload), string(fetchPartitionResp.RecordBatch.Records[0].Value))
	// offset commit
	offsetCommitPartitionReq := service.OffsetCommitPartitionReq{
		ClientId:           clientId,
		PartitionId:        partition,
		OffsetCommitOffset: int64(offset),
	}
	commitPartitionResp, err := k.OffsetCommitPartition(&addr, topic, &offsetCommitPartitionReq)
	assert.Nil(t, err)
	assert.Equal(t, service.NONE, commitPartitionResp.ErrorCode)
}
