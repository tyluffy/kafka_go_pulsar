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
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	"github.com/paashzj/kafka_go_pulsar/pkg/constant"
	"github.com/paashzj/kafka_go_pulsar/pkg/test"
	"github.com/paashzj/kafka_go_pulsar/pkg/utils"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
	"time"
)

var (
	username       = "username"
	password       = "password"
	partition      = 0
	testContent    = "test-content"
	addr           = net.IPNet{IP: net.ParseIP("::1")}
	maxBytes       = 1024 * 1024 * 5
	minBytes       = 1024 * 1024
	maxFetchWaitMs = 2000
	maxFetchRecord = 1
	config         = &Config{
		KafsarConfig: KafsarConfig{
			MaxConsumersPerGroup:     1,
			GroupMinSessionTimeoutMs: 0,
			GroupMaxSessionTimeoutMs: 30000,
			MinFetchWaitMs:           10,
			MaxFetchWaitMs:           maxFetchWaitMs,
			MaxFetchRecord:           maxFetchRecord,
			ContinuousOffset:         false,
			PulsarTenant:             "public",
			PulsarNamespace:          "default",
			OffsetTopic:              "kafka_offset",
		},
		PulsarConfig: PulsarConfig{
			Host:     "localhost",
			HttpPort: 8080,
			TcpPort:  6650,
		},
	}
	MaxFetchConfig = &Config{
		KafsarConfig: KafsarConfig{
			MaxConsumersPerGroup:     1,
			GroupMinSessionTimeoutMs: 0,
			GroupMaxSessionTimeoutMs: 30000,
			MinFetchWaitMs:           1000,
			MaxFetchWaitMs:           maxFetchWaitMs,
			MaxFetchRecord:           100,
			ContinuousOffset:         false,
			PulsarTenant:             "public",
			PulsarNamespace:          "default",
			OffsetTopic:              "kafka_offset",
		},
		PulsarConfig: PulsarConfig{
			Host:     "localhost",
			HttpPort: 8080,
			TcpPort:  6650,
		},
	}

	kafsarServer = test.KafsarImpl{}
)

func TestFetchPartitionNoMessage(t *testing.T) {
	topic := uuid.New().String()
	groupId := uuid.New().String()
	pulsarTopic := test.TopicPrefix + topic
	test.SetupPulsar()
	k, err := NewKafsar(kafsarServer, config)
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()

	// sasl auth
	saslReq := codec.SaslAuthenticateReq{
		Username: username,
		Password: password,
		BaseReq:  codec.BaseReq{ClientId: clientId},
	}
	auth, errorCode := k.SaslAuth(&addr, saslReq)
	assert.Equal(t, codec.NONE, errorCode)
	assert.True(t, true, auth)

	// join group
	joinGroupReq := codec.JoinGroupReq{
		BaseReq:        codec.BaseReq{ClientId: clientId},
		GroupId:        groupId,
		SessionTimeout: sessionTimeoutMs,
		ProtocolType:   protocolType,
		GroupProtocols: protocols,
		MemberId:       "",
	}
	joinGroupResp, err := k.GroupJoin(&addr, &joinGroupReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, joinGroupResp.ErrorCode)

	// offset fetch
	offsetFetchReq := codec.OffsetFetchPartitionReq{
		PartitionId: partition,
	}
	offset, err := k.OffsetFetch(&addr, topic, clientId, groupId, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	fetchPartitionReq := codec.FetchPartitionReq{
		PartitionId: partition,
		FetchOffset: offset.Offset,
	}
	k.FetchPartition(&addr, topic, clientId, &fetchPartitionReq, maxBytes, minBytes, 200, LocalSpan{})

	url := "http://localhost:8080/admin/v2/persistent/public/default/" + pulsarTopic + fmt.Sprintf(constant.PartitionSuffixFormat, partition) + "/subscriptions"
	request, err := test.HttpGetRequest(url)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(request), groupId)
}

func TestFetchAndCommitOffset(t *testing.T) {
	topic := uuid.New().String()
	groupId := uuid.New().String()
	pulsarTopic := utils.PartitionedTopic(test.DefaultTopicType+test.TopicPrefix+topic, partition)
	test.SetupPulsar()
	k, err := NewKafsar(kafsarServer, config)
	if err != nil {
		t.Fatal(err)
	}
	pulsarClient := test.NewPulsarClient()
	defer pulsarClient.Close()
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

	// sasl auth
	saslReq := codec.SaslAuthenticateReq{
		Username: username,
		Password: password,
		BaseReq:  codec.BaseReq{ClientId: clientId},
	}
	auth, errorCode := k.SaslAuth(&addr, saslReq)
	assert.Equal(t, codec.NONE, errorCode)
	assert.True(t, true, auth)

	// join group
	joinGroupReq := codec.JoinGroupReq{
		BaseReq:        codec.BaseReq{ClientId: clientId},
		GroupId:        groupId,
		SessionTimeout: sessionTimeoutMs,
		ProtocolType:   protocolType,
		GroupProtocols: protocols,
	}
	joinGroupResp, err := k.GroupJoin(&addr, &joinGroupReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, joinGroupResp.ErrorCode)

	// offset fetch
	offsetFetchReq := codec.OffsetFetchPartitionReq{
		PartitionId: partition,
	}
	offsetFetchPartitionResp, err := k.OffsetFetch(&addr, topic, clientId, groupId, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, offsetFetchPartitionResp.ErrorCode)

	// fetch partition
	fetchPartitionReq := codec.FetchPartitionReq{
		PartitionId: partition,
		FetchOffset: offsetFetchPartitionResp.Offset,
	}
	fetchPartitionResp := k.FetchPartition(&addr, topic, clientId, &fetchPartitionReq, maxBytes, minBytes, 2000, LocalSpan{})
	assert.Equal(t, codec.NONE, fetchPartitionResp.ErrorCode)
	assert.Equal(t, maxFetchRecord, len(fetchPartitionResp.RecordBatch.Records))
	offset := int64(fetchPartitionResp.RecordBatch.Records[0].RelativeOffset) + fetchPartitionResp.RecordBatch.Offset
	assert.Equal(t, string(message.Payload), string(fetchPartitionResp.RecordBatch.Records[0].Value))
	// offset commit
	offsetCommitPartitionReq := codec.OffsetCommitPartitionReq{
		PartitionId: partition,
		Offset:      offset,
	}
	commitPartitionResp, err := k.OffsetCommitPartition(&addr, topic, clientId, &offsetCommitPartitionReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, commitPartitionResp.ErrorCode)
	// acquire offset
	time.Sleep(5 * time.Second)
	acquireOffset, b := k.GetOffsetManager().AcquireOffset(username, topic, groupId, partition)
	assert.True(t, b)
	assert.Equal(t, acquireOffset.Offset, offset)
}

func TestFetchOffsetAndOffsetCommit(t *testing.T) {
	topic := uuid.New().String()
	groupId := uuid.New().String()
	pulsarTopic := utils.PartitionedTopic(test.DefaultTopicType+test.TopicPrefix+topic, partition)
	test.SetupPulsar()
	k, err := NewKafsar(kafsarServer, config)
	if err != nil {
		t.Fatal(err)
	}
	pulsarClient := test.NewPulsarClient()
	defer pulsarClient.Close()
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

	// sasl auth
	saslReq := codec.SaslAuthenticateReq{
		Username: username,
		Password: password,
		BaseReq:  codec.BaseReq{ClientId: clientId},
	}
	auth, errorCode := k.SaslAuth(&addr, saslReq)
	assert.Equal(t, codec.NONE, errorCode)
	assert.True(t, true, auth)

	// join group
	joinGroupReq := codec.JoinGroupReq{
		BaseReq:        codec.BaseReq{ClientId: clientId},
		GroupId:        groupId,
		SessionTimeout: sessionTimeoutMs,
		ProtocolType:   protocolType,
		GroupProtocols: protocols,
	}
	joinGroupResp, err := k.GroupJoin(&addr, &joinGroupReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, joinGroupResp.ErrorCode)

	// offset fetch
	offsetFetchReq := codec.OffsetFetchPartitionReq{
		PartitionId: partition,
	}
	offsetFetchPartitionResp, err := k.OffsetFetch(&addr, topic, clientId, groupId, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, offsetFetchPartitionResp.ErrorCode)

	// fetch partition
	fetchPartitionReq := codec.FetchPartitionReq{
		PartitionId: partition,
		FetchOffset: offsetFetchPartitionResp.Offset,
	}
	fetchPartitionResp := k.FetchPartition(&addr, topic, clientId, &fetchPartitionReq, maxBytes, minBytes, 2000, LocalSpan{})
	assert.Equal(t, codec.NONE, fetchPartitionResp.ErrorCode)
	assert.Equal(t, maxFetchRecord, len(fetchPartitionResp.RecordBatch.Records))
	offset := int64(fetchPartitionResp.RecordBatch.Records[0].RelativeOffset) + fetchPartitionResp.RecordBatch.Offset
	logrus.Infof("offset is : %d", offset)
	assert.Equal(t, string(message.Payload), string(fetchPartitionResp.RecordBatch.Records[0].Value))
	// offset commit
	offsetCommitPartitionReq := codec.OffsetCommitPartitionReq{
		PartitionId: partition,
		Offset:      offset,
	}
	commitPartitionResp, err := k.OffsetCommitPartition(&addr, topic, clientId, &offsetCommitPartitionReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, commitPartitionResp.ErrorCode)
	time.Sleep(5 * time.Second)
	// leave group
	member := []*codec.LeaveGroupMember{{
		MemberId: joinGroupResp.MemberId,
	}}
	req := codec.LeaveGroupReq{
		BaseReq: codec.BaseReq{ClientId: clientId},
		GroupId: groupId,
		Members: member,
	}
	leave, err := k.GroupLeave(&addr, &req)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, leave.ErrorCode, codec.NONE)

	// send message again
	message = pulsar.ProducerMessage{Value: testContent}
	messageId, err = producer.Send(context.TODO(), &message)
	if err != nil {
		t.Fatal(err)
	}
	logrus.Infof("send msg to pulsar %s", messageId)

	joinGroupResp, err = k.GroupJoin(&addr, &joinGroupReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, joinGroupResp.ErrorCode)

	// offset fetch
	offsetFetchReq = codec.OffsetFetchPartitionReq{
		PartitionId: partition,
	}
	offsetFetchPartitionResp, err = k.OffsetFetch(&addr, topic, clientId, groupId, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, offsetFetchPartitionResp.ErrorCode)

	fetchPartitionResp = k.FetchPartition(&addr, topic, clientId, &fetchPartitionReq, maxBytes, minBytes, 2000, LocalSpan{})
	assert.Equal(t, codec.NONE, fetchPartitionResp.ErrorCode)
	assert.Equal(t, maxFetchRecord, len(fetchPartitionResp.RecordBatch.Records))
	assert.Equal(t, string(message.Payload), string(fetchPartitionResp.RecordBatch.Records[0].Value))

	fetchPartitionResp = k.FetchPartition(&addr, topic, clientId, &fetchPartitionReq, maxBytes, minBytes, 2000, LocalSpan{})
	assert.Equal(t, codec.NONE, fetchPartitionResp.ErrorCode)
	assert.Equal(t, 1, len(fetchPartitionResp.RecordBatch.Records))
	offset = int64(fetchPartitionResp.RecordBatch.Records[0].RelativeOffset) + fetchPartitionResp.RecordBatch.Offset

	// offset commit
	offsetCommitPartitionReq = codec.OffsetCommitPartitionReq{
		PartitionId: partition,
		Offset:      offset,
	}
	commitPartitionResp, err = k.OffsetCommitPartition(&addr, topic, clientId, &offsetCommitPartitionReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, commitPartitionResp.ErrorCode)
	// acquire offset
	time.Sleep(5 * time.Second)
	acquireOffset, b := k.GetOffsetManager().AcquireOffset(username, topic, groupId, partition)
	assert.True(t, b)
	assert.Equal(t, acquireOffset.Offset, ConvertMsgId(messageId))
}

func TestEarliestMsg(t *testing.T) {
	topic := uuid.New().String()
	groupId := uuid.New().String()
	pulsarTopic := utils.PartitionedTopic(test.DefaultTopicType+test.TopicPrefix+topic, partition)
	test.SetupPulsar()
	k, err := NewKafsar(kafsarServer, config)
	if err != nil {
		t.Fatal(err)
	}
	pulsarClient := test.NewPulsarClient()
	defer pulsarClient.Close()
	producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: pulsarTopic})
	if err != nil {
		t.Fatal(err)
	}
	message := pulsar.ProducerMessage{Value: testContent}
	earliestMessageId, err := producer.Send(context.TODO(), &message)
	if err != nil {
		t.Fatal(err)
	}

	message = pulsar.ProducerMessage{Value: testContent}
	latestMessageId, err := producer.Send(context.TODO(), &message)
	if err != nil {
		t.Fatal(err)
	}
	logrus.Infof("send msg to pulsar %s", latestMessageId)
	// sasl auth
	saslReq := codec.SaslAuthenticateReq{
		Username: username,
		Password: password,
		BaseReq:  codec.BaseReq{ClientId: clientId},
	}
	auth, errorCode := k.SaslAuth(&addr, saslReq)
	assert.Equal(t, codec.NONE, errorCode)
	assert.True(t, true, auth)

	// join group
	joinGroupReq := codec.JoinGroupReq{
		BaseReq:        codec.BaseReq{ClientId: clientId},
		GroupId:        groupId,
		SessionTimeout: sessionTimeoutMs,
		ProtocolType:   protocolType,
		GroupProtocols: protocols,
	}
	joinGroupResp, err := k.GroupJoin(&addr, &joinGroupReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, joinGroupResp.ErrorCode)

	// offset fetch
	offsetFetchReq := codec.OffsetFetchPartitionReq{
		PartitionId: partition,
	}
	offsetFetchPartitionResp, err := k.OffsetFetch(&addr, topic, clientId, groupId, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, offsetFetchPartitionResp.ErrorCode)

	// offset fetch
	listOffset := codec.ListOffsetsPartition{
		Time:        constant.TimeEarliest,
		PartitionId: partition,
	}
	listPartition, err := k.OffsetListPartition(&addr, topic, clientId, &listOffset)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, listPartition.ErrorCode)

	// fetch partition
	fetchPartitionReq := codec.FetchPartitionReq{
		PartitionId: partition,
		FetchOffset: offsetFetchPartitionResp.Offset,
	}
	fetchPartitionResp := k.FetchPartition(&addr, topic, clientId, &fetchPartitionReq, maxBytes, minBytes, 2000, LocalSpan{})
	assert.Equal(t, codec.NONE, fetchPartitionResp.ErrorCode)
	assert.Equal(t, maxFetchRecord, len(fetchPartitionResp.RecordBatch.Records))
	offset := int64(fetchPartitionResp.RecordBatch.Records[0].RelativeOffset) + fetchPartitionResp.RecordBatch.Offset
	assert.Equal(t, string(message.Payload), string(fetchPartitionResp.RecordBatch.Records[0].Value))
	// offset commit
	offsetCommitPartitionReq := codec.OffsetCommitPartitionReq{
		PartitionId: partition,
		Offset:      offset,
	}
	commitPartitionResp, err := k.OffsetCommitPartition(&addr, topic, clientId, &offsetCommitPartitionReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, commitPartitionResp.ErrorCode)
	// acquire offset
	time.Sleep(5 * time.Second)
	acquireOffset, b := k.GetOffsetManager().AcquireOffset(username, topic, groupId, partition)
	assert.True(t, b)
	assert.Equal(t, acquireOffset.Offset, ConvertMsgId(earliestMessageId))
}

func TestLatestMsg(t *testing.T) {
	topic := uuid.New().String()
	groupId := uuid.New().String()
	pulsarTopic := utils.PartitionedTopic(test.DefaultTopicType+test.TopicPrefix+topic, partition)
	test.SetupPulsar()
	k, err := NewKafsar(kafsarServer, config)
	if err != nil {
		t.Fatal(err)
	}
	pulsarClient := test.NewPulsarClient()
	defer pulsarClient.Close()
	producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: pulsarTopic})
	if err != nil {
		t.Fatal(err)
	}
	message := pulsar.ProducerMessage{Value: testContent}
	earliestMessageId, err := producer.Send(context.TODO(), &message)
	if err != nil {
		t.Fatal(err)
	}
	logrus.Infof("send msg to pulsar %s", earliestMessageId)

	message = pulsar.ProducerMessage{Value: testContent}
	latestMessageId, err := producer.Send(context.TODO(), &message)
	if err != nil {
		t.Fatal(err)
	}
	logrus.Infof("send msg to pulsar %s", latestMessageId)
	// sasl auth
	saslReq := codec.SaslAuthenticateReq{
		Username: username,
		Password: password,
		BaseReq:  codec.BaseReq{ClientId: clientId},
	}
	auth, errorCode := k.SaslAuth(&addr, saslReq)
	assert.Equal(t, codec.NONE, errorCode)
	assert.True(t, true, auth)

	// join group
	joinGroupReq := codec.JoinGroupReq{
		BaseReq:        codec.BaseReq{ClientId: clientId},
		GroupId:        groupId,
		SessionTimeout: sessionTimeoutMs,
		ProtocolType:   protocolType,
		GroupProtocols: protocols,
	}
	joinGroupResp, err := k.GroupJoin(&addr, &joinGroupReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, joinGroupResp.ErrorCode)

	// offset fetch
	offsetFetchReq := codec.OffsetFetchPartitionReq{
		PartitionId: partition,
	}
	offsetFetchPartitionResp, err := k.OffsetFetch(&addr, topic, clientId, groupId, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, offsetFetchPartitionResp.ErrorCode)

	// offset fetch
	listOffset := codec.ListOffsetsPartition{
		Time:        constant.TimeLasted,
		PartitionId: partition,
	}
	listPartition, err := k.OffsetListPartition(&addr, topic, clientId, &listOffset)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, listPartition.ErrorCode)

	// fetch partition
	fetchPartitionReq := codec.FetchPartitionReq{
		PartitionId: partition,
		FetchOffset: offsetFetchPartitionResp.Offset,
	}
	fetchPartitionResp := k.FetchPartition(&addr, topic, clientId, &fetchPartitionReq, maxBytes, minBytes, 2000, LocalSpan{})
	assert.Equal(t, codec.NONE, fetchPartitionResp.ErrorCode)
	assert.Equal(t, maxFetchRecord, len(fetchPartitionResp.RecordBatch.Records))
	offset := int64(fetchPartitionResp.RecordBatch.Records[0].RelativeOffset) + fetchPartitionResp.RecordBatch.Offset
	assert.Equal(t, string(message.Payload), string(fetchPartitionResp.RecordBatch.Records[0].Value))
	// offset commit
	offsetCommitPartitionReq := codec.OffsetCommitPartitionReq{
		PartitionId: partition,
		Offset:      offset,
	}
	commitPartitionResp, err := k.OffsetCommitPartition(&addr, topic, clientId, &offsetCommitPartitionReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, commitPartitionResp.ErrorCode)
	// acquire offset
	time.Sleep(5 * time.Second)
	acquireOffset, b := k.GetOffsetManager().AcquireOffset(username, topic, groupId, partition)
	assert.True(t, b)
	assert.Equal(t, acquireOffset.Offset, ConvertMsgId(latestMessageId))
}

func TestLatestTypeWithNoMsg(t *testing.T) {
	topic := uuid.New().String()
	groupId := uuid.New().String()
	test.SetupPulsar()
	k, err := NewKafsar(kafsarServer, config)
	if err != nil {
		t.Fatal(err)
	}
	// sasl auth
	saslReq := codec.SaslAuthenticateReq{
		Username: username,
		Password: password,
		BaseReq:  codec.BaseReq{ClientId: clientId},
	}
	auth, errorCode := k.SaslAuth(&addr, saslReq)
	assert.Equal(t, codec.NONE, errorCode)
	assert.True(t, true, auth)

	// join group
	joinGroupReq := codec.JoinGroupReq{
		BaseReq:        codec.BaseReq{ClientId: clientId},
		GroupId:        groupId,
		SessionTimeout: sessionTimeoutMs,
		ProtocolType:   protocolType,
		GroupProtocols: protocols,
	}
	joinGroupResp, err := k.GroupJoin(&addr, &joinGroupReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, joinGroupResp.ErrorCode)

	// offset fetch
	offsetFetchReq := codec.OffsetFetchPartitionReq{
		PartitionId: partition,
	}
	offsetFetchPartitionResp, err := k.OffsetFetch(&addr, topic, clientId, groupId, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, offsetFetchPartitionResp.ErrorCode)

	// offset fetch
	listOffset := codec.ListOffsetsPartition{
		Time:        constant.TimeLasted,
		PartitionId: partition,
	}
	listPartition, err := k.OffsetListPartition(&addr, topic, clientId, &listOffset)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, listPartition.ErrorCode)
}

func TestEarliestTypeWithNoMsg(t *testing.T) {
	topic := uuid.New().String()
	groupId := uuid.New().String()
	test.SetupPulsar()
	k, err := NewKafsar(kafsarServer, config)
	if err != nil {
		t.Fatal(err)
	}
	// sasl auth
	saslReq := codec.SaslAuthenticateReq{
		Username: username,
		Password: password,
		BaseReq:  codec.BaseReq{ClientId: clientId},
	}
	auth, errorCode := k.SaslAuth(&addr, saslReq)
	assert.Equal(t, codec.NONE, errorCode)
	assert.True(t, true, auth)

	// join group
	joinGroupReq := codec.JoinGroupReq{
		BaseReq:        codec.BaseReq{ClientId: clientId},
		GroupId:        groupId,
		SessionTimeout: sessionTimeoutMs,
		ProtocolType:   protocolType,
		GroupProtocols: protocols,
	}
	joinGroupResp, err := k.GroupJoin(&addr, &joinGroupReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, joinGroupResp.ErrorCode)

	// offset fetch
	offsetFetchReq := codec.OffsetFetchPartitionReq{
		PartitionId: partition,
	}
	offsetFetchPartitionResp, err := k.OffsetFetch(&addr, topic, clientId, groupId, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, offsetFetchPartitionResp.ErrorCode)

	// offset fetch
	listOffset := codec.ListOffsetsPartition{
		Time:        constant.TimeEarliest,
		PartitionId: partition,
	}
	listPartition, err := k.OffsetListPartition(&addr, topic, clientId, &listOffset)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, listPartition.ErrorCode)
}

func TestMinBytesMsg(t *testing.T) {
	topic := uuid.New().String()
	groupId := uuid.New().String()
	pulsarTopic := utils.PartitionedTopic(test.DefaultTopicType+test.TopicPrefix+topic, partition)
	test.SetupPulsar()
	k, err := NewKafsar(kafsarServer, config)
	if err != nil {
		t.Fatal(err)
	}
	pulsarClient := test.NewPulsarClient()
	defer pulsarClient.Close()
	producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: pulsarTopic})
	if err != nil {
		t.Fatal(err)
	}
	message := pulsar.ProducerMessage{Value: "testMsg1"}
	earliestMessageId, err := producer.Send(context.TODO(), &message)
	if err != nil {
		t.Fatal(err)
	}
	logrus.Infof("send msg to pulsar %s", earliestMessageId)

	message = pulsar.ProducerMessage{Value: "TestMsg2"}
	latestMessageId, err := producer.Send(context.TODO(), &message)
	if err != nil {
		t.Fatal(err)
	}
	logrus.Infof("send msg to pulsar %s", latestMessageId)
	// sasl auth
	saslReq := codec.SaslAuthenticateReq{
		Username: username,
		Password: password,
		BaseReq:  codec.BaseReq{ClientId: clientId},
	}
	auth, errorCode := k.SaslAuth(&addr, saslReq)
	assert.Equal(t, codec.NONE, errorCode)
	assert.True(t, true, auth)

	// join group
	joinGroupReq := codec.JoinGroupReq{
		BaseReq:        codec.BaseReq{ClientId: clientId},
		GroupId:        groupId,
		SessionTimeout: sessionTimeoutMs,
		ProtocolType:   protocolType,
		GroupProtocols: protocols,
	}
	joinGroupResp, err := k.GroupJoin(&addr, &joinGroupReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, joinGroupResp.ErrorCode)

	// offset fetch
	offsetFetchReq := codec.OffsetFetchPartitionReq{
		PartitionId: partition,
	}
	offsetFetchPartitionResp, err := k.OffsetFetch(&addr, topic, clientId, groupId, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, offsetFetchPartitionResp.ErrorCode)

	// offset fetch
	listOffset := codec.ListOffsetsPartition{
		Time:        constant.TimeLasted,
		PartitionId: partition,
	}
	listPartition, err := k.OffsetListPartition(&addr, topic, clientId, &listOffset)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, listPartition.ErrorCode)

	// fetch partition
	fetchPartitionReq := codec.FetchPartitionReq{
		PartitionId: partition,
		FetchOffset: offsetFetchPartitionResp.Offset,
	}
	testMinBytes := 1
	fetchPartitionResp := k.FetchPartition(&addr, topic, clientId, &fetchPartitionReq, maxBytes, testMinBytes, 2000, LocalSpan{})
	assert.Equal(t, codec.NONE, fetchPartitionResp.ErrorCode)
	assert.Equal(t, 1, len(fetchPartitionResp.RecordBatch.Records))
}

func TestMaxBytesMsg(t *testing.T) {
	topic := uuid.New().String()
	groupId := uuid.New().String()
	pulsarTopic := utils.PartitionedTopic(test.DefaultTopicType+test.TopicPrefix+topic, partition)
	test.SetupPulsar()
	k, err := NewKafsar(kafsarServer, MaxFetchConfig)
	if err != nil {
		t.Fatal(err)
	}
	pulsarClient := test.NewPulsarClient()
	defer pulsarClient.Close()
	producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: pulsarTopic})
	if err != nil {
		t.Fatal(err)
	}
	message := pulsar.ProducerMessage{Payload: []byte("testMsg1")}
	earliestMessageId, err := producer.Send(context.TODO(), &message)
	if err != nil {
		t.Fatal(err)
	}
	logrus.Infof("send msg to pulsar %s", earliestMessageId)

	message = pulsar.ProducerMessage{Payload: []byte("TestMsg2")}
	latestMessageId, err := producer.Send(context.TODO(), &message)
	if err != nil {
		t.Fatal(err)
	}
	logrus.Infof("send msg to pulsar %s", latestMessageId)
	// sasl auth
	saslReq := codec.SaslAuthenticateReq{
		Username: username,
		Password: password,
		BaseReq:  codec.BaseReq{ClientId: clientId},
	}
	auth, errorCode := k.SaslAuth(&addr, saslReq)
	assert.Equal(t, codec.NONE, errorCode)
	assert.True(t, true, auth)

	// join group
	joinGroupReq := codec.JoinGroupReq{
		BaseReq:        codec.BaseReq{ClientId: clientId},
		GroupId:        groupId,
		SessionTimeout: sessionTimeoutMs,
		ProtocolType:   protocolType,
		GroupProtocols: protocols,
	}
	joinGroupResp, err := k.GroupJoin(&addr, &joinGroupReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, joinGroupResp.ErrorCode)

	// offset fetch
	offsetFetchReq := codec.OffsetFetchPartitionReq{
		PartitionId: partition,
	}
	offsetFetchPartitionResp, err := k.OffsetFetch(&addr, topic, clientId, groupId, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, offsetFetchPartitionResp.ErrorCode)

	// offset fetch
	listOffset := codec.ListOffsetsPartition{
		Time:        constant.TimeEarliest,
		PartitionId: partition,
	}
	listPartition, err := k.OffsetListPartition(&addr, topic, clientId, &listOffset)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, listPartition.ErrorCode)

	// fetch partition
	fetchPartitionReq := codec.FetchPartitionReq{
		PartitionId: partition,
		FetchOffset: offsetFetchPartitionResp.Offset,
	}
	testMinBytes := 1
	maxBytes := 500
	fetchPartitionResp := k.FetchPartition(&addr, topic, clientId, &fetchPartitionReq, maxBytes, testMinBytes, 5000, LocalSpan{})
	assert.Equal(t, codec.NONE, fetchPartitionResp.ErrorCode)
	assert.Equal(t, 2, len(fetchPartitionResp.RecordBatch.Records))
}

func TestMultiMemberLeaveGroup(t *testing.T) {
	topic := uuid.New().String()
	groupId := uuid.New().String()
	newConfig := config
	newConfig.KafsarConfig.MaxConsumersPerGroup = 10
	k, err := NewKafsar(kafsarServer, newConfig)
	if err != nil {
		t.Fatal(err)
	}
	// sasl auth
	saslReq := codec.SaslAuthenticateReq{
		Username: username,
		Password: password,
		BaseReq:  codec.BaseReq{ClientId: clientId},
	}
	auth, errorCode := k.SaslAuth(&addr, saslReq)
	assert.Equal(t, codec.NONE, errorCode)
	assert.True(t, true, auth)

	// join group
	joinGroupReq := codec.JoinGroupReq{
		BaseReq:        codec.BaseReq{ClientId: clientId},
		GroupId:        groupId,
		SessionTimeout: sessionTimeoutMs,
		ProtocolType:   protocolType,
		GroupProtocols: protocols,
	}
	joinGroupResp, err := k.GroupJoin(&addr, &joinGroupReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, joinGroupResp.ErrorCode)
	// sync group
	groupAssignments := make([]*codec.GroupAssignment, 1)
	g := &codec.GroupAssignment{}
	g.MemberId = joinGroupResp.MemberId
	g.MemberAssignment = []byte("testAssignment: " + joinGroupResp.MemberId)
	groupAssignments[0] = g

	syncReq := codec.SyncGroupReq{
		BaseReq:          codec.BaseReq{ClientId: clientId},
		GroupId:          groupId,
		GenerationId:     joinGroupResp.GenerationId,
		MemberId:         joinGroupResp.MemberId,
		GroupAssignments: groupAssignments,
	}
	_, err = k.GroupSync(&addr, &syncReq)
	if err != nil {
		t.Fatal(err)
	}

	// other join group
	go func() {
		joinGroupReq2 := codec.JoinGroupReq{
			BaseReq:        codec.BaseReq{ClientId: clientId},
			GroupId:        groupId,
			SessionTimeout: sessionTimeoutMs,
			ProtocolType:   protocolType,
			GroupProtocols: protocols,
		}
		joinGroupResp2, err := k.GroupJoin(&addr, &joinGroupReq2)
		assert.Nil(t, err)
		assert.Equal(t, codec.NONE, joinGroupResp2.ErrorCode)
	}()

	// the first one join
	joinGroupReq3 := codec.JoinGroupReq{
		BaseReq:        codec.BaseReq{ClientId: clientId},
		GroupId:        groupId,
		SessionTimeout: sessionTimeoutMs,
		ProtocolType:   protocolType,
		GroupProtocols: protocols,
		MemberId:       joinGroupResp.MemberId,
	}
	_, err = k.GroupJoin(&addr, &joinGroupReq3)
	if err != nil {
		t.Fatal(err)
	}

	// offset fetch
	offsetFetchReq := codec.OffsetFetchPartitionReq{
		PartitionId: partition,
	}
	offsetFetchPartitionResp, err := k.OffsetFetch(&addr, topic, clientId, groupId, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, offsetFetchPartitionResp.ErrorCode)

	var members []*codec.LeaveGroupMember
	leaveGroupMembers := append(members, &codec.LeaveGroupMember{
		MemberId: joinGroupResp.MemberId,
	})

	leaveGroupResp, err := k.GroupLeave(&addr, &codec.LeaveGroupReq{BaseReq: codec.BaseReq{ClientId: clientId}, GroupId: groupId, Members: leaveGroupMembers})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, leaveGroupResp.ErrorCode)

	var members2 []*codec.LeaveGroupMember
	leaveGroupMembers2 := append(members2, &codec.LeaveGroupMember{
		MemberId: joinGroupResp.MemberId,
	})

	leaveGroupResp2, err := k.GroupLeave(&addr, &codec.LeaveGroupReq{BaseReq: codec.BaseReq{ClientId: clientId}, GroupId: groupId, Members: leaveGroupMembers2})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, leaveGroupResp2.ErrorCode)
}

func TestFetchAfterDisConnect(t *testing.T) {
	topic := uuid.New().String()
	groupId := uuid.New().String()
	pulsarTopic := utils.PartitionedTopic(test.DefaultTopicType+test.TopicPrefix+topic, partition)
	test.SetupPulsar()
	k, err := NewKafsar(kafsarServer, config)
	if err != nil {
		t.Fatal(err)
	}
	pulsarClient := test.NewPulsarClient()
	defer pulsarClient.Close()
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

	// sasl auth
	saslReq := codec.SaslAuthenticateReq{
		Username: username,
		Password: password,
		BaseReq:  codec.BaseReq{ClientId: clientId},
	}
	auth, errorCode := k.SaslAuth(&addr, saslReq)
	assert.Equal(t, codec.NONE, errorCode)
	assert.True(t, true, auth)

	// join group
	joinGroupReq := codec.JoinGroupReq{
		BaseReq:        codec.BaseReq{ClientId: clientId},
		GroupId:        groupId,
		SessionTimeout: sessionTimeoutMs,
		ProtocolType:   protocolType,
		GroupProtocols: protocols,
	}
	joinGroupResp, err := k.GroupJoin(&addr, &joinGroupReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, joinGroupResp.ErrorCode)

	// offset fetch
	offsetFetchReq := codec.OffsetFetchPartitionReq{
		PartitionId: partition,
	}
	offsetFetchPartitionResp, err := k.OffsetFetch(&addr, topic, clientId, groupId, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, offsetFetchPartitionResp.ErrorCode)

	// fetch partition
	fetchPartitionReq := codec.FetchPartitionReq{
		PartitionId: partition,
		FetchOffset: offsetFetchPartitionResp.Offset,
	}
	fetchPartitionResp := k.FetchPartition(&addr, topic, clientId, &fetchPartitionReq, maxBytes, minBytes, 2000, LocalSpan{})
	assert.Equal(t, codec.NONE, fetchPartitionResp.ErrorCode)
	assert.Equal(t, maxFetchRecord, len(fetchPartitionResp.RecordBatch.Records))
	offset := int64(fetchPartitionResp.RecordBatch.Records[0].RelativeOffset) + fetchPartitionResp.RecordBatch.Offset
	assert.Equal(t, string(message.Payload), string(fetchPartitionResp.RecordBatch.Records[0].Value))
	// offset commit
	offsetCommitPartitionReq := codec.OffsetCommitPartitionReq{
		PartitionId: partition,
		Offset:      offset,
	}
	commitPartitionResp, err := k.OffsetCommitPartition(&addr, topic, clientId, &offsetCommitPartitionReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, commitPartitionResp.ErrorCode)
	// acquire offset
	time.Sleep(5 * time.Second)
	acquireOffset, b := k.GetOffsetManager().AcquireOffset(username, topic, groupId, partition)
	assert.True(t, b)
	assert.Equal(t, acquireOffset.Offset, offset)
	k.Disconnect(&addr)
	time.Sleep(1 * time.Second)

	message = pulsar.ProducerMessage{Value: testContent}
	messageId, err = producer.Send(context.TODO(), &message)
	if err != nil {
		t.Fatal(err)
	}
	logrus.Infof("send msg to pulsar %s", messageId)

	auth, errorCode = k.SaslAuth(&addr, saslReq)
	assert.Equal(t, codec.NONE, errorCode)
	assert.True(t, true, auth)

	joinGroupResp, err = k.GroupJoin(&addr, &joinGroupReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, joinGroupResp.ErrorCode)

	offsetFetchPartitionResp, err = k.OffsetFetch(&addr, topic, clientId, groupId, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, offsetFetchPartitionResp.ErrorCode)

	fetchPartitionResp = k.FetchPartition(&addr, topic, clientId, &fetchPartitionReq, maxBytes, minBytes, 2000, LocalSpan{})
	assert.Equal(t, codec.NONE, fetchPartitionResp.ErrorCode)
	assert.Equal(t, maxFetchRecord, len(fetchPartitionResp.RecordBatch.Records))
	assert.Equal(t, string(message.Payload), string(fetchPartitionResp.RecordBatch.Records[0].Value))
}

func TestMsgWithFlowQuota(t *testing.T) {
	topic := uuid.New().String()
	groupId := uuid.New().String()
	pulsarTopic := utils.PartitionedTopic(test.DefaultTopicType+test.TopicPrefix+topic, partition)
	test.SetupPulsar()
	k, err := NewKafsar(test.FlowKafsarImpl{}, MaxFetchConfig)
	if err != nil {
		t.Fatal(err)
	}
	pulsarClient := test.NewPulsarClient()
	defer pulsarClient.Close()
	producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: pulsarTopic})
	if err != nil {
		t.Fatal(err)
	}
	message := pulsar.ProducerMessage{Payload: []byte("testMsg1")}
	earliestMessageId, err := producer.Send(context.TODO(), &message)
	if err != nil {
		t.Fatal(err)
	}
	logrus.Infof("send msg to pulsar %s", earliestMessageId)

	message = pulsar.ProducerMessage{Payload: []byte("TestMsg2")}
	latestMessageId, err := producer.Send(context.TODO(), &message)
	if err != nil {
		t.Fatal(err)
	}
	logrus.Infof("send msg to pulsar %s", latestMessageId)
	// sasl auth
	saslReq := codec.SaslAuthenticateReq{
		Username: username,
		Password: password,
		BaseReq:  codec.BaseReq{ClientId: clientId},
	}
	auth, errorCode := k.SaslAuth(&addr, saslReq)
	assert.Equal(t, codec.NONE, errorCode)
	assert.True(t, true, auth)

	// join group
	joinGroupReq := codec.JoinGroupReq{
		BaseReq:        codec.BaseReq{ClientId: clientId},
		GroupId:        groupId,
		SessionTimeout: sessionTimeoutMs,
		ProtocolType:   protocolType,
		GroupProtocols: protocols,
	}
	joinGroupResp, err := k.GroupJoin(&addr, &joinGroupReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, joinGroupResp.ErrorCode)

	// offset fetch
	offsetFetchReq := codec.OffsetFetchPartitionReq{
		PartitionId: partition,
	}
	offsetFetchPartitionResp, err := k.OffsetFetch(&addr, topic, clientId, groupId, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, offsetFetchPartitionResp.ErrorCode)

	// offset fetch
	listOffset := codec.ListOffsetsPartition{
		Time:        constant.TimeEarliest,
		PartitionId: partition,
	}
	listPartition, err := k.OffsetListPartition(&addr, topic, clientId, &listOffset)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, codec.NONE, listPartition.ErrorCode)

	// fetch partition
	fetchPartitionReq := codec.FetchPartitionReq{
		PartitionId: partition,
		FetchOffset: offsetFetchPartitionResp.Offset,
	}
	testMinBytes := 1
	maxBytes := 15
	fetchPartitionResp := k.FetchPartition(&addr, topic, clientId, &fetchPartitionReq, maxBytes, testMinBytes, 5000, LocalSpan{})
	assert.Equal(t, codec.NONE, fetchPartitionResp.ErrorCode)
	assert.Equal(t, 0, len(fetchPartitionResp.RecordBatch.Records))
}
