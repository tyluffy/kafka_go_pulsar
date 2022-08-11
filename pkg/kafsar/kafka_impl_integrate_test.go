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
	"github.com/paashzj/kafka_go/pkg/service"
	"github.com/paashzj/kafka_go_pulsar/pkg/constant"
	"github.com/paashzj/kafka_go_pulsar/pkg/test"
	"github.com/paashzj/kafka_go_pulsar/pkg/utils"
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
	testClientId   = "consumer-test-client-id"
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
		MemberId:       "",
	}
	joinGroupResp, err := k.GroupJoin(&addr, &joinGroupReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, joinGroupResp.ErrorCode)

	// offset fetch
	offsetFetchReq := service.OffsetFetchPartitionReq{
		GroupId:     groupId,
		ClientId:    testClientId,
		PartitionId: partition,
	}
	offset, err := k.OffsetFetch(&addr, topic, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	fetchPartitionReq := service.FetchPartitionReq{
		PartitionId: partition,
		FetchOffset: offset.Offset,
		ClientId:    testClientId,
	}
	k.FetchPartition(&addr, topic, &fetchPartitionReq, maxBytes, minBytes, 200, TracerSpan{})

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
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, joinGroupResp.ErrorCode)

	// offset fetch
	offsetFetchReq := service.OffsetFetchPartitionReq{
		GroupId:     groupId,
		ClientId:    clientId,
		PartitionId: partition,
	}
	offsetFetchPartitionResp, err := k.OffsetFetch(&addr, topic, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, int16(service.NONE), offsetFetchPartitionResp.ErrorCode)

	// fetch partition
	fetchPartitionReq := service.FetchPartitionReq{
		PartitionId: partition,
		FetchOffset: offsetFetchPartitionResp.Offset,
		ClientId:    clientId,
	}
	fetchPartitionResp := k.FetchPartition(&addr, topic, &fetchPartitionReq, maxBytes, minBytes, 2000, TracerSpan{})
	assert.Equal(t, service.NONE, fetchPartitionResp.ErrorCode)
	assert.Equal(t, maxFetchRecord, len(fetchPartitionResp.RecordBatch.Records))
	offset := int64(fetchPartitionResp.RecordBatch.Records[0].RelativeOffset) + fetchPartitionResp.RecordBatch.Offset
	assert.Equal(t, string(message.Payload), string(fetchPartitionResp.RecordBatch.Records[0].Value))
	// offset commit
	offsetCommitPartitionReq := service.OffsetCommitPartitionReq{
		ClientId:           clientId,
		PartitionId:        partition,
		OffsetCommitOffset: offset,
	}
	commitPartitionResp, err := k.OffsetCommitPartition(&addr, topic, &offsetCommitPartitionReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, commitPartitionResp.ErrorCode)
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
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, joinGroupResp.ErrorCode)

	// offset fetch
	offsetFetchReq := service.OffsetFetchPartitionReq{
		GroupId:     groupId,
		ClientId:    clientId,
		PartitionId: partition,
	}
	offsetFetchPartitionResp, err := k.OffsetFetch(&addr, topic, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, int16(service.NONE), offsetFetchPartitionResp.ErrorCode)

	// fetch partition
	fetchPartitionReq := service.FetchPartitionReq{
		PartitionId: partition,
		FetchOffset: offsetFetchPartitionResp.Offset,
		ClientId:    clientId,
	}
	fetchPartitionResp := k.FetchPartition(&addr, topic, &fetchPartitionReq, maxBytes, minBytes, 2000, TracerSpan{})
	assert.Equal(t, service.NONE, fetchPartitionResp.ErrorCode)
	assert.Equal(t, maxFetchRecord, len(fetchPartitionResp.RecordBatch.Records))
	offset := int64(fetchPartitionResp.RecordBatch.Records[0].RelativeOffset) + fetchPartitionResp.RecordBatch.Offset
	logrus.Infof("offset is : %d", offset)
	assert.Equal(t, string(message.Payload), string(fetchPartitionResp.RecordBatch.Records[0].Value))
	// offset commit
	offsetCommitPartitionReq := service.OffsetCommitPartitionReq{
		ClientId:           clientId,
		PartitionId:        partition,
		OffsetCommitOffset: offset,
	}
	commitPartitionResp, err := k.OffsetCommitPartition(&addr, topic, &offsetCommitPartitionReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, commitPartitionResp.ErrorCode)
	time.Sleep(5 * time.Second)
	// leave group
	member := []*service.LeaveGroupMember{{
		MemberId: joinGroupResp.MemberId,
	}}
	req := service.LeaveGroupReq{
		ClientId: clientId,
		GroupId:  groupId,
		Members:  member,
	}
	leave, err := k.GroupLeave(&addr, &req)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, leave.ErrorCode, service.NONE)

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
	assert.Equal(t, service.NONE, joinGroupResp.ErrorCode)

	// offset fetch
	offsetFetchReq = service.OffsetFetchPartitionReq{
		GroupId:     groupId,
		ClientId:    clientId,
		PartitionId: partition,
	}
	offsetFetchPartitionResp, err = k.OffsetFetch(&addr, topic, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, int16(service.NONE), offsetFetchPartitionResp.ErrorCode)

	fetchPartitionResp = k.FetchPartition(&addr, topic, &fetchPartitionReq, maxBytes, minBytes, 2000, TracerSpan{})
	assert.Equal(t, service.NONE, fetchPartitionResp.ErrorCode)
	assert.Equal(t, maxFetchRecord, len(fetchPartitionResp.RecordBatch.Records))
	offset = int64(fetchPartitionResp.RecordBatch.Records[0].RelativeOffset) + fetchPartitionResp.RecordBatch.Offset
	assert.Equal(t, string(message.Payload), string(fetchPartitionResp.RecordBatch.Records[0].Value))

	fetchPartitionResp = k.FetchPartition(&addr, topic, &fetchPartitionReq, maxBytes, minBytes, 2000, TracerSpan{})
	assert.Equal(t, service.NONE, fetchPartitionResp.ErrorCode)
	assert.Equal(t, 0, len(fetchPartitionResp.RecordBatch.Records))

	// offset commit
	offsetCommitPartitionReq = service.OffsetCommitPartitionReq{
		ClientId:           clientId,
		PartitionId:        partition,
		OffsetCommitOffset: offset,
	}
	commitPartitionResp, err = k.OffsetCommitPartition(&addr, topic, &offsetCommitPartitionReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, commitPartitionResp.ErrorCode)
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
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, joinGroupResp.ErrorCode)

	// offset fetch
	offsetFetchReq := service.OffsetFetchPartitionReq{
		GroupId:     groupId,
		ClientId:    clientId,
		PartitionId: partition,
	}
	offsetFetchPartitionResp, err := k.OffsetFetch(&addr, topic, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, int16(service.NONE), offsetFetchPartitionResp.ErrorCode)

	// offset fetch
	listOffset := service.ListOffsetsPartitionReq{
		Time:        constant.TimeEarliest,
		ClientId:    clientId,
		PartitionId: partition,
	}
	listPartition, err := k.OffsetListPartition(&addr, topic, &listOffset)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, listPartition.ErrorCode)

	// fetch partition
	fetchPartitionReq := service.FetchPartitionReq{
		PartitionId: partition,
		FetchOffset: offsetFetchPartitionResp.Offset,
		ClientId:    clientId,
	}
	fetchPartitionResp := k.FetchPartition(&addr, topic, &fetchPartitionReq, maxBytes, minBytes, 2000, TracerSpan{})
	assert.Equal(t, service.NONE, fetchPartitionResp.ErrorCode)
	assert.Equal(t, maxFetchRecord, len(fetchPartitionResp.RecordBatch.Records))
	offset := int64(fetchPartitionResp.RecordBatch.Records[0].RelativeOffset) + fetchPartitionResp.RecordBatch.Offset
	assert.Equal(t, string(message.Payload), string(fetchPartitionResp.RecordBatch.Records[0].Value))
	// offset commit
	offsetCommitPartitionReq := service.OffsetCommitPartitionReq{
		ClientId:           clientId,
		PartitionId:        partition,
		OffsetCommitOffset: offset,
	}
	commitPartitionResp, err := k.OffsetCommitPartition(&addr, topic, &offsetCommitPartitionReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, commitPartitionResp.ErrorCode)
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
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, joinGroupResp.ErrorCode)

	// offset fetch
	offsetFetchReq := service.OffsetFetchPartitionReq{
		GroupId:     groupId,
		ClientId:    clientId,
		PartitionId: partition,
	}
	offsetFetchPartitionResp, err := k.OffsetFetch(&addr, topic, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, int16(service.NONE), offsetFetchPartitionResp.ErrorCode)

	// offset fetch
	listOffset := service.ListOffsetsPartitionReq{
		Time:        constant.TimeLasted,
		ClientId:    clientId,
		PartitionId: partition,
	}
	listPartition, err := k.OffsetListPartition(&addr, topic, &listOffset)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, listPartition.ErrorCode)

	// fetch partition
	fetchPartitionReq := service.FetchPartitionReq{
		PartitionId: partition,
		FetchOffset: offsetFetchPartitionResp.Offset,
		ClientId:    clientId,
	}
	fetchPartitionResp := k.FetchPartition(&addr, topic, &fetchPartitionReq, maxBytes, minBytes, 2000, TracerSpan{})
	assert.Equal(t, service.NONE, fetchPartitionResp.ErrorCode)
	assert.Equal(t, maxFetchRecord, len(fetchPartitionResp.RecordBatch.Records))
	offset := int64(fetchPartitionResp.RecordBatch.Records[0].RelativeOffset) + fetchPartitionResp.RecordBatch.Offset
	assert.Equal(t, string(message.Payload), string(fetchPartitionResp.RecordBatch.Records[0].Value))
	// offset commit
	offsetCommitPartitionReq := service.OffsetCommitPartitionReq{
		ClientId:           clientId,
		PartitionId:        partition,
		OffsetCommitOffset: offset,
	}
	commitPartitionResp, err := k.OffsetCommitPartition(&addr, topic, &offsetCommitPartitionReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, commitPartitionResp.ErrorCode)
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
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, joinGroupResp.ErrorCode)

	// offset fetch
	offsetFetchReq := service.OffsetFetchPartitionReq{
		GroupId:     groupId,
		ClientId:    clientId,
		PartitionId: partition,
	}
	offsetFetchPartitionResp, err := k.OffsetFetch(&addr, topic, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, int16(service.NONE), offsetFetchPartitionResp.ErrorCode)

	// offset fetch
	listOffset := service.ListOffsetsPartitionReq{
		Time:        constant.TimeLasted,
		ClientId:    clientId,
		PartitionId: partition,
	}
	listPartition, err := k.OffsetListPartition(&addr, topic, &listOffset)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, listPartition.ErrorCode)
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
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, joinGroupResp.ErrorCode)

	// offset fetch
	offsetFetchReq := service.OffsetFetchPartitionReq{
		GroupId:     groupId,
		ClientId:    clientId,
		PartitionId: partition,
	}
	offsetFetchPartitionResp, err := k.OffsetFetch(&addr, topic, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, int16(service.NONE), offsetFetchPartitionResp.ErrorCode)

	// offset fetch
	listOffset := service.ListOffsetsPartitionReq{
		Time:        constant.TimeEarliest,
		ClientId:    clientId,
		PartitionId: partition,
	}
	listPartition, err := k.OffsetListPartition(&addr, topic, &listOffset)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, listPartition.ErrorCode)
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
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, joinGroupResp.ErrorCode)

	// offset fetch
	offsetFetchReq := service.OffsetFetchPartitionReq{
		GroupId:     groupId,
		ClientId:    clientId,
		PartitionId: partition,
	}
	offsetFetchPartitionResp, err := k.OffsetFetch(&addr, topic, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, int16(service.NONE), offsetFetchPartitionResp.ErrorCode)

	// offset fetch
	listOffset := service.ListOffsetsPartitionReq{
		Time:        constant.TimeLasted,
		ClientId:    clientId,
		PartitionId: partition,
	}
	listPartition, err := k.OffsetListPartition(&addr, topic, &listOffset)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, listPartition.ErrorCode)

	// fetch partition
	fetchPartitionReq := service.FetchPartitionReq{
		PartitionId: partition,
		FetchOffset: offsetFetchPartitionResp.Offset,
		ClientId:    clientId,
	}
	testMinBytes := 1
	fetchPartitionResp := k.FetchPartition(&addr, topic, &fetchPartitionReq, maxBytes, testMinBytes, 2000, TracerSpan{})
	assert.Equal(t, service.NONE, fetchPartitionResp.ErrorCode)
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
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, joinGroupResp.ErrorCode)

	// offset fetch
	offsetFetchReq := service.OffsetFetchPartitionReq{
		GroupId:     groupId,
		ClientId:    clientId,
		PartitionId: partition,
	}
	offsetFetchPartitionResp, err := k.OffsetFetch(&addr, topic, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, int16(service.NONE), offsetFetchPartitionResp.ErrorCode)

	// offset fetch
	listOffset := service.ListOffsetsPartitionReq{
		Time:        constant.TimeEarliest,
		ClientId:    clientId,
		PartitionId: partition,
	}
	listPartition, err := k.OffsetListPartition(&addr, topic, &listOffset)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, listPartition.ErrorCode)

	// fetch partition
	fetchPartitionReq := service.FetchPartitionReq{
		PartitionId: partition,
		FetchOffset: offsetFetchPartitionResp.Offset,
		ClientId:    clientId,
	}
	testMinBytes := 1
	maxBytes := 15
	fetchPartitionResp := k.FetchPartition(&addr, topic, &fetchPartitionReq, maxBytes, testMinBytes, 5000, TracerSpan{})
	assert.Equal(t, service.NONE, fetchPartitionResp.ErrorCode)
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
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, joinGroupResp.ErrorCode)

	// join group
	joinGroupReq = service.JoinGroupReq{
		ClientId:       clientId,
		GroupId:        groupId,
		SessionTimeout: sessionTimeoutMs,
		ProtocolType:   protocolType,
		GroupProtocols: protocols,
	}
	joinGroupResp2, err := k.GroupJoin(&addr, &joinGroupReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, joinGroupResp2.ErrorCode)

	// offset fetch
	offsetFetchReq := service.OffsetFetchPartitionReq{
		GroupId:     groupId,
		ClientId:    testClientId,
		PartitionId: partition,
	}
	offsetFetchPartitionResp, err := k.OffsetFetch(&addr, topic, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, int16(service.NONE), offsetFetchPartitionResp.ErrorCode)

	var members []*service.LeaveGroupMember
	leaveGroupMembers := append(members, &service.LeaveGroupMember{
		MemberId: joinGroupResp.MemberId,
	})

	leaveGroupResp, err := k.GroupLeave(&addr, &service.LeaveGroupReq{ClientId: clientId, GroupId: groupId, Members: leaveGroupMembers})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, leaveGroupResp.ErrorCode)

	var members2 []*service.LeaveGroupMember
	leaveGroupMembers2 := append(members2, &service.LeaveGroupMember{
		MemberId: joinGroupResp.MemberId,
	})

	leaveGroupResp2, err := k.GroupLeave(&addr, &service.LeaveGroupReq{ClientId: clientId, GroupId: groupId, Members: leaveGroupMembers2})
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, leaveGroupResp2.ErrorCode)
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
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, joinGroupResp.ErrorCode)

	// offset fetch
	offsetFetchReq := service.OffsetFetchPartitionReq{
		GroupId:     groupId,
		ClientId:    testClientId,
		PartitionId: partition,
	}
	offsetFetchPartitionResp, err := k.OffsetFetch(&addr, topic, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, int16(service.NONE), offsetFetchPartitionResp.ErrorCode)

	// fetch partition
	fetchPartitionReq := service.FetchPartitionReq{
		PartitionId: partition,
		FetchOffset: offsetFetchPartitionResp.Offset,
		ClientId:    testClientId,
	}
	fetchPartitionResp := k.FetchPartition(&addr, topic, &fetchPartitionReq, maxBytes, minBytes, 2000, TracerSpan{})
	assert.Equal(t, service.NONE, fetchPartitionResp.ErrorCode)
	assert.Equal(t, maxFetchRecord, len(fetchPartitionResp.RecordBatch.Records))
	offset := int64(fetchPartitionResp.RecordBatch.Records[0].RelativeOffset) + fetchPartitionResp.RecordBatch.Offset
	assert.Equal(t, string(message.Payload), string(fetchPartitionResp.RecordBatch.Records[0].Value))
	// offset commit
	offsetCommitPartitionReq := service.OffsetCommitPartitionReq{
		ClientId:           testClientId,
		PartitionId:        partition,
		OffsetCommitOffset: offset,
	}
	commitPartitionResp, err := k.OffsetCommitPartition(&addr, topic, &offsetCommitPartitionReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, commitPartitionResp.ErrorCode)
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
	assert.Equal(t, service.NONE, errorCode)
	assert.True(t, true, auth)

	joinGroupResp, err = k.GroupJoin(&addr, &joinGroupReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, joinGroupResp.ErrorCode)

	offsetFetchPartitionResp, err = k.OffsetFetch(&addr, topic, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, int16(service.NONE), offsetFetchPartitionResp.ErrorCode)

	fetchPartitionResp = k.FetchPartition(&addr, topic, &fetchPartitionReq, maxBytes, minBytes, 2000, TracerSpan{})
	assert.Equal(t, service.NONE, fetchPartitionResp.ErrorCode)
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
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, joinGroupResp.ErrorCode)

	// offset fetch
	offsetFetchReq := service.OffsetFetchPartitionReq{
		GroupId:     groupId,
		ClientId:    clientId,
		PartitionId: partition,
	}
	offsetFetchPartitionResp, err := k.OffsetFetch(&addr, topic, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, int16(service.NONE), offsetFetchPartitionResp.ErrorCode)

	// offset fetch
	listOffset := service.ListOffsetsPartitionReq{
		Time:        constant.TimeEarliest,
		ClientId:    clientId,
		PartitionId: partition,
	}
	listPartition, err := k.OffsetListPartition(&addr, topic, &listOffset)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, listPartition.ErrorCode)

	// fetch partition
	fetchPartitionReq := service.FetchPartitionReq{
		PartitionId: partition,
		FetchOffset: offsetFetchPartitionResp.Offset,
		ClientId:    clientId,
	}
	testMinBytes := 1
	maxBytes := 15
	fetchPartitionResp := k.FetchPartition(&addr, topic, &fetchPartitionReq, maxBytes, testMinBytes, 5000, TracerSpan{})
	assert.Equal(t, service.NONE, fetchPartitionResp.ErrorCode)
	assert.Equal(t, 0, len(fetchPartitionResp.RecordBatch.Records))
}
