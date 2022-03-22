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
	"github.com/paashzj/kafka_go_pulsar/pkg/utils"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
	"time"
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
	groupProtocol  []*service.GroupProtocol
	protocols      = append(groupProtocol, &protocol)
	partition      = 0
	testClientId   = "consumer-test-client-id"
	testContent    = "test-content"
	addr           = net.IPNet{IP: net.ParseIP("::1")}
	maxFetchWaitMs = 2000
	maxFetchRecord = 1
	config         = &kafsar.Config{
		KafsarConfig: kafsar.KafsarConfig{
			MaxConsumersPerGroup:     1,
			GroupMinSessionTimeoutMs: 0,
			GroupMaxSessionTimeoutMs: 30000,
			MinFetchWaitMs:           10,
			FetchIdleWaitMs:          10,
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
	k.FetchPartition(&addr, topic, &fetchPartitionReq, 200, 10, time.Now())

	url := "http://localhost:8080/admin/v2/persistent/public/default/" + pulsarTopic + fmt.Sprintf(constant.PartitionSuffixFormat, partition) + "/subscriptions"
	request, err := HttpGetRequest(url)
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, string(request), "reader")
}

func TestFetchAndCommitOffset(t *testing.T) {
	topic := uuid.New().String()
	groupId := uuid.New().String()
	pulsarTopic := utils.PartitionedTopic(defaultTopicType+topicPrefix+topic, partition)
	setupPulsar()
	k, err := kafsar.NewKafsar(kafsarServer, config)
	if err != nil {
		t.Fatal(err)
	}
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
	fetchPartitionResp := k.FetchPartition(&addr, topic, &fetchPartitionReq, 2000, 10, time.Now())
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
	pulsarTopic := utils.PartitionedTopic(defaultTopicType+topicPrefix+topic, partition)
	setupPulsar()
	k, err := kafsar.NewKafsar(kafsarServer, config)
	if err != nil {
		t.Fatal(err)
	}
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
	fetchPartitionResp := k.FetchPartition(&addr, topic, &fetchPartitionReq, 2000, 10, time.Now())
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
		ClientId:    testClientId,
		PartitionId: partition,
	}
	offsetFetchPartitionResp, err = k.OffsetFetch(&addr, topic, &offsetFetchReq)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, int16(service.NONE), offsetFetchPartitionResp.ErrorCode)

	fetchPartitionResp = k.FetchPartition(&addr, topic, &fetchPartitionReq, 2000, 10, time.Now())
	assert.Equal(t, service.NONE, fetchPartitionResp.ErrorCode)
	assert.Equal(t, maxFetchRecord, len(fetchPartitionResp.RecordBatch.Records))
	offset = int64(fetchPartitionResp.RecordBatch.Records[0].RelativeOffset) + fetchPartitionResp.RecordBatch.Offset
	assert.Equal(t, string(message.Payload), string(fetchPartitionResp.RecordBatch.Records[0].Value))

	fetchPartitionResp = k.FetchPartition(&addr, topic, &fetchPartitionReq, 2000, 10, time.Now())
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
	assert.Equal(t, acquireOffset.Offset, kafsar.ConvertMsgId(messageId))
}

func TestEarliestMsg(t *testing.T) {
	topic := uuid.New().String()
	groupId := uuid.New().String()
	pulsarTopic := utils.PartitionedTopic(defaultTopicType+topicPrefix+topic, partition)
	setupPulsar()
	k, err := kafsar.NewKafsar(kafsarServer, config)
	if err != nil {
		t.Fatal(err)
	}
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
		ClientId:    testClientId,
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
		ClientId:    testClientId,
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
		ClientId:    testClientId,
	}
	fetchPartitionResp := k.FetchPartition(&addr, topic, &fetchPartitionReq, 2000, 10, time.Now())
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
	assert.Equal(t, acquireOffset.Offset, kafsar.ConvertMsgId(earliestMessageId))
}

func TestLatestMsg(t *testing.T) {
	topic := uuid.New().String()
	groupId := uuid.New().String()
	pulsarTopic := utils.PartitionedTopic(defaultTopicType+topicPrefix+topic, partition)
	setupPulsar()
	k, err := kafsar.NewKafsar(kafsarServer, config)
	if err != nil {
		t.Fatal(err)
	}
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
		ClientId:    testClientId,
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
		ClientId:    testClientId,
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
		ClientId:    testClientId,
	}
	fetchPartitionResp := k.FetchPartition(&addr, topic, &fetchPartitionReq, 2000, 10, time.Now())
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
	assert.Equal(t, acquireOffset.Offset, kafsar.ConvertMsgId(latestMessageId))
}

func TestLatestTypeWithNoMsg(t *testing.T) {
	topic := uuid.New().String()
	groupId := uuid.New().String()
	setupPulsar()
	k, err := kafsar.NewKafsar(kafsarServer, config)
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
		ClientId:    testClientId,
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
		ClientId:    testClientId,
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
	setupPulsar()
	k, err := kafsar.NewKafsar(kafsarServer, config)
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
		ClientId:    testClientId,
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
		ClientId:    testClientId,
		PartitionId: partition,
	}
	listPartition, err := k.OffsetListPartition(&addr, topic, &listOffset)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, service.NONE, listPartition.ErrorCode)
}
