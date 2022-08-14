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
	"container/list"
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/paashzj/kafka_go_pulsar/pkg/constant"
	"github.com/paashzj/kafka_go_pulsar/pkg/service"
	"github.com/paashzj/kafka_go_pulsar/pkg/utils"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"net"
	"strings"
	"sync"
	"time"
)

type KafkaImpl struct {
	server             Server
	pulsarConfig       PulsarConfig
	pulsarCommonClient pulsar.Client
	pulsarClientManage map[string]pulsar.Client
	groupCoordinator   GroupCoordinator
	kafsarConfig       KafsarConfig
	readerManager      map[string]*ReaderMetadata
	mutex              sync.RWMutex
	userInfoManager    map[string]*userInfo
	offsetManager      OffsetManager
	memberManager      map[string]*MemberInfo
	topicGroupManager  map[string]string
	tracer             *NoErrorTracer // skywalking tracer
}

type userInfo struct {
	username string
	clientId string
}

type MessageIdPair struct {
	MessageId pulsar.MessageID
	Offset    int64
}

type MemberInfo struct {
	memberId        string
	groupId         string
	groupInstanceId *string
	clientId        string
}

func newKafsar(impl Server, config *Config) (*KafkaImpl, error) {
	kafka := KafkaImpl{server: impl, pulsarConfig: config.PulsarConfig, kafsarConfig: config.KafsarConfig}
	pulsarUrl := fmt.Sprintf("pulsar://%s:%d", kafka.pulsarConfig.Host, kafka.pulsarConfig.TcpPort)
	var err error
	pulsarClient, err := pulsar.NewClient(pulsar.ClientOptions{URL: pulsarUrl})
	if err != nil {
		return nil, err
	}
	pulsarAddr := kafka.getPulsarHttpUrl()
	kafka.offsetManager, err = NewOffsetManager(pulsarClient, config.KafsarConfig, pulsarAddr)
	if err != nil {
		pulsarClient.Close()
		return nil, err
	}

	offsetChannel := kafka.offsetManager.Start()
	for {
		if <-offsetChannel {
			break
		}
	}
	if kafka.kafsarConfig.GroupCoordinatorType == Cluster {
		kafka.groupCoordinator = NewGroupCoordinatorCluster()
	} else if kafka.kafsarConfig.GroupCoordinatorType == Standalone {
		kafka.groupCoordinator = NewGroupCoordinatorStandalone(kafka.pulsarConfig, kafka.kafsarConfig, pulsarClient)
	} else {
		return nil, errors.Errorf("unexpect GroupCoordinatorType: %v", kafka.kafsarConfig.GroupCoordinatorType)
	}
	kafka.pulsarCommonClient = pulsarClient
	kafka.readerManager = make(map[string]*ReaderMetadata)
	kafka.userInfoManager = make(map[string]*userInfo)
	kafka.memberManager = make(map[string]*MemberInfo)
	kafka.pulsarClientManage = make(map[string]pulsar.Client)
	kafka.topicGroupManager = make(map[string]string)
	kafka.tracer = NewTracer(config.TraceConfig)
	return &kafka, nil
}

func (k *KafkaImpl) Produce(addr net.Addr, kafkaTopic string, partition int, req *service.ProducePartitionReq) (*service.ProducePartitionResp, error) {
	panic("implement me")
}

func (k *KafkaImpl) Fetch(addr net.Addr, req *service.FetchReq) ([]*service.FetchTopicResp, error) {
	traceSpan := k.tracer.CreateLocalLogSpan(context.Background())
	k.tracer.SpanLog(traceSpan, "fetch action starting")

	var maxWaitTime int
	if req.MaxWaitTime < k.kafsarConfig.MaxFetchWaitMs {
		maxWaitTime = req.MaxWaitTime
	} else {
		maxWaitTime = k.kafsarConfig.MaxFetchWaitMs
	}
	reqList := req.FetchTopicReqList
	result := make([]*service.FetchTopicResp, len(reqList))
	for i, topicReq := range reqList {
		topicSpan := k.traceFetchLog(traceSpan, fmt.Sprintf(
			"topic: %s fetching", topicReq.Topic), true, false)
		f := &service.FetchTopicResp{}
		f.Topic = topicReq.Topic
		f.FetchPartitionRespList = make([]*service.FetchPartitionResp, len(topicReq.FetchPartitionReqList))
		for j, partitionReq := range topicReq.FetchPartitionReqList {
			f.FetchPartitionRespList[j] = k.FetchPartition(addr, topicReq.Topic, partitionReq,
				req.MaxBytes, req.MinBytes, maxWaitTime/len(topicReq.FetchPartitionReqList), topicSpan)
		}
		result[i] = f
		k.traceFetchLog(topicSpan, fmt.Sprintf("topic: %s fetched", topicReq.Topic), false, true)
	}
	k.tracer.SpanLogWithClose(traceSpan, "fetch action done")
	return result, nil
}

// FetchPartition visible for testing
func (k *KafkaImpl) FetchPartition(addr net.Addr, kafkaTopic string, req *service.FetchPartitionReq, maxBytes int, minBytes int, maxWaitMs int, span TracerSpan) *service.FetchPartitionResp {
	// open tracer, log
	fetchSpan := k.traceFetchLog(span, fmt.Sprintf("fetching partition %s:%d",
		kafkaTopic, req.PartitionId), true, false)
	defer k.traceFetchLog(fetchSpan, fmt.Sprintf("fetched partition %s:%d",
		kafkaTopic, req.PartitionId), false, true)
	start := time.Now()
	k.mutex.RLock()
	user, exist := k.userInfoManager[addr.String()]
	k.mutex.RUnlock()
	var records []*service.Record
	recordBatch := service.RecordBatch{Records: records}
	if !exist {
		logrus.Errorf("fetch partition failed when get userinfo by addr %s, kafka topic: %s", addr.String(), kafkaTopic)
		return &service.FetchPartitionResp{
			PartitionId: req.PartitionId,
			ErrorCode:   service.UNKNOWN_SERVER_ERROR,
			RecordBatch: &recordBatch,
		}
	}
	logrus.Infof("%s fetch topic: %s partition %d", addr.String(), kafkaTopic, req.PartitionId)
	partitionedTopic, err := k.partitionedTopic(user, kafkaTopic, req.PartitionId)
	if err != nil {
		logrus.Errorf("fetch partition failed when get pulsar topic %s, kafka topic: %s", addr.String(), kafkaTopic)
		return &service.FetchPartitionResp{
			PartitionId: req.PartitionId,
			ErrorCode:   service.UNKNOWN_SERVER_ERROR,
			RecordBatch: &recordBatch,
		}
	}
	k.mutex.RLock()
	readerMetadata, exist := k.readerManager[partitionedTopic+req.ClientId]
	if !exist {
		groupId, exist := k.topicGroupManager[partitionedTopic]
		k.mutex.RUnlock()
		if exist {
			group, err := k.groupCoordinator.GetGroup(user.username, groupId)
			if err == nil && group.groupStatus != Stable {
				logrus.Infof("group is preparing rebalance. grouId: %s, topic: %s", groupId, partitionedTopic)
				return &service.FetchPartitionResp{
					ErrorCode: service.REBALANCE_IN_PROGRESS,
				}
			}
		}
		logrus.Errorf("can not find reader for topic: %s when fetch partition", partitionedTopic)
		return &service.FetchPartitionResp{
			PartitionId: req.PartitionId,
			ErrorCode:   service.UNKNOWN_SERVER_ERROR,
			RecordBatch: &recordBatch,
		}
	}
	k.mutex.RUnlock()
	byteLength := 0
	var baseOffset int64
	fistMessage := true
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(maxWaitMs)*time.Millisecond)
	defer cancel()
OUT:
	for {
		if time.Since(start).Milliseconds() >= int64(maxWaitMs) || len(recordBatch.Records) >= k.kafsarConfig.MaxFetchRecord {
			break OUT
		}
		flowControl := k.server.HasFlowQuota(user.username, partitionedTopic)
		if !flowControl {
			break
		}
		message, err := readerMetadata.reader.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break OUT
			}
			logrus.Errorf("read msg failed. err: %s", err)
			continue
		}
		byteLength = byteLength + utils.CalculateMsgLength(message)
		logrus.Infof("receive msg: %s from %s", message.ID(), message.Topic())
		offset := convOffset(message, k.kafsarConfig.ContinuousOffset)
		if fistMessage {
			fistMessage = false
			baseOffset = offset
		}
		relativeOffset := offset - baseOffset
		record := service.Record{
			Value:          message.Payload(),
			RelativeOffset: int(relativeOffset),
		}
		recordBatch.Records = append(recordBatch.Records, &record)
		readerMetadata.messageIds.PushBack(MessageIdPair{
			MessageId: message.ID(),
			Offset:    offset,
		})
		if byteLength > minBytes && time.Since(start).Milliseconds() >= int64(k.kafsarConfig.MinFetchWaitMs) {
			break
		}
		if byteLength > maxBytes {
			break
		}
	}
	recordBatch.Offset = baseOffset
	return &service.FetchPartitionResp{
		ErrorCode:        service.NONE,
		PartitionId:      req.PartitionId,
		LastStableOffset: 0,
		LogStartOffset:   0,
		RecordBatch:      &recordBatch,
	}
}

func (k *KafkaImpl) GroupJoin(addr net.Addr, req *service.JoinGroupReq) (*service.JoinGroupResp, error) {
	k.mutex.RLock()
	user, exist := k.userInfoManager[addr.String()]
	k.mutex.RUnlock()
	if !exist {
		logrus.Errorf("username not found in join group: %s", req.GroupId)
		return &service.JoinGroupResp{
			ErrorCode:    service.UNKNOWN_SERVER_ERROR,
			MemberId:     req.MemberId,
			GenerationId: -1,
		}, nil
	}
	logrus.Infof("%s joining to group: %s, memberId: %s", addr.String(), req.GroupId, req.MemberId)
	joinGroupResp, err := k.groupCoordinator.HandleJoinGroup(user.username, req.GroupId, req.MemberId, req.ClientId, req.ProtocolType,
		req.SessionTimeout, req.GroupProtocols)
	if err != nil {
		logrus.Errorf("unexpected exception in join group: %s, error: %s", req.GroupId, err)
		return &service.JoinGroupResp{
			ErrorCode:    service.UNKNOWN_SERVER_ERROR,
			MemberId:     req.MemberId,
			GenerationId: -1,
		}, nil
	}
	memberInfo := MemberInfo{
		memberId:        joinGroupResp.MemberId,
		groupId:         req.GroupId,
		groupInstanceId: req.GroupInstanceId,
		clientId:        req.ClientId,
	}
	k.mutex.Lock()
	k.memberManager[addr.String()] = &memberInfo
	k.mutex.Unlock()
	return joinGroupResp, nil
}

func (k *KafkaImpl) GroupLeave(addr net.Addr, req *service.LeaveGroupReq) (*service.LeaveGroupResp, error) {
	k.mutex.RLock()
	user, exist := k.userInfoManager[addr.String()]
	k.mutex.RUnlock()
	if !exist {
		logrus.Errorf("username not found in leave group: %s", req.GroupId)
		return &service.LeaveGroupResp{
			ErrorCode: service.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	logrus.Infof("%s leaving group: %s, members: %+v", addr.String(), req.GroupId, req.Members)
	leaveGroupResp, err := k.groupCoordinator.HandleLeaveGroup(user.username, req.GroupId, req.Members)
	if err != nil {
		logrus.Errorf("unexpected exception in leaving group: %s, error: %s", req.GroupId, err)
		return &service.LeaveGroupResp{
			ErrorCode: service.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	group, err := k.groupCoordinator.GetGroup(user.username, req.GroupId)
	if err != nil {
		logrus.Errorf("get group %s failed, error: %s", req.GroupId, err)
		return &service.LeaveGroupResp{
			ErrorCode: service.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	for _, topic := range group.partitionedTopic {
		k.mutex.Lock()
		readerMetadata, exist := k.readerManager[topic+req.ClientId]
		if exist {
			readerMetadata.reader.Close()
			logrus.Infof("success close reader topic: %s", group.partitionedTopic)
			delete(k.readerManager, topic+req.ClientId)
			readerMetadata = nil
		}
		client, exist := k.pulsarClientManage[topic+req.ClientId]
		if exist {
			client.Close()
			delete(k.pulsarClientManage, topic+req.ClientId)
			client = nil
		}
		delete(k.topicGroupManager, topic)
		k.mutex.Unlock()
	}
	return leaveGroupResp, nil
}

func (k *KafkaImpl) GroupSync(addr net.Addr, req *service.SyncGroupReq) (*service.SyncGroupResp, error) {
	k.mutex.RLock()
	user, exist := k.userInfoManager[addr.String()]
	k.mutex.RUnlock()
	if !exist {
		logrus.Errorf("username not found in sync group: %s", req.GroupId)
		return &service.SyncGroupResp{
			ErrorCode: service.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	logrus.Infof("%s syncing group: %s, memberId: %s", addr.String(), req.GroupId, req.MemberId)
	syncGroupResp, err := k.groupCoordinator.HandleSyncGroup(user.username, req.GroupId, req.MemberId, req.GenerationId, req.GroupAssignments)
	if err != nil {
		logrus.Errorf("unexpected exception in sync group: %s, error: %s", req.GroupId, err)
		return &service.SyncGroupResp{
			ErrorCode: service.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	return syncGroupResp, nil
}

func (k *KafkaImpl) OffsetListPartition(addr net.Addr, kafkaTopic string, req *service.ListOffsetsPartitionReq) (*service.ListOffsetsPartitionResp, error) {
	k.mutex.RLock()
	user, exist := k.userInfoManager[addr.String()]
	k.mutex.RUnlock()
	if !exist {
		logrus.Errorf("offset list failed when get username by addr %s, kafka topic: %s", addr.String(), kafkaTopic)
		return &service.ListOffsetsPartitionResp{
			ErrorCode: service.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	logrus.Infof("%s offset list topic: %s, partition: %d", addr.String(), kafkaTopic, req.PartitionId)
	partitionedTopic, err := k.partitionedTopic(user, kafkaTopic, req.PartitionId)
	if err != nil {
		logrus.Errorf("get topic failed. err: %s", err)
		return &service.ListOffsetsPartitionResp{
			ErrorCode: service.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	k.mutex.RLock()
	client, exist := k.pulsarClientManage[partitionedTopic+req.ClientId]
	if !exist {
		groupId, exist := k.topicGroupManager[partitionedTopic]
		k.mutex.RUnlock()
		if exist {
			group, err := k.groupCoordinator.GetGroup(user.username, groupId)
			if err == nil && group.groupStatus != Stable {
				logrus.Infof("group is preparing rebalance. grouId: %s, topic: %s", groupId, partitionedTopic)
				return &service.ListOffsetsPartitionResp{
					ErrorCode: service.REBALANCE_IN_PROGRESS,
				}, nil
			}
		}
		logrus.Errorf("get pulsar client failed. err: %s", err)
		return &service.ListOffsetsPartitionResp{
			ErrorCode: service.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	readerMessages, exist := k.readerManager[partitionedTopic+req.ClientId]
	k.mutex.RUnlock()
	if !exist {
		logrus.Errorf("offset list failed, topic: %s, does not exist", partitionedTopic)
		return &service.ListOffsetsPartitionResp{
			ErrorCode: service.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	offset := constant.DefaultOffset
	if req.Time == constant.TimeLasted {
		msg, err := utils.GetLatestMsgId(partitionedTopic, k.getPulsarHttpUrl())
		if err != nil {
			logrus.Errorf("get topic %s latest offset failed %s\n", kafkaTopic, err)
			return &service.ListOffsetsPartitionResp{
				ErrorCode: service.UNKNOWN_SERVER_ERROR,
			}, nil
		}
		lastedMsg, err := utils.ReadLastedMsg(partitionedTopic, k.kafsarConfig.MaxFetchWaitMs, msg, client)
		if err != nil {
			logrus.Errorf("read lasted msg failed. topic: %s, err: %s", kafkaTopic, err)
			return &service.ListOffsetsPartitionResp{
				ErrorCode: service.UNKNOWN_SERVER_ERROR,
			}, nil
		}
		if lastedMsg != nil {
			err := readerMessages.reader.Seek(lastedMsg.ID())
			if err != nil {
				logrus.Errorf("offset list failed, topic: %s, err: %s", partitionedTopic, err)
				return &service.ListOffsetsPartitionResp{
					ErrorCode: service.UNKNOWN_SERVER_ERROR,
				}, nil
			}
			offset = convOffset(lastedMsg, k.kafsarConfig.ContinuousOffset)
		}
	}
	return &service.ListOffsetsPartitionResp{
		PartitionId: req.PartitionId,
		Offset:      offset,
		Time:        constant.TimeEarliest,
	}, nil
}

func (k *KafkaImpl) OffsetCommitPartition(addr net.Addr, kafkaTopic string, req *service.OffsetCommitPartitionReq) (*service.OffsetCommitPartitionResp, error) {
	k.mutex.RLock()
	user, exist := k.userInfoManager[addr.String()]
	k.mutex.RUnlock()
	if !exist {
		logrus.Errorf("offset commit failed when get userinfo by addr %s, kafka topic: %s", addr.String(), kafkaTopic)
		return &service.OffsetCommitPartitionResp{
			PartitionId: req.PartitionId,
			ErrorCode:   service.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	logrus.Infof("%s topic: %s, partition: %d, commit offset: %d", addr.String(), kafkaTopic, req.PartitionId, req.OffsetCommitOffset)
	partitionedTopic, err := k.partitionedTopic(user, kafkaTopic, req.PartitionId)
	if err != nil {
		logrus.Errorf("offset commit failed when get pulsar topic %s, kafka topic: %s", addr.String(), kafkaTopic)
		return &service.OffsetCommitPartitionResp{
			PartitionId: req.PartitionId,
			ErrorCode:   service.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	k.mutex.RLock()
	readerMessages, exist := k.readerManager[partitionedTopic+req.ClientId]
	if !exist {
		groupId, exist := k.topicGroupManager[partitionedTopic]
		k.mutex.RUnlock()
		if exist {
			group, err := k.groupCoordinator.GetGroup(user.username, groupId)
			if err == nil && group.groupStatus != Stable {
				logrus.Warnf("group is preparing rebalance. groupId: %s, topic: %s", groupId, partitionedTopic)
				return &service.OffsetCommitPartitionResp{ErrorCode: service.REBALANCE_IN_PROGRESS}, nil
			}
		}
		logrus.Errorf("commit offset failed, topic: %s, does not exist", partitionedTopic)
		return &service.OffsetCommitPartitionResp{ErrorCode: service.UNKNOWN_TOPIC_ID}, nil
	}
	k.mutex.RUnlock()
	length := readerMessages.messageIds.Len()
	for i := 0; i < length; i++ {
		front := readerMessages.messageIds.Front()
		if front == nil {
			break
		}
		messageIdPair := front.Value.(MessageIdPair)
		// kafka commit offset maybe greater than current offset
		if messageIdPair.Offset == req.OffsetCommitOffset || ((messageIdPair.Offset < req.OffsetCommitOffset) && (i == length-1)) {
			err := k.offsetManager.CommitOffset(user.username, kafkaTopic, readerMessages.groupId, req.PartitionId, messageIdPair)
			if err != nil {
				logrus.Errorf("commit offset failed. topic: %s, err: %s", kafkaTopic, err)
				return &service.OffsetCommitPartitionResp{
					PartitionId: req.PartitionId,
					ErrorCode:   service.UNKNOWN_SERVER_ERROR,
				}, nil
			}
			logrus.Infof("ack pulsar %s for %s", partitionedTopic, messageIdPair.MessageId)
			readerMessages.messageIds.Remove(front)
			break
		}
		if messageIdPair.Offset > req.OffsetCommitOffset {
			break
		}
		readerMessages.messageIds.Remove(front)
	}
	return &service.OffsetCommitPartitionResp{
		PartitionId: req.PartitionId,
		ErrorCode:   service.NONE,
	}, nil
}

func (k *KafkaImpl) OffsetFetch(addr net.Addr, topic string, req *service.OffsetFetchPartitionReq) (*service.OffsetFetchPartitionResp, error) {
	k.mutex.RLock()
	user, exist := k.userInfoManager[addr.String()]
	k.mutex.RUnlock()
	if !exist {
		logrus.Errorf("offset fetch failed when get userinfo by addr %s, kafka topic: %s", addr.String(), topic)
		return &service.OffsetFetchPartitionResp{
			ErrorCode: int16(service.UNKNOWN_SERVER_ERROR),
		}, nil
	}
	logrus.Infof("%s fetch topic: %s offset, partition: %d", addr.String(), topic, req.PartitionId)
	partitionedTopic, err := k.partitionedTopic(user, topic, req.PartitionId)
	if err != nil {
		logrus.Errorf("offset fetch failed when get pulsar topic %s, kafka topic: %s", addr.String(), topic)
		return &service.OffsetFetchPartitionResp{
			ErrorCode: int16(service.UNKNOWN_SERVER_ERROR),
		}, nil
	}
	subscriptionName, err := k.server.SubscriptionName(req.GroupId)
	if err != nil {
		logrus.Errorf("sync group %s failed when offset fetch, error: %s", req.GroupId, err)
	}
	messagePair, flag := k.offsetManager.AcquireOffset(user.username, topic, req.GroupId, req.PartitionId)
	messageId := pulsar.EarliestMessageID()
	kafkaOffset := constant.UnknownOffset
	if flag {
		kafkaOffset = messagePair.Offset
		messageId = messagePair.MessageId
	}
	k.mutex.RLock()
	_, exist = k.readerManager[partitionedTopic+req.ClientId]
	k.mutex.RUnlock()
	if !exist {
		k.mutex.Lock()
		metadata := ReaderMetadata{groupId: req.GroupId, messageIds: list.New()}
		channel, reader, err := k.createReader(partitionedTopic, subscriptionName, messageId, req.ClientId)
		if err != nil {
			k.mutex.Unlock()
			logrus.Errorf("%s, create channel failed, error: %s", topic, err)
			return &service.OffsetFetchPartitionResp{
				ErrorCode: int16(service.UNKNOWN_SERVER_ERROR),
			}, nil
		}
		metadata.reader = reader
		metadata.channel = channel
		k.readerManager[partitionedTopic+req.ClientId] = &metadata
		k.mutex.Unlock()
	}
	group, err := k.groupCoordinator.GetGroup(user.username, req.GroupId)
	if err != nil {
		logrus.Errorf("get group %s failed, error: %s", req.GroupId, err)
		return &service.OffsetFetchPartitionResp{
			ErrorCode: int16(service.UNKNOWN_SERVER_ERROR),
		}, nil
	}
	if !k.checkPartitionTopicExist(group.partitionedTopic, partitionedTopic) {
		group.partitionedTopic = append(group.partitionedTopic, partitionedTopic)
	}
	k.mutex.Lock()
	k.topicGroupManager[partitionedTopic] = group.groupId
	k.mutex.Unlock()

	return &service.OffsetFetchPartitionResp{
		PartitionId: req.PartitionId,
		Offset:      kafkaOffset,
		LeaderEpoch: -1,
		Metadata:    nil,
		ErrorCode:   int16(service.NONE),
	}, nil
}

func (k *KafkaImpl) partitionedTopic(user *userInfo, kafkaTopic string, partitionId int) (string, error) {
	pulsarTopic, err := k.server.PulsarTopic(user.username, kafkaTopic)
	if err != nil {
		return "", err
	}
	return pulsarTopic + fmt.Sprintf(constant.PartitionSuffixFormat, partitionId), nil
}

func (k *KafkaImpl) OffsetLeaderEpoch(addr net.Addr, topic string, req *service.OffsetLeaderEpochPartitionReq) (*service.OffsetLeaderEpochPartitionResp, error) {
	k.mutex.RLock()
	user, exist := k.userInfoManager[addr.String()]
	k.mutex.RUnlock()
	if !exist {
		logrus.Errorf("offset fetch failed when get userinfo by addr %s, kafka topic: %s", addr.String(), topic)
		return &service.OffsetLeaderEpochPartitionResp{
			ErrorCode: int16(service.UNKNOWN_SERVER_ERROR),
		}, nil
	}
	logrus.Infof("%s offset leader epoch topic: %s, partition: %d", addr.String(), topic, req.PartitionId)
	partitionedTopic, err := k.partitionedTopic(user, topic, req.PartitionId)
	if err != nil {
		logrus.Errorf("get partitioned topic failed. topic: %s", topic)
		return &service.OffsetLeaderEpochPartitionResp{
			ErrorCode: int16(service.UNKNOWN_SERVER_ERROR),
		}, nil
	}
	msgByte, err := utils.GetLatestMsgId(partitionedTopic, k.getPulsarHttpUrl())
	if err != nil {
		logrus.Errorf("get last msgId failed. topic: %s", topic)
		return &service.OffsetLeaderEpochPartitionResp{
			ErrorCode: int16(service.UNKNOWN_SERVER_ERROR),
		}, nil
	}
	msg, err := utils.ReadLastedMsg(partitionedTopic, k.kafsarConfig.MaxFetchWaitMs, msgByte, k.pulsarCommonClient)
	if err != nil {
		logrus.Errorf("get last msgId failed. topic: %s", topic)
		return &service.OffsetLeaderEpochPartitionResp{
			ErrorCode: int16(service.UNKNOWN_SERVER_ERROR),
		}, nil
	}
	offset := convOffset(msg, k.kafsarConfig.ContinuousOffset)
	return &service.OffsetLeaderEpochPartitionResp{
		ErrorCode:   int16(service.NONE),
		PartitionId: req.PartitionId,
		LeaderEpoch: req.LeaderEpoch,
		Offset:      offset,
	}, nil
}

func (k *KafkaImpl) SaslAuth(addr net.Addr, req service.SaslReq) (bool, service.ErrorCode) {
	auth, err := k.server.Auth(req.Username, req.Password, req.ClientId)
	if err != nil || !auth {
		return false, service.SASL_AUTHENTICATION_FAILED
	}
	k.mutex.RLock()
	_, exist := k.userInfoManager[addr.String()]
	k.mutex.RUnlock()
	if !exist {
		k.mutex.Lock()
		k.userInfoManager[addr.String()] = &userInfo{
			username: req.Username,
			clientId: req.ClientId,
		}
		k.mutex.Unlock()
	}
	return true, service.NONE
}

func (k *KafkaImpl) SaslAuthTopic(addr net.Addr, req service.SaslReq, topic, permissionType string) (bool, service.ErrorCode) {
	auth, err := k.server.AuthTopic(req.Username, req.Password, req.ClientId, topic, permissionType)
	if err != nil || !auth {
		return false, service.SASL_AUTHENTICATION_FAILED
	}
	return true, service.NONE
}

func (k *KafkaImpl) SaslAuthConsumerGroup(addr net.Addr, req service.SaslReq, consumerGroup string) (bool, service.ErrorCode) {
	auth, err := k.server.AuthTopicGroup(req.Username, req.Password, req.ClientId, consumerGroup)
	if err != nil || !auth {
		return false, service.SASL_AUTHENTICATION_FAILED
	}
	return true, service.NONE
}

func (k *KafkaImpl) Disconnect(addr net.Addr) {
	logrus.Infof("lost connection: %s", addr)
	if addr == nil {
		return
	}
	k.mutex.RLock()
	memberInfo, exist := k.memberManager[addr.String()]
	k.mutex.RUnlock()
	if !exist {
		k.mutex.Lock()
		delete(k.userInfoManager, addr.String())
		k.mutex.Unlock()
		return
	}
	memberList := []*service.LeaveGroupMember{
		{
			MemberId:        memberInfo.memberId,
			GroupInstanceId: memberInfo.groupInstanceId,
		},
	}
	req := service.LeaveGroupReq{
		ClientId: memberInfo.clientId,
		GroupId:  memberInfo.groupId,
		Members:  memberList,
	}
	_, err := k.GroupLeave(addr, &req)
	if err != nil {
		logrus.Errorf("leave group failed. err: %s", err)
	}
	// leave group will use user information
	k.mutex.Lock()
	delete(k.userInfoManager, addr.String())
	k.mutex.Unlock()
}

func (k *KafkaImpl) Close() {
	k.offsetManager.Close()
	k.mutex.Lock()
	for key, value := range k.pulsarClientManage {
		value.Close()
		delete(k.pulsarClientManage, key)
	}
	k.mutex.Unlock()
}

func (k *KafkaImpl) GetOffsetManager() OffsetManager {
	return k.offsetManager
}

func (k *KafkaImpl) createReader(partitionedTopic string, subscriptionName string, messageId pulsar.MessageID, clientId string) (chan pulsar.ReaderMessage, pulsar.Reader, error) {
	client, exist := k.pulsarClientManage[partitionedTopic+clientId]
	if !exist {
		var err error
		pulsarUrl := fmt.Sprintf("pulsar://%s:%d", k.pulsarConfig.Host, k.pulsarConfig.TcpPort)
		client, err = pulsar.NewClient(pulsar.ClientOptions{URL: pulsarUrl})
		if err != nil {
			logrus.Errorf("create pulsar client failed.")
			return nil, nil, err
		}
		k.pulsarClientManage[partitionedTopic+clientId] = client
	}
	channel := make(chan pulsar.ReaderMessage, k.kafsarConfig.ConsumerReceiveQueueSize)
	options := pulsar.ReaderOptions{
		Topic:             partitionedTopic,
		Name:              subscriptionName,
		SubscriptionName:  subscriptionName,
		StartMessageID:    messageId,
		MessageChannel:    channel,
		ReceiverQueueSize: k.kafsarConfig.ConsumerReceiveQueueSize,
	}
	reader, err := client.CreateReader(options)
	if err != nil {
		return nil, nil, err
	}
	return channel, reader, nil
}

func (k *KafkaImpl) HeartBeat(addr net.Addr, req service.HeartBeatReq) *service.HeartBeatResp {
	k.mutex.RLock()
	user, exist := k.userInfoManager[addr.String()]
	k.mutex.RUnlock()
	if !exist {
		logrus.Errorf("offset fetch failed when get userinfo by addr %s", addr.String())
		return &service.HeartBeatResp{
			ErrorCode: service.UNKNOWN_SERVER_ERROR,
		}
	}
	resp := k.groupCoordinator.HandleHeartBeat(user.username, req.GroupId)
	if resp.ErrorCode == service.REBALANCE_IN_PROGRESS {
		group, err := k.groupCoordinator.GetGroup(user.username, req.GroupId)
		if err != nil {
			logrus.Errorf("offset fetch failed when get userinfo by addr %s", addr.String())
			return resp
		}
		for _, topic := range group.partitionedTopic {
			k.mutex.Lock()
			readerMetadata, exist := k.readerManager[topic+req.ClientId]
			if exist {
				readerMetadata.reader.Close()
				logrus.Infof("success close reader topic: %s", group.partitionedTopic)
				delete(k.readerManager, topic+req.ClientId)
				readerMetadata = nil
			}
			client, exist := k.pulsarClientManage[topic+req.ClientId]
			if exist {
				client.Close()
				delete(k.pulsarClientManage, topic+req.ClientId)
				client = nil
			}
			k.mutex.Unlock()
		}
	}
	return resp
}

func (k *KafkaImpl) PartitionNum(addr net.Addr, kafkaTopic string) (int, error) {
	k.mutex.RLock()
	user, exist := k.userInfoManager[addr.String()]
	k.mutex.RUnlock()
	if !exist {
		logrus.Errorf("get partitionNum failed. user is not found. topic: %s", kafkaTopic)
		return 0, errors.New("user not found.")
	}
	num, err := k.server.PartitionNum(user.username, kafkaTopic)
	if err != nil {
		logrus.Errorf("get partition num failed. topic: %s, err: %s", kafkaTopic, err)
		return 0, errors.New("get partition num failed.")
	}
	return num, nil
}

func (k *KafkaImpl) getPulsarHttpUrl() string {
	return fmt.Sprintf("http://%s:%d", k.pulsarConfig.Host, k.pulsarConfig.HttpPort)
}

func (k *KafkaImpl) checkPartitionTopicExist(topics []string, partitionTopic string) bool {
	for _, topic := range topics {
		if strings.EqualFold(topic, partitionTopic) {
			return true
		}
	}
	return false
}

func (k *KafkaImpl) checkTracer() bool {
	if k.tracer == nil {
		return false
	}
	if !k.tracer.enableTrace {
		return false
	}
	return true
}

// traceFetchLog trace fetch action, if isNewSpan == true, will create a new child span to log
// if isClose == true, will close when span is written
func (k *KafkaImpl) traceFetchLog(span TracerSpan, log string, isNewSpan, isClose bool) TracerSpan {
	if !k.checkTracer() {
		return span
	}
	if span.span == nil {
		return span
	}

	var childSpan TracerSpan
	if isNewSpan {
		childSpan = k.tracer.CreateLocalLogSpan(span.ctx)
	} else {
		childSpan = span
	}

	if isClose {
		k.tracer.SpanLogWithClose(childSpan, log)
		return childSpan
	}

	k.tracer.SpanLog(childSpan, log)
	return childSpan
}
