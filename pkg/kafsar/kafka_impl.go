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
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/paashzj/kafka_go/pkg/service"
	"github.com/sirupsen/logrus"
	"net"
)

type KafkaImpl struct {
	server           Server
	pulsarConfig     PulsarConfig
	pulsarClient     pulsar.Client
	groupCoordinator *GroupCoordinatorImpl
	kafsarConfig     KafsarConfig
}

func (k *KafkaImpl) InitGroupCoordinator() (err error) {
	k.groupCoordinator = &GroupCoordinatorImpl{pulsarConfig: k.pulsarConfig, KafsarConfig: k.kafsarConfig, pulsarClient: k.pulsarClient}
	k.groupCoordinator.GroupManager = make(map[string]Group)
	return
}

func (k *KafkaImpl) Produce(addr net.Addr, topic string, partition int, req *service.ProducePartitionReq) (*service.ProducePartitionResp, error) {
	panic("implement me")
}

func (k *KafkaImpl) ConnPulsar() (err error) {
	k.pulsarClient, err = pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
	return
}

func (k *KafkaImpl) FetchPartition(addr net.Addr, topic string, req *service.FetchPartitionReq) (*service.FetchPartitionResp, error) {
	panic("implement me")
}

func (k *KafkaImpl) GroupJoin(addr net.Addr, req *service.JoinGroupReq) (*service.JoinGroupResp, error) {
	logrus.Infof("%#v joining to group: %s, memberId: %s", addr, req.GroupId, req.MemberId)
	joinGroupResp, err := k.groupCoordinator.HandleJoinGroup(req.GroupId, req.MemberId, req.ClientId, req.ProtocolType,
		req.SessionTimeout, req.GroupProtocols)
	if err != nil {
		logrus.Errorf("unexpected exception in join group: %s, error: %s", req.GroupId, err)
		return &service.JoinGroupResp{
			ErrorCode:    service.UNKNOWN_SERVER_ERROR,
			MemberId:     req.MemberId,
			GenerationId: -1,
		}, nil
	}
	return joinGroupResp, nil
}

func (k *KafkaImpl) GroupLeave(addr net.Addr, req *service.LeaveGroupReq) (*service.LeaveGroupResp, error) {
	logrus.Infof("%+v leaving group: %s, members: %+v", addr, req.GroupId, req.Members)
	leaveGroupResp, err := k.groupCoordinator.HandleLeaveGroup(req.GroupId, req.Members)
	if err != nil {
		logrus.Errorf("unexpected exception in leaving group: %s, error: %s", req.GroupId, err)
		return &service.LeaveGroupResp{
			ErrorCode: service.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	return leaveGroupResp, nil
}

func (k *KafkaImpl) GroupSync(addr net.Addr, req *service.SyncGroupReq) (*service.SyncGroupResp, error) {
	logrus.Infof("%+v syncing group: %s, memberId: %s", addr, req.GroupId, req.MemberId)
	syncGroupResp, err := k.groupCoordinator.HandleSyncGroup(req.GroupId, req.MemberId, req.GenerationId, req.GroupAssignments)
	if err != nil {
		logrus.Errorf("unexpected exception in sync group: %s, error: %s", req.GroupId, err)
		return &service.SyncGroupResp{
			ErrorCode: service.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	return syncGroupResp, nil
}

func (k *KafkaImpl) OffsetListPartition(addr net.Addr, topic string, req *service.ListOffsetsPartitionReq) (*service.ListOffsetsPartitionResp, error) {
	panic("implement me")
}

func (k *KafkaImpl) OffsetCommitPartition(addr net.Addr, topic string, req *service.OffsetCommitPartitionReq) (*service.OffsetCommitPartitionResp, error) {
	panic("implement me")
}

func (k *KafkaImpl) OffsetFetch(addr net.Addr, topic string, req *service.OffsetFetchPartitionReq) (*service.OffsetFetchPartitionResp, error) {
	panic("implement me")
}

func (k *KafkaImpl) SaslAuth(req service.SaslReq) (bool, service.ErrorCode) {
	auth, err := k.server.Auth(req.Username, req.Password, req.ClientId)
	if err != nil || !auth {
		return false, service.SASL_AUTHENTICATION_FAILED
	}
	return true, service.NONE
}

func (k *KafkaImpl) SaslAuthTopic(req service.SaslReq, topic, permissionType string) (bool, service.ErrorCode) {
	auth, err := k.server.AuthTopic(req.Username, req.Password, req.ClientId, topic, permissionType)
	if err != nil || !auth {
		return false, service.SASL_AUTHENTICATION_FAILED
	}
	return true, service.NONE
}

func (k *KafkaImpl) SaslAuthConsumerGroup(req service.SaslReq, consumerGroup string) (bool, service.ErrorCode) {
	auth, err := k.server.AuthTopicGroup(req.Username, req.Password, req.ClientId, consumerGroup)
	if err != nil || !auth {
		return false, service.SASL_AUTHENTICATION_FAILED
	}
	return true, service.NONE
}

func (k *KafkaImpl) Disconnect(addr net.Addr) {
	panic("implement me")
}
