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

package network

import (
	"github.com/paashzj/kafka_go_pulsar/pkg/network/ctx"
	"github.com/paashzj/kafka_go_pulsar/pkg/service"
	"github.com/panjf2000/gnet"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/sirupsen/logrus"
)

func (s *Server) Metadata(ctx *ctx.NetworkContext, frame []byte, version int16, config *KafkaProtocolConfig) ([]byte, gnet.Action) {
	if version == 1 || version == 9 {
		return s.ReactMetadataVersion(ctx, frame, version, config)
	}
	logrus.Error("unknown metadata version ", version)
	return nil, gnet.Close
}

func (s *Server) ReactMetadataVersion(ctx *ctx.NetworkContext, frame []byte, version int16, config *KafkaProtocolConfig) ([]byte, gnet.Action) {
	metadataTopicReq, r, stack := codec.DecodeMetadataReq(frame, version)
	if r != nil {
		logrus.Warn("decode metadata error", r, string(stack))
		return nil, gnet.Close
	}
	logrus.Debug("metadata req ", metadataTopicReq)
	topics := metadataTopicReq.Topics
	if len(topics) > 1 {
		logrus.Error("currently, not support more than one topic")
		return nil, gnet.Close
	}
	topic := topics[0].Topic
	var metadataResp *codec.MetadataResp
	partitionNum, err := s.kafkaImpl.PartitionNum(ctx.Addr, topic)
	if err != nil {
		metadataResp2 := codec.MetadataResp{}
		metadataResp2.CorrelationId = metadataTopicReq.CorrelationId
		metadataResp2.BrokerMetadataList = make([]*codec.BrokerMetadata, 1)
		metadataResp2.BrokerMetadataList[0] = &codec.BrokerMetadata{NodeId: config.NodeId, Host: config.AdvertiseHost, Port: int(config.AdvertisePort), Rack: nil}
		metadataResp2.ClusterId = config.ClusterId
		metadataResp2.ControllerId = config.NodeId
		metadataResp2.TopicMetadataList = make([]*codec.TopicMetadata, 1)
		topicMetadata := codec.TopicMetadata{ErrorCode: int16(service.UNKNOWN_SERVER_ERROR), Topic: topic, IsInternal: false, TopicAuthorizedOperation: -2147483648}
		topicMetadata.PartitionMetadataList = make([]*codec.PartitionMetadata, 0)
		metadataResp2.TopicMetadataList[0] = &topicMetadata
		metadataResp2.ClusterAuthorizedOperation = -2147483648
		metadataResp = &metadataResp2
	} else {
		metadataResp2 := codec.MetadataResp{}
		metadataResp2.CorrelationId = metadataTopicReq.CorrelationId
		metadataResp2.BrokerMetadataList = make([]*codec.BrokerMetadata, 1)
		metadataResp2.BrokerMetadataList[0] = &codec.BrokerMetadata{NodeId: config.NodeId, Host: config.AdvertiseHost, Port: int(config.AdvertisePort), Rack: nil}
		metadataResp2.ClusterId = config.ClusterId
		metadataResp2.ControllerId = config.NodeId
		metadataResp2.TopicMetadataList = make([]*codec.TopicMetadata, 1)
		topicMetadata := codec.TopicMetadata{ErrorCode: 0, Topic: topic, IsInternal: false, TopicAuthorizedOperation: -2147483648}
		topicMetadata.PartitionMetadataList = make([]*codec.PartitionMetadata, partitionNum)
		for i := 0; i < partitionNum; i++ {
			partitionMetadata := &codec.PartitionMetadata{ErrorCode: 0, PartitionId: i, LeaderId: config.NodeId, LeaderEpoch: 0, OfflineReplicas: nil}
			replicas := make([]*codec.Replica, 1)
			replicas[0] = &codec.Replica{ReplicaId: config.NodeId}
			partitionMetadata.Replicas = replicas
			partitionMetadata.CaughtReplicas = replicas
			topicMetadata.PartitionMetadataList[i] = partitionMetadata
		}
		metadataResp2.TopicMetadataList[0] = &topicMetadata
		metadataResp2.ClusterAuthorizedOperation = -2147483648
		metadataResp = &metadataResp2
	}
	return metadataResp.Bytes(version), gnet.None
}
