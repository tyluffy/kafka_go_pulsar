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

func (s *Server) ListOffsetsVersion(ctx *ctx.NetworkContext, req *codec.ListOffsetsReq) (*codec.ListOffsetsResp, gnet.Action) {
	if !s.checkSasl(ctx) {
		return nil, gnet.Close
	}
	logrus.Debug("list offset req ", req)
	lowOffsetReqList := make([]*service.ListOffsetsTopicReq, len(req.TopicReqList))
	for i, topicReq := range req.TopicReqList {
		if !s.checkSaslTopic(ctx, topicReq.Topic, CONSUMER_PERMISSION_TYPE) {
			return nil, gnet.Close
		}
		lowTopicReq := &service.ListOffsetsTopicReq{}
		lowTopicReq.Topic = topicReq.Topic
		lowTopicReq.PartitionReqList = make([]*service.ListOffsetsPartitionReq, len(topicReq.PartitionReqList))
		for j, partitionReq := range topicReq.PartitionReqList {
			lowPartitionReq := &service.ListOffsetsPartitionReq{}
			lowPartitionReq.PartitionId = partitionReq.PartitionId
			lowPartitionReq.ClientId = req.ClientId
			lowPartitionReq.Time = partitionReq.Time
			lowTopicReq.PartitionReqList[j] = lowPartitionReq
		}
		lowOffsetReqList[i] = lowTopicReq
	}
	lowOffsetRespList, err := service.Offset(ctx.Addr, s.kafkaImpl, lowOffsetReqList)
	if err != nil {
		return nil, gnet.Close
	}
	resp := &codec.ListOffsetsResp{
		BaseResp: codec.BaseResp{
			CorrelationId: req.CorrelationId,
		},
	}
	resp.TopicRespList = make([]*codec.ListOffsetsTopicResp, len(lowOffsetRespList))
	for i, lowTopicResp := range lowOffsetRespList {
		f := &codec.ListOffsetsTopicResp{}
		f.Topic = lowTopicResp.Topic
		f.PartitionRespList = make([]*codec.ListOffsetsPartitionResp, len(lowTopicResp.PartitionRespList))
		for j, p := range lowTopicResp.PartitionRespList {
			partitionResp := &codec.ListOffsetsPartitionResp{}
			partitionResp.PartitionId = p.PartitionId
			partitionResp.ErrorCode = int16(p.ErrorCode)
			partitionResp.Timestamp = p.Time
			partitionResp.Offset = p.Offset
			partitionResp.LeaderEpoch = 0
			f.PartitionRespList[j] = partitionResp
		}
		resp.TopicRespList[i] = f
	}
	return resp, gnet.None
}
