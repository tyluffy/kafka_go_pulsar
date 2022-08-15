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
)

func (s *Server) OffsetForLeaderEpochVersion(ctx *ctx.NetworkContext, req *codec.OffsetForLeaderEpochReq) (*codec.OffsetForLeaderEpochResp, gnet.Action) {
	if !s.checkSasl(ctx) {
		return nil, gnet.Close
	}
	lowReqList := make([]*service.OffsetLeaderEpochTopicReq, len(req.TopicReqList))
	for i, topicReq := range req.TopicReqList {
		if !s.checkSaslTopic(ctx, topicReq.Topic, CONSUMER_PERMISSION_TYPE) {
			return nil, gnet.Close
		}
		lowTopicReq := &service.OffsetLeaderEpochTopicReq{}
		lowTopicReq.Topic = topicReq.Topic
		lowTopicReq.PartitionReqList = make([]*service.OffsetLeaderEpochPartitionReq, len(topicReq.PartitionReqList))
		for j, partitionReq := range topicReq.PartitionReqList {
			lowPartitionReq := &service.OffsetLeaderEpochPartitionReq{}
			lowPartitionReq.PartitionId = partitionReq.PartitionId
			lowPartitionReq.CurrentLeaderEpoch = partitionReq.CurrentLeaderEpoch
			lowPartitionReq.LeaderEpoch = partitionReq.LeaderEpoch
			lowTopicReq.PartitionReqList[j] = lowPartitionReq
		}
		lowReqList[i] = lowTopicReq
	}
	lowTopicRespList, err := service.OffsetLeaderEpoch(ctx.Addr, s.kafsarImpl, lowReqList)
	if err != nil {
		return nil, gnet.Close
	}
	resp := &codec.OffsetForLeaderEpochResp{
		BaseResp: codec.BaseResp{
			CorrelationId: req.CorrelationId,
		},
	}
	resp.TopicRespList = make([]*codec.OffsetForLeaderEpochTopicResp, len(lowTopicRespList))
	for i, lowTopicResp := range lowTopicRespList {
		f := &codec.OffsetForLeaderEpochTopicResp{}
		f.Topic = lowTopicResp.Topic
		f.PartitionRespList = make([]*codec.OffsetForLeaderEpochPartitionResp, len(lowTopicResp.PartitionRespList))
		for j, p := range lowTopicResp.PartitionRespList {
			partitionResp := &codec.OffsetForLeaderEpochPartitionResp{}
			partitionResp.ErrorCode = p.ErrorCode
			partitionResp.PartitionId = p.PartitionId
			partitionResp.LeaderEpoch = p.LeaderEpoch
			partitionResp.Offset = p.Offset
			f.PartitionRespList[j] = partitionResp
		}
		resp.TopicRespList[i] = f
	}
	return resp, gnet.None
}
