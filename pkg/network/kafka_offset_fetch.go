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

func (s *Server) OffsetFetch(ctx *ctx.NetworkContext, frame []byte, version int16) ([]byte, gnet.Action) {
	if version == 1 || version == 6 || version == 7 {
		return s.OffsetFetchVersion(ctx, frame, version)
	}
	logrus.Error("unknown offset fetch version ", version)
	return nil, gnet.Close
}

func (s *Server) OffsetFetchVersion(ctx *ctx.NetworkContext, frame []byte, version int16) ([]byte, gnet.Action) {
	req, r, stack := codec.DecodeOffsetFetchReq(frame, version)
	if r != nil {
		logrus.Warn("decode offset fetch error", r, string(stack))
		return nil, gnet.Close
	}
	if !s.checkSasl(ctx) {
		return nil, gnet.Close
	}
	logrus.Debug("offset fetch req ", req)
	lowReq := &service.OffsetFetchReq{}
	lowReq.TopicReqList = make([]*service.OffsetFetchTopicReq, len(req.TopicReqList))
	for i, topicReq := range req.TopicReqList {
		if !s.checkSaslTopic(ctx, topicReq.Topic, CONSUMER_PERMISSION_TYPE) {
			return nil, gnet.Close
		}
		lowTopicReq := &service.OffsetFetchTopicReq{}
		lowTopicReq.Topic = topicReq.Topic
		lowTopicReq.PartitionReqList = make([]*service.OffsetFetchPartitionReq, len(topicReq.PartitionReqList))
		for j, partitionReq := range topicReq.PartitionReqList {
			lowPartitionReq := &service.OffsetFetchPartitionReq{}
			lowPartitionReq.PartitionId = partitionReq.PartitionId
			lowPartitionReq.ClientId = req.ClientId
			lowPartitionReq.GroupId = req.GroupId
			lowTopicReq.PartitionReqList[j] = lowPartitionReq
		}
		lowReq.TopicReqList[i] = lowTopicReq
	}
	lowResp, err := service.OffsetFetch(ctx.Addr, s.kafkaImpl, lowReq)
	if err != nil {
		return nil, gnet.Close
	}
	resp := codec.OffsetFetchResp{
		BaseResp: codec.BaseResp{
			CorrelationId: req.CorrelationId,
		},
	}
	resp.ErrorCode = lowResp.ErrorCode
	resp.TopicRespList = make([]*codec.OffsetFetchTopicResp, len(lowResp.TopicRespList))
	for i, lowTopicResp := range lowResp.TopicRespList {
		f := &codec.OffsetFetchTopicResp{}
		f.Topic = lowTopicResp.Topic
		f.PartitionRespList = make([]*codec.OffsetFetchPartitionResp, len(lowTopicResp.PartitionRespList))
		for j, p := range lowTopicResp.PartitionRespList {
			partitionResp := &codec.OffsetFetchPartitionResp{}
			partitionResp.PartitionId = p.PartitionId
			partitionResp.Offset = p.Offset
			partitionResp.LeaderEpoch = p.LeaderEpoch
			partitionResp.Metadata = p.Metadata
			f.PartitionRespList[j] = partitionResp
		}
		resp.TopicRespList[i] = f
	}
	return resp.Bytes(version), gnet.None
}
