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
	"github.com/panjf2000/gnet"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/sirupsen/logrus"
)

func (s *Server) OffsetCommitVersion(ctx *ctx.NetworkContext, req *codec.OffsetCommitReq) (*codec.OffsetCommitResp, gnet.Action) {
	if !s.checkSasl(ctx) {
		return nil, gnet.Close
	}
	logrus.Debug("offset commit req ", req)
	resp := make([]*codec.OffsetCommitTopicResp, len(req.TopicReqList))
	for i, topicReq := range req.TopicReqList {
		if !s.checkSaslTopic(ctx, topicReq.Topic, CONSUMER_PERMISSION_TYPE) {
			return nil, gnet.Close
		}
		f := &codec.OffsetCommitTopicResp{
			Topic:             topicReq.Topic,
			PartitionRespList: make([]*codec.OffsetCommitPartitionResp, len(topicReq.PartitionReqList)),
		}
		for j, partitionReq := range topicReq.PartitionReqList {
			var err error
			f.PartitionRespList[j], err = s.kafsarImpl.OffsetCommitPartition(ctx.Addr, topicReq.Topic, req.ClientId, partitionReq)
			if err != nil {
				return nil, gnet.Close
			}
		}
		resp[i] = f
	}
	return &codec.OffsetCommitResp{
		BaseResp: codec.BaseResp{
			CorrelationId: req.CorrelationId,
		},
		TopicRespList: resp,
	}, gnet.None
}
