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

func (s *Server) ReactFetch(ctx *ctx.NetworkContext, req *codec.FetchReq) (*codec.FetchResp, gnet.Action) {
	if !s.checkSasl(ctx) {
		return nil, gnet.Close
	}
	logrus.Debug("fetch req ", req)
	for _, topicReq := range req.TopicReqList {
		if !s.checkSaslTopic(ctx, topicReq.Topic, CONSUMER_PERMISSION_TYPE) {
			return nil, gnet.Close
		}
	}
	lowTopicRespList, err := s.kafsarImpl.Fetch(ctx.Addr, req)
	if err != nil {
		return nil, gnet.Close
	}
	resp := codec.NewFetchResp(req.CorrelationId)
	resp.TopicRespList = lowTopicRespList
	for i, lowTopicResp := range lowTopicRespList {
		for _, p := range lowTopicResp.PartitionRespList {
			if p.RecordBatch != nil {
				p.RecordBatch = s.convertRecordBatchResp(p.RecordBatch)
			}
		}
		resp.TopicRespList[i] = lowTopicResp
	}
	return resp, gnet.None
}

func (s *Server) convertRecordBatchResp(lowRecordBatch *codec.RecordBatch) *codec.RecordBatch {
	return &codec.RecordBatch{
		Offset:          lowRecordBatch.Offset,
		MessageSize:     lowRecordBatch.MessageSize,
		LeaderEpoch:     lowRecordBatch.LeaderEpoch,
		MagicByte:       2,
		Flags:           0,
		LastOffsetDelta: lowRecordBatch.LastOffsetDelta,
		FirstTimestamp:  lowRecordBatch.FirstTimestamp,
		LastTimestamp:   lowRecordBatch.LastTimestamp,
		ProducerId:      -1,
		ProducerEpoch:   -1,
		BaseSequence:    lowRecordBatch.BaseSequence,
		Records:         lowRecordBatch.Records,
	}
}
