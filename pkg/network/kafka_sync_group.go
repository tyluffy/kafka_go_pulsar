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

func (s *Server) ReactSyncGroup(ctx *ctx.NetworkContext, req *codec.SyncGroupReq) (*codec.SyncGroupResp, gnet.Action) {
	if !s.checkSaslGroup(ctx, req.GroupId) {
		return nil, gnet.Close
	}
	logrus.Debug("sync group req", req)
	lowReq := &codec.SyncGroupReq{}
	lowReq.GroupId = req.GroupId
	lowReq.ClientId = req.ClientId
	lowReq.GenerationId = req.GenerationId
	lowReq.MemberId = req.MemberId
	lowReq.GroupInstanceId = req.GroupInstanceId
	lowReq.ProtocolType = req.ProtocolType
	lowReq.ProtocolName = req.ProtocolName
	lowReq.GroupAssignments = make([]*codec.GroupAssignment, len(req.GroupAssignments))
	for i, groupAssignment := range req.GroupAssignments {
		g := &codec.GroupAssignment{}
		g.MemberAssignment = groupAssignment.MemberAssignment
		g.MemberId = groupAssignment.MemberId
		lowReq.GroupAssignments[i] = g
	}
	resp := &codec.SyncGroupResp{
		BaseResp: codec.BaseResp{
			CorrelationId: req.CorrelationId,
		},
	}
	lowResp, err := s.kafsarImpl.GroupSync(ctx.Addr, lowReq)
	if err != nil {
		return nil, gnet.Close
	}
	resp.ProtocolType = lowResp.ProtocolType
	resp.ProtocolName = lowResp.ProtocolName
	resp.MemberAssignment = lowResp.MemberAssignment
	return resp, gnet.None
}
