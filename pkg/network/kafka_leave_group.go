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

func (s *Server) ReactLeaveGroup(ctx *ctx.NetworkContext, req *codec.LeaveGroupReq) (*codec.LeaveGroupResp, gnet.Action) {
	if !s.checkSaslGroup(ctx, req.GroupId) {
		return nil, gnet.Close
	}
	logrus.Debug("leave group req ", req)
	lowReq := &codec.LeaveGroupReq{}
	lowReq.GroupId = req.GroupId
	lowReq.ClientId = req.ClientId
	lowReq.Members = make([]*codec.LeaveGroupMember, len(req.Members))
	for i, member := range req.Members {
		m := &codec.LeaveGroupMember{}
		m.MemberId = member.MemberId
		m.GroupInstanceId = member.GroupInstanceId
		lowReq.Members[i] = m
	}
	resp := &codec.LeaveGroupResp{
		BaseResp: codec.BaseResp{
			CorrelationId: req.CorrelationId,
		},
	}
	lowResp, err := s.kafsarImpl.GroupLeave(ctx.Addr, lowReq)
	if err != nil {
		return nil, gnet.Close
	}
	resp.ErrorCode = lowResp.ErrorCode
	resp.Members = make([]*codec.LeaveGroupMember, len(lowResp.Members))
	for i, member := range lowResp.Members {
		m := &codec.LeaveGroupMember{}
		m.MemberId = member.MemberId
		m.GroupInstanceId = member.GroupInstanceId
		resp.Members[i] = m
	}
	resp.MemberErrorCode = lowResp.MemberErrorCode
	return resp, gnet.None
}
