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
	lowResp, err := s.kafsarImpl.GroupSync(ctx.Addr, req)
	if err != nil {
		return nil, gnet.Close
	}
	return &codec.SyncGroupResp{
		BaseResp: codec.BaseResp{
			CorrelationId: req.CorrelationId,
		},
		ThrottleTime:     lowResp.ThrottleTime,
		ErrorCode:        lowResp.ErrorCode,
		ProtocolType:     lowResp.ProtocolType,
		ProtocolName:     lowResp.ProtocolName,
		MemberAssignment: lowResp.MemberAssignment,
	}, gnet.None
}
