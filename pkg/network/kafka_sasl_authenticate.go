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

func (s *Server) ReactSaslHandshakeAuth(req *codec.SaslAuthenticateReq, context *ctx.NetworkContext) (*codec.SaslAuthenticateResp, gnet.Action) {
	logrus.Debug("sasl handshake request ", req)
	saslHandshakeResp := &codec.SaslAuthenticateResp{
		BaseResp: codec.BaseResp{
			CorrelationId: req.CorrelationId,
		},
	}
	saslReq := codec.SaslAuthenticateReq{Username: req.Username, Password: req.Password, BaseReq: codec.BaseReq{ClientId: req.ClientId}}
	authResult, errorCode := service.SaslAuth(context.Addr, s.kafsarImpl, saslReq)
	if errorCode != 0 {
		return nil, gnet.Close
	}
	if authResult {
		context.Authed(true)
		s.SaslMap.Store(context.Addr, saslReq)
		return saslHandshakeResp, gnet.None
	} else {
		return nil, gnet.Close
	}
}
