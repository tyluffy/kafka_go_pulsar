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
	"github.com/panjf2000/gnet"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/sirupsen/logrus"
)

func (s *Server) ApiVersions(frame []byte, version int16) ([]byte, gnet.Action) {
	if version == 0 || version == 3 {
		return s.ReactApiVersion(frame, version)
	}
	logrus.Error("unknown api version ", version)
	return nil, gnet.Close
}

func (s *Server) ReactApiVersion(frame []byte, version int16) ([]byte, gnet.Action) {
	apiRequest, r, stack := codec.DecodeApiReq(frame, version)
	if r != nil {
		logrus.Warn("decode api versions error", r, string(stack))
		return nil, gnet.Close
	}
	logrus.Debug("api request ", apiRequest)
	resp := codec.ApiResp{
		BaseResp: codec.BaseResp{
			CorrelationId: apiRequest.CorrelationId,
		},
	}
	resp.ErrorCode = 0
	apiRespVersions := make([]*codec.ApiRespVersion, 20)
	apiRespVersions[0] = &codec.ApiRespVersion{ApiKey: codec.Produce, MinVersion: 0, MaxVersion: 9}
	apiRespVersions[1] = &codec.ApiRespVersion{ApiKey: codec.Fetch, MinVersion: 0, MaxVersion: 12}
	apiRespVersions[2] = &codec.ApiRespVersion{ApiKey: codec.ListOffsets, MinVersion: 0, MaxVersion: 6}
	apiRespVersions[3] = &codec.ApiRespVersion{ApiKey: codec.Metadata, MinVersion: 0, MaxVersion: 11}
	apiRespVersions[4] = &codec.ApiRespVersion{ApiKey: codec.OffsetCommit, MinVersion: 0, MaxVersion: 8}
	apiRespVersions[5] = &codec.ApiRespVersion{ApiKey: codec.OffsetFetch, MinVersion: 0, MaxVersion: 7}
	apiRespVersions[6] = &codec.ApiRespVersion{ApiKey: codec.FindCoordinator, MinVersion: 0, MaxVersion: 3}
	apiRespVersions[7] = &codec.ApiRespVersion{ApiKey: codec.JoinGroup, MinVersion: 0, MaxVersion: 7}
	apiRespVersions[8] = &codec.ApiRespVersion{ApiKey: codec.Heartbeat, MinVersion: 0, MaxVersion: 4}
	apiRespVersions[9] = &codec.ApiRespVersion{ApiKey: codec.LeaveGroup, MinVersion: 0, MaxVersion: 4}
	apiRespVersions[10] = &codec.ApiRespVersion{ApiKey: codec.SyncGroup, MinVersion: 0, MaxVersion: 5}
	apiRespVersions[11] = &codec.ApiRespVersion{ApiKey: codec.DescribeGroups, MinVersion: 0, MaxVersion: 5}
	apiRespVersions[12] = &codec.ApiRespVersion{ApiKey: codec.ListGroups, MinVersion: 0, MaxVersion: 4}
	apiRespVersions[13] = &codec.ApiRespVersion{ApiKey: codec.SaslHandshake, MinVersion: 0, MaxVersion: 1}
	apiRespVersions[14] = &codec.ApiRespVersion{ApiKey: codec.ApiVersions, MinVersion: 0, MaxVersion: 3}
	apiRespVersions[15] = &codec.ApiRespVersion{ApiKey: codec.CreateTopics, MinVersion: 0, MaxVersion: 7}
	apiRespVersions[16] = &codec.ApiRespVersion{ApiKey: codec.DeleteTopics, MinVersion: 0, MaxVersion: 6}
	apiRespVersions[17] = &codec.ApiRespVersion{ApiKey: codec.DeleteRecords, MinVersion: 0, MaxVersion: 2}
	apiRespVersions[18] = &codec.ApiRespVersion{ApiKey: codec.OffsetForLeaderEpoch, MinVersion: 0, MaxVersion: 4}
	apiRespVersions[19] = &codec.ApiRespVersion{ApiKey: codec.SaslAuthenticate, MinVersion: 0, MaxVersion: 2}
	resp.ApiRespVersions = apiRespVersions
	resp.ThrottleTime = 0
	apiResponses := &resp
	return apiResponses.Bytes(version), gnet.None
}
