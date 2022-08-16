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

package service

import (
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"net"
)

type KafsarServer interface {
	PartitionNum(addr net.Addr, topic string) (int, error)

	// Fetch method called this already authed
	Fetch(addr net.Addr, req *codec.FetchReq) ([]*codec.FetchTopicResp, error)

	// GroupJoin method called this already authed
	GroupJoin(addr net.Addr, req *codec.JoinGroupReq) (*codec.JoinGroupResp, error)

	// GroupLeave method called this already authed
	GroupLeave(addr net.Addr, req *codec.LeaveGroupReq) (*codec.LeaveGroupResp, error)

	// GroupSync method called this already authed
	GroupSync(addr net.Addr, req *codec.SyncGroupReq) (*codec.SyncGroupResp, error)

	// OffsetListPartition method called this already authed
	OffsetListPartition(addr net.Addr, topic, clientID string, req *codec.ListOffsetsPartition) (*codec.ListOffsetsPartitionResp, error)

	// OffsetCommitPartition method called this already authed
	OffsetCommitPartition(addr net.Addr, topic, clientID string, req *codec.OffsetCommitPartitionReq) (*codec.OffsetCommitPartitionResp, error)

	// OffsetFetch method called this already authed
	OffsetFetch(addr net.Addr, topic, clientID, groupID string, req *codec.OffsetFetchPartitionReq) (*codec.OffsetFetchPartitionResp, error)

	// OffsetLeaderEpoch method called this already authed
	OffsetLeaderEpoch(addr net.Addr, topic string, req *codec.OffsetLeaderEpochPartitionReq) (*codec.OffsetForLeaderEpochPartitionResp, error)

	// Produce method called this already authed
	Produce(addr net.Addr, topic string, partition int, req *codec.ProducePartitionReq) (*codec.ProducePartitionResp, error)

	SaslAuth(addr net.Addr, req codec.SaslAuthenticateReq) (bool, codec.ErrorCode)

	SaslAuthTopic(addr net.Addr, req codec.SaslAuthenticateReq, topic, permissionType string) (bool, codec.ErrorCode)

	SaslAuthConsumerGroup(addr net.Addr, req codec.SaslAuthenticateReq, consumerGroup string) (bool, codec.ErrorCode)

	HeartBeat(addr net.Addr, req codec.HeartbeatReq) *codec.HeartbeatResp

	Disconnect(addr net.Addr)
}
