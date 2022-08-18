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
	"context"
	"github.com/paashzj/kafka_go_pulsar/pkg/network/ctx"
	"github.com/panjf2000/gnet"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/protocol-laboratory/kafka-codec-go/kgnet"
	"github.com/sirupsen/logrus"
	"net"
	"sync"
	"sync/atomic"
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

func NewServer(config *kgnet.GnetServerConfig, kfkProtocolConfig *KafkaProtocolConfig, impl KafsarServer) (*Server, error) {
	server := &Server{
		kafkaProtocolConfig: kfkProtocolConfig,
		kafsarImpl:          impl,
	}
	server.kafkaServer = kgnet.NewKafkaServer(*config, server)
	return server, nil
}

func (s *Server) Run() error {
	go func() {
		err := s.kafkaServer.Run()
		if err != nil {
			logrus.Error("kafsar broker started error ", err)
		}
	}()
	return nil
}

func (s *Server) Close(ctx context.Context) (err error) {
	return s.kafkaServer.Stop(ctx)
}

func (s *Server) OnInitComplete(server gnet.Server) (action gnet.Action) {
	logrus.Info("Kafka Server started")
	return
}

func (s *Server) OnOpened(c gnet.Conn) (out []byte, action gnet.Action) {
	if atomic.LoadInt32(&s.connCount) > s.kafkaProtocolConfig.MaxConn {
		logrus.Error("connection reach max, refused to connect ", c.RemoteAddr())
		return nil, gnet.Close
	}
	connCount := atomic.AddInt32(&s.connCount, 1)
	s.ConnMap.Store(c.RemoteAddr(), c)
	logrus.Info("new connection connected ", connCount, " from ", c.RemoteAddr())
	return
}

func (s *Server) OnClosed(c gnet.Conn, err error) (action gnet.Action) {
	logrus.Info("connection closed from ", c.RemoteAddr())
	s.kafsarImpl.Disconnect(c.RemoteAddr())
	s.ConnMap.Delete(c.RemoteAddr())
	s.SaslMap.Delete(c.RemoteAddr())
	atomic.AddInt32(&s.connCount, -1)
	return
}

func (s *Server) InvalidKafkaPacket(c gnet.Conn) {
	logrus.Error("invalid data packet", c.RemoteAddr())
}

func (s *Server) ConnError(c gnet.Conn, err error) {
	logrus.Warn("conn error", c.RemoteAddr(), err)
}

func (s *Server) UnSupportedApi(c gnet.Conn, apiKey codec.ApiCode, apiVersion int16) {
	logrus.Error("unsupported api ", c.RemoteAddr(), apiKey, apiVersion)
}

func (s *Server) ApiVersion(c gnet.Conn, req *codec.ApiReq) (*codec.ApiResp, gnet.Action) {
	version := req.ApiVersion
	if version <= 3 {
		return s.ReactApiVersion(req)
	}
	logrus.Warn("Unsupported apiVersion version", version)
	return nil, gnet.Close
}

func (s *Server) Fetch(c gnet.Conn, req *codec.FetchReq) (*codec.FetchResp, gnet.Action) {
	networkContext := s.getCtx(c)
	if !s.Authed(networkContext) {
		return nil, gnet.Close
	}
	version := req.ApiVersion
	if version == 10 || version == 11 {
		return s.ReactFetch(networkContext, req)
	}
	return nil, gnet.Close
}

func (s *Server) FindCoordinator(c gnet.Conn, req *codec.FindCoordinatorReq) (*codec.FindCoordinatorResp, gnet.Action) {
	networkContext := s.getCtx(c)
	if !s.Authed(networkContext) {
		return nil, gnet.Close
	}
	version := req.ApiVersion
	if version == 0 || version == 3 {
		return s.ReactFindCoordinator(req, s.kafkaProtocolConfig)
	}
	return nil, gnet.Close
}

func (s *Server) Heartbeat(c gnet.Conn, req *codec.HeartbeatReq) (*codec.HeartbeatResp, gnet.Action) {
	networkContext := s.getCtx(c)
	version := req.ApiVersion
	if version == 4 {
		return s.ReactHeartbeat(req, networkContext)
	}
	logrus.Warn("Unsupported heartbeat version", version)
	return nil, gnet.Close
}

func (s *Server) JoinGroup(c gnet.Conn, req *codec.JoinGroupReq) (*codec.JoinGroupResp, gnet.Action) {
	networkContext := s.getCtx(c)
	if !s.Authed(networkContext) {
		return nil, gnet.Close
	}
	version := req.ApiVersion
	if version == 1 || version == 6 {
		return s.ReactJoinGroup(networkContext, req)
	}
	logrus.Warn("Unsupported joinGroup version", version)
	return nil, gnet.Close
}

func (s *Server) LeaveGroup(c gnet.Conn, req *codec.LeaveGroupReq) (*codec.LeaveGroupResp, gnet.Action) {
	networkContext := s.getCtx(c)
	if !s.Authed(networkContext) {
		return nil, gnet.Close
	}
	version := req.ApiVersion
	if version == 0 || version == 4 {
		return s.ReactLeaveGroup(networkContext, req)
	}
	logrus.Warn("Unsupported leaveGroup version", version)
	return nil, gnet.Close
}

func (s *Server) ListOffsets(c gnet.Conn, req *codec.ListOffsetsReq) (*codec.ListOffsetsResp, gnet.Action) {
	networkContext := s.getCtx(c)
	if !s.Authed(networkContext) {
		return nil, gnet.Close
	}
	version := req.ApiVersion
	if version == 1 || version == 5 {
		return s.ListOffsetsVersion(networkContext, req)
	}
	logrus.Warn("Unsupported listOffsets version", version)
	return nil, gnet.Close
}

func (s *Server) Metadata(c gnet.Conn, req *codec.MetadataReq) (*codec.MetadataResp, gnet.Action) {
	networkContext := s.getCtx(c)
	if !s.Authed(networkContext) {
		return nil, gnet.Close
	}
	version := req.ApiVersion
	if version == 1 || version == 8 || version == 9 {
		return s.ReactMetadata(networkContext, req, s.kafkaProtocolConfig)
	}
	logrus.Warn("Unsupported metadata version", version)
	return nil, gnet.Close
}

func (s *Server) OffsetCommit(c gnet.Conn, req *codec.OffsetCommitReq) (*codec.OffsetCommitResp, gnet.Action) {
	networkContext := s.getCtx(c)
	if !s.Authed(networkContext) {
		return nil, gnet.Close
	}
	version := req.ApiVersion
	if version == 2 || version == 8 {
		return s.OffsetCommitVersion(networkContext, req)
	}
	logrus.Warn("Unsupported offsetCommit version", version)
	return nil, gnet.Close
}

func (s *Server) OffsetFetch(c gnet.Conn, req *codec.OffsetFetchReq) (*codec.OffsetFetchResp, gnet.Action) {
	networkContext := s.getCtx(c)
	if !s.Authed(networkContext) {
		return nil, gnet.Close
	}
	version := req.ApiVersion
	if version == 1 || version == 6 || version == 7 {
		return s.OffsetFetchVersion(networkContext, req)
	}
	logrus.Warn("Unsupported offsetFetch version", version)
	return nil, gnet.Close
}

func (s *Server) OffsetForLeaderEpoch(c gnet.Conn, req *codec.OffsetForLeaderEpochReq) (*codec.OffsetForLeaderEpochResp, gnet.Action) {
	networkContext := s.getCtx(c)
	if !s.Authed(networkContext) {
		return nil, gnet.Close
	}
	version := req.ApiVersion
	if version == 3 {
		return s.OffsetForLeaderEpochVersion(networkContext, req)
	}
	logrus.Warn("Unsupported offsetForLeaderEpoch version", version)
	return nil, gnet.Close
}

func (s *Server) Produce(c gnet.Conn, req *codec.ProduceReq) (*codec.ProduceResp, gnet.Action) {
	networkContext := s.getCtx(c)
	if !s.Authed(networkContext) {
		return nil, gnet.Close
	}
	version := req.ApiVersion
	if version == 7 || version == 8 {
		return s.ReactProduce(networkContext, req, s.kafkaProtocolConfig)
	}
	logrus.Warn("Unsupported producer version", version)
	return nil, gnet.Close
}

func (s *Server) SaslAuthenticate(c gnet.Conn, req *codec.SaslAuthenticateReq) (*codec.SaslAuthenticateResp, gnet.Action) {
	networkContext := s.getCtx(c)
	version := req.ApiVersion
	if version == 1 || version == 2 {
		return s.ReactSaslHandshakeAuth(req, networkContext)
	}
	logrus.Warn("Unsupported saslAuthenticate version", version)
	return nil, gnet.Close
}

func (s *Server) SaslHandshake(c gnet.Conn, req *codec.SaslHandshakeReq) (*codec.SaslHandshakeResp, gnet.Action) {
	version := req.ApiVersion
	if version == 1 || version == 2 {
		return s.ReactSasl(req)
	}
	return nil, gnet.Close
}

func (s *Server) SyncGroup(c gnet.Conn, req *codec.SyncGroupReq) (*codec.SyncGroupResp, gnet.Action) {
	networkContext := s.getCtx(c)
	if !s.Authed(networkContext) {
		return nil, gnet.Close
	}
	version := req.ApiVersion
	if version == 1 || version == 4 || version == 5 {
		return s.ReactSyncGroup(networkContext, req)
	}
	logrus.Warn("Unsupported syncGroup version", version)
	return nil, gnet.Close
}

func (s *Server) getCtx(c gnet.Conn) *ctx.NetworkContext {
	s.connMutex.Lock()
	connCtx := c.Context()
	if connCtx == nil {
		addr := c.RemoteAddr()
		c.SetContext(&ctx.NetworkContext{Addr: addr})
	}
	s.connMutex.Unlock()
	return c.Context().(*ctx.NetworkContext)
}

type Server struct {
	connCount           int32
	connMutex           sync.Mutex
	ConnMap             sync.Map
	SaslMap             sync.Map
	kafkaProtocolConfig *KafkaProtocolConfig
	kafsarImpl          KafsarServer
	kafkaServer         *kgnet.KafkaServer
}
