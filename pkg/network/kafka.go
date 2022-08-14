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
	"encoding/binary"
	"fmt"
	"github.com/paashzj/kafka_go_pulsar/pkg/network/ctx"
	"github.com/paashzj/kafka_go_pulsar/pkg/service"
	"github.com/panjf2000/gnet"
	"github.com/protocol-laboratory/kafka-codec-go/codec"
	"github.com/protocol-laboratory/kafka-codec-go/kgnet"
	"github.com/sirupsen/logrus"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

// connCount kafka connection count
var connCount int32

var connMutex sync.Mutex

var serverConfig *kgnet.GnetConfig

func Run(config *kgnet.GnetConfig, kfkProtocolConfig *KafkaProtocolConfig, impl service.KfkServer) (*Server, error) {
	serverConfig = config
	server := &Server{
		EventServer:         nil,
		kafkaProtocolConfig: kfkProtocolConfig,
		kafkaImpl:           impl,
	}
	encoderConfig := gnet.EncoderConfig{
		ByteOrder:                       binary.BigEndian,
		LengthFieldLength:               4,
		LengthAdjustment:                0,
		LengthIncludesLengthFieldLength: false,
	}
	decoderConfig := gnet.DecoderConfig{
		ByteOrder:           binary.BigEndian,
		LengthFieldOffset:   0,
		LengthFieldLength:   4,
		LengthAdjustment:    0,
		InitialBytesToStrip: 4,
	}
	kfkCodec := gnet.NewLengthFieldBasedFrameCodec(encoderConfig, decoderConfig)
	go func() {
		err := gnet.Serve(server, fmt.Sprintf("tcp://%s:%d", config.ListenHost, config.ListenPort), gnet.WithNumEventLoop(config.EventLoopNum), gnet.WithCodec(kfkCodec))
		logrus.Error("kafsar broker started error ", err)
	}()
	return server, nil
}

func Close() (err error) {
	if serverConfig != nil {
		addr := fmt.Sprintf("tcp://%s:%d", serverConfig.ListenHost, serverConfig.ListenPort)
		err = gnet.Stop(context.Background(), addr)
	}
	return
}

type Server struct {
	*gnet.EventServer
	ConnMap             sync.Map
	SaslMap             sync.Map
	kafkaProtocolConfig *KafkaProtocolConfig
	kafkaImpl           service.KfkServer
}

func (s *Server) OnInitComplete(server gnet.Server) (action gnet.Action) {
	logrus.Info("Kafka Server started")
	return
}

// React Kafka 协议格式为APIKey和API Version
// APIKey 样例: 00 12
func (s *Server) React(frame []byte, c gnet.Conn) (bytes []byte, g gnet.Action) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Warn("Recovered in f", r, string(debug.Stack()))
			g = gnet.Close
		}
	}()
	logrus.Debug("frame len is ", len(frame))
	if len(frame) < 5 {
		logrus.Error("invalid data packet")
		return nil, gnet.Close
	}
	connMutex.Lock()
	connCtx := c.Context()
	if connCtx == nil {
		addr := c.RemoteAddr()
		c.SetContext(&ctx.NetworkContext{Addr: addr})
	}
	connMutex.Unlock()
	connCtx = c.Context()
	networkContext := connCtx.(*ctx.NetworkContext)
	apiKey := codec.ApiCode(binary.BigEndian.Uint16(frame))
	apiVersion := int16(binary.BigEndian.Uint16(frame[2:]))
	switch apiKey {
	case codec.ApiVersions:
		return s.ApiVersions(frame[4:], apiVersion)
	case codec.SaslHandshake:
		return s.SaslHandshake(frame[4:], apiVersion)
	case codec.SaslAuthenticate:
		return s.SaslAuthenticate(frame[4:], apiVersion, networkContext)
	case codec.Heartbeat:
		return s.Heartbeat(frame[4:], apiVersion, networkContext)
	case codec.JoinGroup:
		if !s.Authed(networkContext) {
			return s.AuthFailed()
		}
		return s.JoinGroup(networkContext, frame[4:], apiVersion)
	case codec.SyncGroup:
		if !s.Authed(networkContext) {
			return s.AuthFailed()
		}
		return s.SyncGroup(networkContext, frame[4:], apiVersion)
	case codec.OffsetFetch:
		if !s.Authed(networkContext) {
			return s.AuthFailed()
		}
		return s.OffsetFetch(networkContext, frame[4:], apiVersion)
	case codec.ListOffsets:
		if !s.Authed(networkContext) {
			return s.AuthFailed()
		}
		return s.ListOffsets(networkContext, frame[4:], apiVersion)
	case codec.Fetch:
		if !s.Authed(networkContext) {
			return s.AuthFailed()
		}
		return s.Fetch(networkContext, frame[4:], apiVersion)
	case codec.OffsetCommit:
		if !s.Authed(networkContext) {
			return s.AuthFailed()
		}
		return s.OffsetCommit(networkContext, frame[4:], apiVersion)
	case codec.OffsetForLeaderEpoch:
		if !s.Authed(networkContext) {
			return s.AuthFailed()
		}
		return s.OffsetForLeaderEpoch(networkContext, frame[4:], apiVersion)
	case codec.LeaveGroup:
		if !s.Authed(networkContext) {
			return s.AuthFailed()
		}
		return s.LeaveGroup(networkContext, frame[4:], apiVersion)
	case codec.Produce:
		if !s.Authed(networkContext) {
			return s.AuthFailed()
		}
		return s.Produce(networkContext, frame[4:], apiVersion, s.kafkaProtocolConfig)
	case codec.Metadata:
		if !s.Authed(networkContext) {
			return s.AuthFailed()
		}
		return s.Metadata(networkContext, frame[4:], apiVersion, s.kafkaProtocolConfig)
	case codec.FindCoordinator:
		if !s.Authed(networkContext) {
			return s.AuthFailed()
		}
		return s.FindCoordinator(frame[4:], apiVersion, s.kafkaProtocolConfig)
	}

	logrus.Error("unknown api ", apiKey, apiVersion)
	return nil, gnet.Close
}

func (s *Server) OnOpened(c gnet.Conn) (out []byte, action gnet.Action) {
	if atomic.LoadInt32(&connCount) > s.kafkaProtocolConfig.MaxConn {
		logrus.Error("connection reach max, refused to connect ", c.RemoteAddr())
		return nil, gnet.Close
	}
	connCount := atomic.AddInt32(&connCount, 1)
	s.ConnMap.Store(c.RemoteAddr(), c)
	logrus.Info("new connection connected ", connCount, " from ", c.RemoteAddr())
	return
}

func (s *Server) OnClosed(c gnet.Conn, err error) (action gnet.Action) {
	logrus.Info("connection closed from ", c.RemoteAddr())
	s.kafkaImpl.Disconnect(c.RemoteAddr())
	s.ConnMap.Delete(c.RemoteAddr())
	s.SaslMap.Delete(c.RemoteAddr())
	atomic.AddInt32(&connCount, -1)
	return
}
