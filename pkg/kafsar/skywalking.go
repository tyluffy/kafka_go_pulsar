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

package kafsar

import (
	"context"
	"errors"
	"github.com/SkyAPM/go2sky"
	"github.com/SkyAPM/go2sky/propagation"
	"github.com/SkyAPM/go2sky/reporter"
	"github.com/hashicorp/go-uuid"
	"github.com/sirupsen/logrus"
	"net"
	"os"
	"strconv"
	"time"
)

type NoErrorTracer struct {
	enableTrace bool
	tracer      *go2sky.Tracer
}

type TracerSpan struct {
	span go2sky.Span
	ctx  context.Context
}

func NewTracer(config TraceConfig) *NoErrorTracer {
	n := &NoErrorTracer{}
	if config.SkywalkingHost == "" || config.SkywalkingPort == 0 || config.DisableTracing {
		return n
	}

	grpcReporter, err := reporter.NewGRPCReporter(net.JoinHostPort(config.SkywalkingHost, strconv.Itoa(config.SkywalkingPort)))
	if err != nil {
		logrus.Errorf("skywalking init error, fall back to no trace, error message: %s", err.Error())
		return n
	}

	name := instanceName()
	logrus.Infof("start skywalking with instance name %s", name)
	t, err := go2sky.NewTracer("kafka_go_pulsar", go2sky.WithReporter(grpcReporter), go2sky.WithSampler(config.SampleRate), go2sky.WithInstance(name))
	if err != nil {
		logrus.Errorf("skywalking init error, fall back to no trace, error message: %s", err.Error())
		return n
	}

	n.tracer = t
	n.enableTrace = true
	return n
}

func (n *NoErrorTracer) CreateEntrySpan(ctx context.Context, operationName string, extractor propagation.Extractor) (go2sky.Span, context.Context, error) {
	if !n.enableTrace {
		return nil, nil, errors.New("disable tracing now")
	}
	return n.tracer.CreateEntrySpan(ctx, operationName, extractor)
}

// SpanLog write a log base exist span
func (n *NoErrorTracer) SpanLog(span TracerSpan, log ...string) {
	if !n.enableTrace {
		return
	}
	if span.span == nil {
		return
	}
	span.span.Log(time.Now(), log...)
}

// SpanLogWithClose write a log then close it
func (n *NoErrorTracer) SpanLogWithClose(span TracerSpan, log ...string) {
	if !n.enableTrace {
		return
	}
	if span.span == nil {
		return
	}
	span.span.Log(time.Now(), log...)
	span.span.End()
}

func (n *NoErrorTracer) CreateLocalSpan(ctx context.Context) (go2sky.Span, context.Context, error) {
	if !n.enableTrace {
		return nil, nil, errors.New("disable tracing now")
	}
	return n.tracer.CreateLocalSpan(ctx)
}

func (n *NoErrorTracer) CreateLocalLogSpan(ctx context.Context) TracerSpan {
	if !n.enableTrace {
		return TracerSpan{}
	}
	span, newCtx, err := n.CreateLocalSpan(ctx)
	if err != nil {
		return TracerSpan{}
	}
	return TracerSpan{span: span, ctx: newCtx}
}

func instanceName() string {
	hostname, err := os.Hostname()
	if err == nil {
		return hostname
	}
	generateUUID, err := uuid.GenerateUUID()
	if err == nil {
		return generateUUID
	}
	return "kafka-go-pulsar"
}
