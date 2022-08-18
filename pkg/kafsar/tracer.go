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
	"fmt"
	"github.com/SkyAPM/go2sky"
	"github.com/SkyAPM/go2sky/reporter"
	"github.com/hashicorp/go-uuid"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	oteltrace "go.opentelemetry.io/otel/trace"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type TraceConfig struct {
	DisableTracing bool
	Host           string
	Port           int
	SampleRate     float64
}

type NoErrorTracer interface {
	// IsDisabled check whether tracer disabled
	IsDisabled() bool
	// NewProvider create a trace driver
	NewProvider()
	// NewSpan create span
	NewSpan(ctx context.Context, operateName string, logs ...string) LocalSpan
	// SetAttribute set attribute from localSpan
	SetAttribute(span LocalSpan, k, v string)
	// NewSubSpan create child span from localSpan
	NewSubSpan(span LocalSpan, operateName string, logs ...string) LocalSpan
	// EndSpan close span
	EndSpan(span LocalSpan, logs ...string)
}

type OtelTracerConfig TraceConfig
type SkywalkingTracerConfig TraceConfig

// TraceConfig impl NoErrorTracer interface
var _ NoErrorTracer = (*OtelTracerConfig)(nil)
var _ NoErrorTracer = (*SkywalkingTracerConfig)(nil)

func (ot *OtelTracerConfig) IsDisabled() bool {
	return ot.DisableTracing || ot.Host == "" || ot.Port == 0
}

func (ot *OtelTracerConfig) NewProvider() {
	if ot.IsDisabled() {
		return
	}
	var url = fmt.Sprintf("http://%s:%d/api/traces", ot.Host, ot.Port)
	exporter, err := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(url)))
	if err != nil {
		logrus.Errorf("init jaeger failed: %s", err.Error())
		return
	}
	provider := tracesdk.NewTracerProvider(
		// Always be sure to batch in production.
		tracesdk.WithBatcher(exporter),
		// Record information about this application in a Resource.
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(instanceName()),
		)),
		tracesdk.WithSampler(tracesdk.TraceIDRatioBased(ot.SampleRate)),
	)
	otel.SetTracerProvider(provider)
}

func (ot *OtelTracerConfig) NewSpan(ctx context.Context, opName string, logs ...string) LocalSpan {
	if ot.IsDisabled() {
		return LocalSpan{}
	}
	tracer := otel.Tracer(opName)
	childCtx, sp := tracer.Start(ctx, opName)
	if len(logs) != 0 {
		sp.SetAttributes(attribute.String("log", strings.Join(logs, "\n")))
	}
	span := LocalSpan{
		ctx:       childCtx,
		traceType: TraceTypeOtel,
		otel:      sp,
	}
	return span
}

func (ot *OtelTracerConfig) spanInvalid(span LocalSpan) bool {
	return ot.IsDisabled() || span.traceType != TraceTypeOtel || span.otel == nil
}

func (ot *OtelTracerConfig) NewSubSpan(span LocalSpan, opName string, logs ...string) LocalSpan {
	if ot.spanInvalid(span) {
		return LocalSpan{}
	}
	return ot.NewSpan(span.ctx, opName, logs...)
}

func (ot *OtelTracerConfig) EndSpan(span LocalSpan, logs ...string) {
	if ot.spanInvalid(span) {
		return
	}
	if len(logs) != 0 {
		span.otel.SetAttributes(attribute.String("end log", strings.Join(logs, "\n")))
	}
	span.otel.End()
}

func (ot *OtelTracerConfig) SetAttribute(span LocalSpan, k, v string) {
	if ot.spanInvalid(span) {
		return
	}
	span.otel.SetAttributes(attribute.String(k, v))
}

func (st *SkywalkingTracerConfig) NewProvider() {
	if st.IsDisabled() {
		return
	}
	grpcReporter, err := reporter.NewGRPCReporter(net.JoinHostPort(st.Host, strconv.Itoa(st.Port)))
	if err != nil {
		logrus.Errorf("skywalking init error, fall back to no trace, error message: %s", err.Error())
		return
	}
	tracer, err := go2sky.NewTracer("kafka_go_pulsar", go2sky.WithReporter(grpcReporter),
		go2sky.WithSampler(st.SampleRate), go2sky.WithInstance(instanceName()))
	if err != nil {
		logrus.Errorf("skywalking init error, fall back to no trace, error message: %s", err.Error())
		return
	}
	go2sky.SetGlobalTracer(tracer)
}

func (st *SkywalkingTracerConfig) NewSpan(ctx context.Context, operateName string, logs ...string) LocalSpan {
	if st.IsDisabled() {
		return LocalSpan{}
	}
	sp, childCtx, err := go2sky.GetGlobalTracer().CreateLocalSpan(ctx)
	if err != nil {
		logrus.Errorf("create new span failed: %s", err.Error())
		return LocalSpan{}
	}
	if len(logs) != 0 {
		sp.Log(time.Now(), logs...)
	}
	sp.SetOperationName(operateName)
	return LocalSpan{
		ctx:       childCtx,
		traceType: TraceTypeSkywalking,
		sw:        sp,
	}
}

func (st *SkywalkingTracerConfig) spanInvalid(span LocalSpan) bool {
	return st.IsDisabled() || span.traceType != TraceTypeSkywalking || span.sw == nil
}

func (st *SkywalkingTracerConfig) SetAttribute(span LocalSpan, k, v string) {
	if st.spanInvalid(span) {
		return
	}
	span.sw.Tag(go2sky.Tag(k), v)
}

func (st *SkywalkingTracerConfig) NewSubSpan(span LocalSpan, operateName string, logs ...string) LocalSpan {
	if st.spanInvalid(span) {
		return LocalSpan{}
	}
	return st.NewSpan(span.ctx, operateName, logs...)
}

func (st *SkywalkingTracerConfig) EndSpan(span LocalSpan, logs ...string) {
	if st.spanInvalid(span) {
		return
	}
	if len(logs) != 0 {
		span.sw.Log(time.Now(), logs...)
	}
	span.sw.End()
}

func (st *SkywalkingTracerConfig) IsDisabled() bool {
	return st.DisableTracing || st.Host == "" || st.Port == 0
}

type TraceType int

const (
	TraceTypeSkywalking TraceType = iota
	TraceTypeOtel
)

type LocalSpan struct {
	ctx       context.Context
	traceType TraceType
	sw        go2sky.Span
	otel      oteltrace.Span
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
