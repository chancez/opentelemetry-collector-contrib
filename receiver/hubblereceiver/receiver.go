// Copyright  The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package receiver implements an OpenTelemetry receiver that connects to Hubble API
// and can produce trace or logs data.
package receiver

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"

	zaphook "github.com/Sytten/logrus-zap-hook"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hubblereceiver/common"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hubblereceiver/logs"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hubblereceiver/trace"
)

type hubbleReceiver struct {
	cfg    *Config
	ctx    context.Context
	cancel context.CancelFunc

	zapLogger    *zap.Logger
	logrusLogger *logrus.Logger

	hubbleConn *grpc.ClientConn

	traceReceiver *hubbleTraceReceiver
	logsReceiver  *hubbleLogsReceiver

	settings component.ReceiverCreateSettings
}

type (
	hubbleTraceReceiver struct {
		consumer consumer.Traces
	}
	hubbleLogsReceiver struct {
		consumer consumer.Logs
	}
)

func newHubbleReceiver(cfg *Config, settings component.ReceiverCreateSettings) *hubbleReceiver {
	logrusLogger := logrus.New()
	logrusLogger.ReportCaller = true
	logrusLogger.SetOutput(ioutil.Discard)
	hook, _ := zaphook.NewZapHook(settings.Logger)
	logrusLogger.Hooks.Add(hook)

	return &hubbleReceiver{
		cfg:          cfg,
		settings:     settings,
		zapLogger:    settings.Logger,
		logrusLogger: logrusLogger,
	}
}

func (r *hubbleReceiver) Start(_ context.Context, host component.Host) error {
	// custom background context must be used for long-running tasks
	// (see https://github.com/open-telemetry/opentelemetry-collector/blob/v0.38.0/component/component.go#L41-L45)
	r.ctx, r.cancel = context.WithCancel(context.Background())

	dialOpts, err := r.cfg.GRPCClientSettings.ToDialOptions(host, r.settings.TelemetrySettings)
	if err != nil {
		return err
	}
	r.zapLogger.Info("connecting to Hubble endpoint")
	r.hubbleConn, err = grpc.DialContext(r.ctx, r.cfg.GRPCClientSettings.SanitizedEndpoint(), dialOpts...)
	if err != nil {
		return fmt.Errorf("failed to connect to Hubble server: %w", err)
	}

	errs := make(chan error)

	if r.traceReceiver != nil {
		go r.traceReceiver.run(r.ctx, r.logrusLogger, r.hubbleConn, r.cfg, errs)
	}
	if r.logsReceiver != nil {
		go r.logsReceiver.run(r.ctx, r.logrusLogger, r.hubbleConn, r.cfg, errs)
	}

	go func() {
		for err := range errs {
			if err != nil {
				r.zapLogger.Error("hubble receiver error", zap.Error(err))
			}
		}
	}()

	return nil
}

func (r *hubbleReceiver) Shutdown(_ context.Context) error {
	r.cancel()
	_ = r.hubbleConn.Close()

	return nil
}

func (r *hubbleReceiver) registerTraceConsumer(tc consumer.Traces) error {
	if tc == nil {
		return component.ErrNilNextConsumer
	}
	r.traceReceiver = &hubbleTraceReceiver{
		consumer: tc,
	}
	return nil
}

func (r *hubbleReceiver) registerLogsConsumer(lc consumer.Logs) error {
	if lc == nil {
		return component.ErrNilNextConsumer
	}
	r.logsReceiver = &hubbleLogsReceiver{
		consumer: lc,
	}
	return nil
}

func (r *hubbleTraceReceiver) run(ctx context.Context, log *logrus.Logger, hubbleConn *grpc.ClientConn, cfg *Config, errs chan<- error) error {
	log.Info("starting Hubble trace receiver")

	spanDB, err := os.MkdirTemp("", "hubble-otel-trace-cache-") // TODO: allow user to pass dir name for persistence
	if err != nil {
		return fmt.Errorf("failed to create temporary directory for span database: %w", err)
	}

	flowsToTraces := make(chan protoreflect.Message, cfg.BufferSize)

	converter, err := trace.NewFlowConverter(log, spanDB, &cfg.FlowEncodingOptions.Traces, &cfg.IncludeFlowTypes.Traces, cfg.FallbackServiceNamePrefix, cfg.TraceCacheWindow, cfg.ParseTraceHeaders)
	if err != nil {
		return fmt.Errorf("failed to create trace converter: %w", err)
	}

	go common.RunConverter(cfg.NewOutgoingContext(ctx), hubbleConn, converter, flowsToTraces, errs, grpc.WaitForReady(cfg.GRPCClientSettings.WaitForReady))

	exporter := trace.NewBufferedDirectTraceExporter(log, r.consumer, cfg.BufferSize)
	go common.RunExporter(ctx, log, exporter, flowsToTraces, errs)

	return nil
}

func (r *hubbleLogsReceiver) run(ctx context.Context, log *logrus.Logger, hubbleConn *grpc.ClientConn, cfg *Config, errs chan<- error) error {
	log.Info("starting Hubble logs receiver")

	flowsToLogs := make(chan protoreflect.Message, cfg.BufferSize)

	converter := logs.NewFlowConverter(log, &cfg.FlowEncodingOptions.Logs, &cfg.IncludeFlowTypes.Logs, cfg.FallbackServiceNamePrefix)
	go common.RunConverter(cfg.NewOutgoingContext(ctx), hubbleConn, converter, flowsToLogs, errs, grpc.WaitForReady(cfg.GRPCClientSettings.WaitForReady))

	exporter := logs.NewBufferedDirectLogsExporter(log, r.consumer, cfg.BufferSize)
	go common.RunExporter(ctx, log, exporter, flowsToLogs, errs)

	return nil
}
