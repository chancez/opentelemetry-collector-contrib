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

package logs

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	logsCollectorV1 "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	logsV1 "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type BufferedLogExporter struct {
	otlpLogs          logsCollectorV1.LogsServiceClient
	bufferSize        int
	headers           map[string]string
	exportCallOptions []grpc.CallOption
}

func NewBufferedLogExporter(otlpConn *grpc.ClientConn, bufferSize int, headers map[string]string, callOptions ...grpc.CallOption) *BufferedLogExporter {
	return &BufferedLogExporter{
		otlpLogs:          logsCollectorV1.NewLogsServiceClient(otlpConn),
		bufferSize:        bufferSize,
		exportCallOptions: callOptions,
	}
}

func (s *BufferedLogExporter) Export(ctx context.Context, flows <-chan protoreflect.Message) error {
	logs := make([]*logsV1.ResourceLogs, s.bufferSize)

	for i := range logs {
		flow, ok := (<-flows).Interface().(*logsV1.ResourceLogs)
		if !ok {
			return fmt.Errorf("cannot convert protoreflect.Message to logsV1.ResourceLogs")
		}
		logs[i] = flow
	}

	if s.headers != nil {
		ctx = metadata.NewOutgoingContext(ctx, metadata.New(s.headers))
	}
	_, err := s.otlpLogs.Export(ctx, &logsCollectorV1.ExportLogsServiceRequest{ResourceLogs: logs}, s.exportCallOptions...)
	return err
}

type BufferedDirectLogsExporter struct {
	log         *logrus.Logger
	consumer    consumer.Logs
	bufferSize  int
	unmarshaler plog.Unmarshaler
}

func NewBufferedDirectLogsExporter(log *logrus.Logger, consumer consumer.Logs, bufferSize int) *BufferedDirectLogsExporter {
	return &BufferedDirectLogsExporter{
		log:         log,
		consumer:    consumer,
		bufferSize:  bufferSize,
		unmarshaler: plog.NewProtoUnmarshaler(),
	}
}

func (s *BufferedDirectLogsExporter) Export(ctx context.Context, flows <-chan protoreflect.Message) error {
	logs := make([]*logsV1.ResourceLogs, s.bufferSize)

	for i := range logs {
		flow, ok := (<-flows).Interface().(*logsV1.ResourceLogs)
		if !ok {
			return fmt.Errorf("cannot convert protoreflect.Message to logsV1.ResourceLogs")
		}
		logs[i] = flow
	}

	// there is no clear way of converting public Go types (go.opentelemetry.io/proto/otlp)
	// and internal collector types (go.opentelemetry.io/collector/model);
	// see https://github.com/open-telemetry/opentelemetry-collector/issues/4254
	data, err := proto.Marshal(&logsCollectorV1.ExportLogsServiceRequest{ResourceLogs: logs})
	if err != nil {
		return fmt.Errorf("cannot marshal lgs: %w", err)
	}
	unmarshalledLogs, err := s.unmarshaler.UnmarshalLogs(data)
	if err != nil {
		return fmt.Errorf("cannot unmarshal logs: %w", err)
	}

	s.log.Debug("flushing log buffer to the consumer")
	return s.consumer.ConsumeLogs(ctx, unmarshalledLogs)
}
