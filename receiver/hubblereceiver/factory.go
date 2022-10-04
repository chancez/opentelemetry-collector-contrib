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

package receiver

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hubblereceiver/common"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hubblereceiver/trace"
)

const (
	typeStr   = "hubble"
	stability = component.StabilityLevelAlpha
)

var receivers = make(map[config.Receiver]*hubbleReceiver)

func getReceiver(cfg config.Receiver, settings component.ReceiverCreateSettings) *hubbleReceiver {
	if r, ok := receivers[cfg]; ok {
		return r
	}
	r := newHubbleReceiver(cfg.(*Config), settings)
	receivers[cfg] = r
	return r
}
func NewFactory() component.ReceiverFactory {
	return component.NewReceiverFactory(
		typeStr,
		createDefaultConfig,
		component.WithTracesReceiver(createTracesReceiver, stability),
		component.WithLogsReceiver(createLogsReceiver, stability))
}

func createDefaultConfig() config.Receiver {
	_true, _false := new(bool), new(bool)
	*_true, *_false = true, false

	defaultTraceEncoding, defaultLogEncoding := common.DefaultTraceEncoding, common.DefaultLogEncoding

	return &Config{
		ReceiverSettings:          config.NewReceiverSettings(config.NewComponentID(typeStr)),
		BufferSize:                2048,
		FallbackServiceNamePrefix: common.OTelAttrServiceNameDefaultPrefix,
		TraceCacheWindow:          trace.DefaultTraceCacheWindow,
		ParseTraceHeaders:         true,
		FlowEncodingOptions: FlowEncodingOptions{
			Traces: common.EncodingOptions{
				Encoding:      &defaultTraceEncoding,
				LabelsAsMaps:  _true,
				HeadersAsMaps: _true,
				TopLevelKeys:  _true,
			},
			Logs: common.EncodingOptions{
				Encoding:         &defaultLogEncoding,
				LabelsAsMaps:     _true,
				HeadersAsMaps:    _true,
				TopLevelKeys:     _false,
				LogPayloadAsBody: _false,
			},
		},
		IncludeFlowTypes: IncludeFlowTypes{
			Traces: common.IncludeFlowTypes{},
			Logs:   common.IncludeFlowTypes{},
		},
	}
}

func createTracesReceiver(
	_ context.Context,
	settings component.ReceiverCreateSettings,
	cfg config.Receiver,
	nextConsumer consumer.Traces,
) (component.TracesReceiver, error) {
	r := getReceiver(cfg, settings)
	if err := r.registerTraceConsumer(nextConsumer); err != nil {
		return nil, err
	}
	return r, nil
}

func createLogsReceiver(
	_ context.Context,
	settings component.ReceiverCreateSettings,
	cfg config.Receiver,
	nextConsumer consumer.Logs,
) (component.LogsReceiver, error) {
	r := getReceiver(cfg, settings)
	if err := r.registerLogsConsumer(nextConsumer); err != nil {
		return nil, err
	}
	return r, nil
}
