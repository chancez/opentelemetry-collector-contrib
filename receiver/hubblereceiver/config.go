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
	"errors"
	"time"

	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/config/configgrpc"
	"google.golang.org/grpc/metadata"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/hubblereceiver/common"
)

type Config struct {
	config.ReceiverSettings       `mapstructure:",squash"`
	configgrpc.GRPCClientSettings `mapstructure:",squash"`

	BufferSize int `mapstructure:"buffer_size"`

	FlowEncodingOptions FlowEncodingOptions `mapstructure:"flow_encoding_options"`
	IncludeFlowTypes    IncludeFlowTypes    `mapstructure:"include_flow_types"`

	FallbackServiceNamePrefix string        `mapstructure:"fallback_service_name_prefix"`
	TraceCacheWindow          time.Duration `mapstructure:"trace_cache_window"`
	ParseTraceHeaders         bool          `mapstructure:"parse_trace_headers"`
}

type FlowEncodingOptions struct {
	Traces common.EncodingOptions `mapstructure:"traces"`
	Logs   common.EncodingOptions `mapstructure:"logs"`
}

type IncludeFlowTypes struct {
	Traces common.IncludeFlowTypes `mapstructure:"traces"`
	Logs   common.IncludeFlowTypes `mapstructure:"logs"`
}

var _ config.Receiver = (*Config)(nil)

func (cfg *Config) Validate() error {
	if cfg.Endpoint == "" {
		return errors.New("hubble endpoint must be specified")
	}
	if err := cfg.FlowEncodingOptions.Traces.ValidForTraces(); err != nil {
		return err
	}
	if err := cfg.FlowEncodingOptions.Logs.ValidForLogs(); err != nil {
		return err
	}
	if err := cfg.IncludeFlowTypes.Traces.Validate(); err != nil {
		return err
	}
	if err := cfg.IncludeFlowTypes.Logs.Validate(); err != nil {
		return err
	}
	return nil
}

func (cfg *Config) NewOutgoingContext(ctx context.Context) context.Context {
	if cfg.GRPCClientSettings.Headers == nil {
		return ctx
	}
	return metadata.NewOutgoingContext(ctx, metadata.New(cfg.GRPCClientSettings.Headers))
}
