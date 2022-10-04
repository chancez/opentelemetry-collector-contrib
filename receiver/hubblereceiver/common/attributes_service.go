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

package common

import (
	"net/url"

	flowV1 "github.com/cilium/cilium/api/v1/flow"
	hubbleLabels "github.com/cilium/hubble-ui/backend/domain/labels"
	commonV1 "go.opentelemetry.io/proto/otlp/common/v1"
)

const (
	OTelAttrServiceName      = "service.name"
	OTelAttrServiceNamespace = "service.namespace"

	OTelAttrServiceNameDefaultPrefix = "hubble-otel-unknown"
)

func GetServiceAttributes(flow *flowV1.Flow, fallbackServiceNamePrefix string) []*commonV1.KeyValue {
	resourceAttributes := map[string]string{
		OTelAttrServiceName: fallbackServiceNamePrefix + "-any",
	}

	if src := flow.Source; src != nil {
		switch srcProps := hubbleLabels.Props(src.Labels); {
		case srcProps.AppName != nil:
			resourceAttributes[OTelAttrServiceName] = *srcProps.AppName
			resourceAttributes[OTelAttrServiceNamespace] = src.Namespace
		case srcProps.IsHost:
			resourceAttributes[OTelAttrServiceName] = fallbackServiceNamePrefix + "-host"
		case srcProps.IsInit:
			resourceAttributes[OTelAttrServiceName] = fallbackServiceNamePrefix + "-init"
		case srcProps.IsKubeDNS:
			resourceAttributes[OTelAttrServiceName] = fallbackServiceNamePrefix + "-kube-dns"
		case srcProps.IsPrometheus:
			resourceAttributes[OTelAttrServiceName] = fallbackServiceNamePrefix + "-prometheus"
		case srcProps.IsRemoteNode:
			resourceAttributes[OTelAttrServiceName] = fallbackServiceNamePrefix + "-remote-node"
		case srcProps.IsWorld:
			resourceAttributes[OTelAttrServiceName] = fallbackServiceNamePrefix + "-world"
			// handle the case where pod name is known, but effective traffic source is world
			if serviceName := getServiceNameFromURL(flow); serviceName != "" {
				resourceAttributes[OTelAttrServiceName] = serviceName
			}
		}
	}

	return NewStringAttributes(resourceAttributes)
}

func getServiceNameFromURL(flow *flowV1.Flow) string {
	// there are cases where src.PodName could be set also,
	// but generally speaking it might just make sense to grab
	// the hostname from URL, it will also pickup internet
	// traffic also
	if l7 := flow.GetL7(); l7 != nil {
		if http := flow.GetL7().GetHttp(); http != nil {
			if u, err := url.Parse(http.Url); err == nil {
				return u.Hostname()
			}
		}
	}
	return ""
}
