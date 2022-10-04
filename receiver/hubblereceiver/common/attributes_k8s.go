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
	commonV1 "go.opentelemetry.io/proto/otlp/common/v1"

	flowV1 "github.com/cilium/cilium/api/v1/flow"
)

const (
	OTelAttrK8sNodeName      = "k8s.node.name"
	OTelAttrK8sNamespaceName = "k8s.namespace.name"
	OTelAttrK8sPodName       = "k8s.pod.name"
)

func GetKubernetesAttributes(flow *flowV1.Flow) []*commonV1.KeyValue {
	resourceAttributes := map[string]string{
		OTelAttrK8sNodeName: flow.GetNodeName(),
	}

	if src := flow.Source; src != nil {
		if src.Namespace != "" {
			resourceAttributes[OTelAttrK8sNamespaceName] = src.Namespace
		}
		if src.PodName != "" {
			resourceAttributes[OTelAttrK8sPodName] = src.PodName
		}
	}

	return NewStringAttributes(resourceAttributes)
}
