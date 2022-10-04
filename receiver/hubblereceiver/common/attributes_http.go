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
	"strings"

	flowV1 "github.com/cilium/cilium/api/v1/flow"
	commonV1 "go.opentelemetry.io/proto/otlp/common/v1"
)

const (
	OTelAttrHTTPMethod     = "http.method"
	OTelAttrHTTPURL        = "http.url"
	OTelAttrHTTPFlavor     = "http.flavor"
	OTelAttrHTTPHost       = "http.host"
	OTelAttrHTTPUserAgent  = "http.user_agent"
	OTelAttrHTTPStatusCode = "http.status_code"

	OTelAttrHTTPRequestHeader  = "http.request.header."
	OTelAttrHTTPResponseHeader = "http.response.header."
)

func GetHTTPAttributes(l7 *flowV1.Layer7) []*commonV1.KeyValue {
	http := l7.GetHttp()
	if http == nil {
		return nil
	}

	base := map[string]string{
		OTelAttrHTTPMethod: http.Method,
		OTelAttrHTTPURL:    http.Url,
	}

	switch http.Protocol {
	case "HTTP/1.0":
		base[OTelAttrHTTPFlavor] = "1.0"
	case "HTTP/1.1":
		base[OTelAttrHTTPFlavor] = "1.1"
	case "HTTP/2.0":
		base[OTelAttrHTTPFlavor] = "2.0"
	case "SPDY":
		base[OTelAttrHTTPFlavor] = "SPDY"
	case "QUIC":
		base[OTelAttrHTTPFlavor] = "QUIC"
	}

	headers := map[string][]string{}

	appendHeader := func(k, v string) {
		if _, ok := headers[k]; ok {
			headers[k] = append(headers[k], v)
		} else {
			headers[k] = []string{v}
		}
	}

	for _, header := range http.Headers {
		k := NormaliseHeaderKey(header.Key)
		// this is duplicate of cilium.flow_event.l7.http.headers,
		// however key format is very nuanced, so keeping both
		// copies is deemed reasonable

		switch k {
		case "host":
			base[OTelAttrHTTPHost] = header.Value
			continue
		case "user_agent":
			base[OTelAttrHTTPUserAgent] = header.Value
			continue
		}

		switch l7.Type {
		case flowV1.L7FlowType_REQUEST:
			appendHeader(OTelAttrHTTPRequestHeader+k, header.Value)
		case flowV1.L7FlowType_RESPONSE:
			appendHeader(OTelAttrHTTPResponseHeader+k, header.Value)
		}
	}

	attributes := NewStringAttributes(base)

	if http.Code != 0 {
		attributes = append(attributes, &commonV1.KeyValue{
			Key: OTelAttrHTTPStatusCode,
			Value: &commonV1.AnyValue{
				Value: &commonV1.AnyValue_IntValue{
					IntValue: int64(http.Code),
				},
			},
		})
	}

	for k, v := range headers {
		attributes = append(attributes, &commonV1.KeyValue{
			Key:   k,
			Value: newStringArrayValue(v...),
		})
	}

	return attributes
}

func NormaliseHeaderKey(k string) string {
	return strings.ReplaceAll(strings.ToLower(k), "-", "_")
}
