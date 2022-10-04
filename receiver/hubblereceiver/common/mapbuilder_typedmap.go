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
	"google.golang.org/protobuf/reflect/protoreflect"
)

type typedMap struct {
	list          []*commonV1.KeyValue
	labelsAsMaps  bool
	headersAsMaps bool
}

func (l *typedMap) items() []*commonV1.KeyValue { return l.list }

func (l *typedMap) newLeaf(_ string) leafer {
	return func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		mb := &typedMap{
			labelsAsMaps:  l.labelsAsMaps,
			headersAsMaps: l.headersAsMaps,
		}

		switch {
		case isRegularMessage(fd):
			item := &commonV1.AnyValue{}
			v.Message().Range(mb.newLeaf(""))
			item.Value = &commonV1.AnyValue_KvlistValue{
				KvlistValue: &commonV1.KeyValueList{
					Values: mb.items(),
				},
			}
			l.list = append(l.list, &commonV1.KeyValue{
				Key:   string(fd.Name()),
				Value: item,
			})
		default:
			if item := newValue(true, l.labelsAsMaps, l.headersAsMaps, fd, v, mb, ""); item != nil {
				l.list = append(l.list, &commonV1.KeyValue{
					Key:   string(fd.Name()),
					Value: item,
				})
			}
		}
		return true
	}
}
