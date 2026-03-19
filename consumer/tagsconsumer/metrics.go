// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tagsconsumer // import "go.opentelemetry.io/collector/consumer/tagsconsumer"

import (
	"context"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type metricsConsumer struct {
	next consumer.Metrics
	tags map[string]string
}

func (mc *metricsConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (mc *metricsConsumer) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	rms := md.ResourceMetrics()
	for i := range rms.Len() {
		attrs := rms.At(i).Resource().Attributes()
		for k, v := range mc.tags {
			attrs.PutStr(k, v)
		}
	}
	return mc.next.ConsumeMetrics(ctx, md)
}

// NewMetrics wraps next, stamping tags as resource attributes on every ResourceMetrics.
// If tags is empty, next is returned unwrapped.
func NewMetrics(next consumer.Metrics, tags map[string]string) consumer.Metrics {
	if len(tags) == 0 {
		return next
	}
	return &metricsConsumer{next: next, tags: tags}
}
