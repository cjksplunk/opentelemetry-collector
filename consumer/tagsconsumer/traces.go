// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tagsconsumer // import "go.opentelemetry.io/collector/consumer/tagsconsumer"

import (
	"context"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type tracesConsumer struct {
	next consumer.Traces
	tags map[string]string
}

func (tc *tracesConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (tc *tracesConsumer) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	rss := td.ResourceSpans()
	for i := range rss.Len() {
		attrs := rss.At(i).Resource().Attributes()
		for k, v := range tc.tags {
			attrs.PutStr(k, v)
		}
	}
	return tc.next.ConsumeTraces(ctx, td)
}

// NewTraces wraps next, stamping tags as resource attributes on every ResourceSpans.
// If tags is empty, next is returned unwrapped.
func NewTraces(next consumer.Traces, tags map[string]string) consumer.Traces {
	if len(tags) == 0 {
		return next
	}
	return &tracesConsumer{next: next, tags: tags}
}
