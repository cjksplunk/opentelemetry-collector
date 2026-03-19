// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tagsconsumer // import "go.opentelemetry.io/collector/consumer/tagsconsumer"

import (
	"context"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
)

type logsConsumer struct {
	next consumer.Logs
	tags map[string]string
}

func (lc *logsConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

func (lc *logsConsumer) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	rls := ld.ResourceLogs()
	for i := range rls.Len() {
		attrs := rls.At(i).Resource().Attributes()
		for k, v := range lc.tags {
			attrs.PutStr(k, v)
		}
	}
	return lc.next.ConsumeLogs(ctx, ld)
}

// NewLogs wraps next, stamping tags as resource attributes on every ResourceLogs.
// If tags is empty, next is returned unwrapped.
func NewLogs(next consumer.Logs, tags map[string]string) consumer.Logs {
	if len(tags) == 0 {
		return next
	}
	return &logsConsumer{next: next, tags: tags}
}