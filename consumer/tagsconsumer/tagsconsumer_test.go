// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tagsconsumer

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// --- Logs ---

func TestNewLogs_NoTags_ReturnsNext(t *testing.T) {
	next, err := consumer.NewLogs(func(context.Context, plog.Logs) error { return nil })
	require.NoError(t, err)
	got := NewLogs(next, nil)
	assert.Same(t, next, got)

	got2 := NewLogs(next, map[string]string{})
	assert.Same(t, next, got2)
}

func TestNewLogs_Capabilities(t *testing.T) {
	next, err := consumer.NewLogs(func(context.Context, plog.Logs) error { return nil })
	require.NoError(t, err)
	w := NewLogs(next, map[string]string{"k": "v"})
	assert.Equal(t, consumer.Capabilities{MutatesData: true}, w.Capabilities())
}

func TestNewLogs_StampsResourceAttributes(t *testing.T) {
	var received plog.Logs
	next, err := consumer.NewLogs(func(_ context.Context, ld plog.Logs) error {
		received = ld
		return nil
	})
	require.NoError(t, err)

	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty()

	tags := map[string]string{"env": "prod", "region": "us-east-1"}
	w := NewLogs(next, tags)
	require.NoError(t, w.ConsumeLogs(context.Background(), ld))

	attrs := received.ResourceLogs().At(0).Resource().Attributes()
	v, ok := attrs.Get("env")
	require.True(t, ok)
	assert.Equal(t, "prod", v.Str())

	v, ok = attrs.Get("region")
	require.True(t, ok)
	assert.Equal(t, "us-east-1", v.Str())
}

func TestNewLogs_TagsOverrideExistingAttributes(t *testing.T) {
	var received plog.Logs
	next, err := consumer.NewLogs(func(_ context.Context, ld plog.Logs) error {
		received = ld
		return nil
	})
	require.NoError(t, err)

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("env", "staging")

	w := NewLogs(next, map[string]string{"env": "prod"})
	require.NoError(t, w.ConsumeLogs(context.Background(), ld))

	v, ok := received.ResourceLogs().At(0).Resource().Attributes().Get("env")
	require.True(t, ok)
	assert.Equal(t, "prod", v.Str())
}

func TestNewLogs_PropagatesError(t *testing.T) {
	want := errors.New("downstream error")
	next, err := consumer.NewLogs(func(context.Context, plog.Logs) error { return want })
	require.NoError(t, err)

	w := NewLogs(next, map[string]string{"k": "v"})
	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty()
	assert.Equal(t, want, w.ConsumeLogs(context.Background(), ld))
}

func TestNewLogs_MultipleResourceLogs(t *testing.T) {
	var received plog.Logs
	next, err := consumer.NewLogs(func(_ context.Context, ld plog.Logs) error {
		received = ld
		return nil
	})
	require.NoError(t, err)

	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty()
	ld.ResourceLogs().AppendEmpty()

	w := NewLogs(next, map[string]string{"host": "myhost"})
	require.NoError(t, w.ConsumeLogs(context.Background(), ld))

	for i := range received.ResourceLogs().Len() {
		v, ok := received.ResourceLogs().At(i).Resource().Attributes().Get("host")
		require.True(t, ok, "resource %d missing tag", i)
		assert.Equal(t, "myhost", v.Str())
	}
}

// --- Metrics ---

func TestNewMetrics_NoTags_ReturnsNext(t *testing.T) {
	next, err := consumer.NewMetrics(func(context.Context, pmetric.Metrics) error { return nil })
	require.NoError(t, err)
	got := NewMetrics(next, nil)
	assert.Same(t, next, got)
}

func TestNewMetrics_Capabilities(t *testing.T) {
	next, err := consumer.NewMetrics(func(context.Context, pmetric.Metrics) error { return nil })
	require.NoError(t, err)
	w := NewMetrics(next, map[string]string{"k": "v"})
	assert.Equal(t, consumer.Capabilities{MutatesData: true}, w.Capabilities())
}

func TestNewMetrics_StampsResourceAttributes(t *testing.T) {
	var received pmetric.Metrics
	next, err := consumer.NewMetrics(func(_ context.Context, md pmetric.Metrics) error {
		received = md
		return nil
	})
	require.NoError(t, err)

	md := pmetric.NewMetrics()
	md.ResourceMetrics().AppendEmpty()

	w := NewMetrics(next, map[string]string{"env": "prod"})
	require.NoError(t, w.ConsumeMetrics(context.Background(), md))

	v, ok := received.ResourceMetrics().At(0).Resource().Attributes().Get("env")
	require.True(t, ok)
	assert.Equal(t, "prod", v.Str())
}

func TestNewMetrics_PropagatesError(t *testing.T) {
	want := errors.New("downstream error")
	next, err := consumer.NewMetrics(func(context.Context, pmetric.Metrics) error { return want })
	require.NoError(t, err)

	w := NewMetrics(next, map[string]string{"k": "v"})
	md := pmetric.NewMetrics()
	md.ResourceMetrics().AppendEmpty()
	assert.Equal(t, want, w.ConsumeMetrics(context.Background(), md))
}

// --- Traces ---

func TestNewTraces_NoTags_ReturnsNext(t *testing.T) {
	next, err := consumer.NewTraces(func(context.Context, ptrace.Traces) error { return nil })
	require.NoError(t, err)
	got := NewTraces(next, nil)
	assert.Same(t, next, got)
}

func TestNewTraces_Capabilities(t *testing.T) {
	next, err := consumer.NewTraces(func(context.Context, ptrace.Traces) error { return nil })
	require.NoError(t, err)
	w := NewTraces(next, map[string]string{"k": "v"})
	assert.Equal(t, consumer.Capabilities{MutatesData: true}, w.Capabilities())
}

func TestNewTraces_StampsResourceAttributes(t *testing.T) {
	var received ptrace.Traces
	next, err := consumer.NewTraces(func(_ context.Context, td ptrace.Traces) error {
		received = td
		return nil
	})
	require.NoError(t, err)

	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty()

	w := NewTraces(next, map[string]string{"env": "prod"})
	require.NoError(t, w.ConsumeTraces(context.Background(), td))

	v, ok := received.ResourceSpans().At(0).Resource().Attributes().Get("env")
	require.True(t, ok)
	assert.Equal(t, "prod", v.Str())
}

func TestNewTraces_PropagatesError(t *testing.T) {
	want := errors.New("downstream error")
	next, err := consumer.NewTraces(func(context.Context, ptrace.Traces) error { return want })
	require.NoError(t, err)

	w := NewTraces(next, map[string]string{"k": "v"})
	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty()
	assert.Equal(t, want, w.ConsumeTraces(context.Background(), td))
}
