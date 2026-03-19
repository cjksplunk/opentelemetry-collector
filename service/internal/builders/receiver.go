// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package builders // import "go.opentelemetry.io/collector/service/internal/builders"

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/tagsconsumer"
	"go.opentelemetry.io/collector/consumer/xconsumer"
	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.opentelemetry.io/collector/receiver/xreceiver"
)

// ReceiverBuilder receiver is a helper struct that given a set of Configs and
// Factories helps with creating receivers.
type ReceiverBuilder struct {
	cfgs      map[component.ID]component.Config
	tags      map[component.ID]map[string]string
	factories map[component.Type]receiver.Factory
}

// ReceiverBuilderOption is a functional option for ReceiverBuilder.
type ReceiverBuilderOption func(*ReceiverBuilder)

// WithReceiverTags sets per-receiver tags that will be stamped as resource
// attributes on all telemetry produced by the receiver.
func WithReceiverTags(tags map[component.ID]map[string]string) ReceiverBuilderOption {
	return func(b *ReceiverBuilder) {
		b.tags = tags
	}
}

// NewReceiver creates a new ReceiverBuilder to help with creating
// components form a set of configs and factories.
func NewReceiver(cfgs map[component.ID]component.Config, factories map[component.Type]receiver.Factory, opts ...ReceiverBuilderOption) *ReceiverBuilder {
	b := &ReceiverBuilder{cfgs: cfgs, factories: factories}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// CreateTraces creates a Traces receiver based on the settings and config.
func (b *ReceiverBuilder) CreateTraces(ctx context.Context, set receiver.Settings, next consumer.Traces) (receiver.Traces, error) {
	if next == nil {
		return nil, errNilNextConsumer
	}
	cfg, existsCfg := b.cfgs[set.ID]
	if !existsCfg {
		return nil, fmt.Errorf("receiver %q is not configured", set.ID)
	}

	f, existsFactory := b.factories[set.ID.Type()]
	if !existsFactory {
		return nil, fmt.Errorf("receiver factory not available for: %q", set.ID)
	}

	logDeprecatedTypeAlias(set.Logger, f, set.ID.Type())
	logStabilityLevel(set.Logger, f.TracesStability())
	if len(b.tags) > 0 {
		if tags := b.tags[set.ID]; len(tags) > 0 {
			set.Logger.Debug("applying receiver tags to traces", zap.Any("tags", tags))
			next = tagsconsumer.NewTraces(next, tags)
		}
	}
	return f.CreateTraces(ctx, set, cfg, next)
}

// CreateMetrics creates a Metrics receiver based on the settings and config.
func (b *ReceiverBuilder) CreateMetrics(ctx context.Context, set receiver.Settings, next consumer.Metrics) (receiver.Metrics, error) {
	if next == nil {
		return nil, errNilNextConsumer
	}
	cfg, existsCfg := b.cfgs[set.ID]
	if !existsCfg {
		return nil, fmt.Errorf("receiver %q is not configured", set.ID)
	}

	f, existsFactory := b.factories[set.ID.Type()]
	if !existsFactory {
		return nil, fmt.Errorf("receiver factory not available for: %q", set.ID)
	}

	logDeprecatedTypeAlias(set.Logger, f, set.ID.Type())
	logStabilityLevel(set.Logger, f.MetricsStability())
	if len(b.tags) > 0 {
		if tags := b.tags[set.ID]; len(tags) > 0 {
			set.Logger.Debug("applying receiver tags to metrics", zap.Any("tags", tags))
			next = tagsconsumer.NewMetrics(next, tags)
		}
	}
	return f.CreateMetrics(ctx, set, cfg, next)
}

// CreateLogs creates a Logs receiver based on the settings and config.
func (b *ReceiverBuilder) CreateLogs(ctx context.Context, set receiver.Settings, next consumer.Logs) (receiver.Logs, error) {
	if next == nil {
		return nil, errNilNextConsumer
	}
	cfg, existsCfg := b.cfgs[set.ID]
	if !existsCfg {
		return nil, fmt.Errorf("receiver %q is not configured", set.ID)
	}

	f, existsFactory := b.factories[set.ID.Type()]
	if !existsFactory {
		return nil, fmt.Errorf("receiver factory not available for: %q", set.ID)
	}

	logDeprecatedTypeAlias(set.Logger, f, set.ID.Type())
	logStabilityLevel(set.Logger, f.LogsStability())
	if len(b.tags) > 0 {
		if tags := b.tags[set.ID]; len(tags) > 0 {
			set.Logger.Debug("applying receiver tags to logs", zap.Any("tags", tags))
			next = tagsconsumer.NewLogs(next, tags)
		}
	}
	return f.CreateLogs(ctx, set, cfg, next)
}

// CreateProfiles creates a Profiles receiver based on the settings and config.
func (b *ReceiverBuilder) CreateProfiles(ctx context.Context, set receiver.Settings, next xconsumer.Profiles) (xreceiver.Profiles, error) {
	if next == nil {
		return nil, errNilNextConsumer
	}
	cfg, existsCfg := b.cfgs[set.ID]
	if !existsCfg {
		return nil, fmt.Errorf("receiver %q is not configured", set.ID)
	}

	recvFact, existsFactory := b.factories[set.ID.Type()]
	if !existsFactory {
		return nil, fmt.Errorf("receiver factory not available for: %q", set.ID)
	}

	f, ok := recvFact.(xreceiver.Factory)
	if !ok {
		return nil, pipeline.ErrSignalNotSupported
	}

	logDeprecatedTypeAlias(set.Logger, f, set.ID.Type())
	logStabilityLevel(set.Logger, f.ProfilesStability())
	return f.CreateProfiles(ctx, set, cfg, next)
}

func (b *ReceiverBuilder) Factory(componentType component.Type) component.Factory {
	return b.factories[componentType]
}

// NewNopReceiverConfigsAndFactories returns a configuration and factories that allows building a new nop receiver.
func NewNopReceiverConfigsAndFactories() (map[component.ID]component.Config, map[component.Type]receiver.Factory) {
	nopFactory := receivertest.NewNopFactory()
	configs := map[component.ID]component.Config{
		component.NewID(NopType): nopFactory.CreateDefaultConfig(),
	}
	factories := map[component.Type]receiver.Factory{
		NopType: nopFactory,
	}

	return configs, factories
}
