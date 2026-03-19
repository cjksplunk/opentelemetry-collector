// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package configunmarshaler // import "go.opentelemetry.io/collector/otelcol/internal/configunmarshaler"

import (
	"errors"
	"fmt"

	"golang.org/x/exp/maps"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
)

type Configs[F component.Factory] struct {
	cfgs map[component.ID]component.Config
	tags map[component.ID]map[string]string
	// extractTags enables extraction of the "tags:" key from raw component configs.
	// Only set for receiver configs; other component types reject "tags:" as unknown.
	extractTags bool

	factories map[component.Type]F
}

func NewConfigs[F component.Factory](factories map[component.Type]F) *Configs[F] {
	return &Configs[F]{factories: factories}
}

// NewReceiverConfigs creates a Configs instance for receivers with tags extraction enabled.
// The "tags:" key is stripped from each receiver's raw config before factory unmarshal
// and made available via Tags().
func NewReceiverConfigs[F component.Factory](factories map[component.Type]F) *Configs[F] {
	return &Configs[F]{factories: factories, extractTags: true}
}

func (c *Configs[F]) Unmarshal(conf *confmap.Conf) error {
	rawCfgs := make(map[component.ID]map[string]any)
	if err := conf.Unmarshal(&rawCfgs); err != nil {
		return err
	}

	// Prepare resulting maps.
	c.cfgs = make(map[component.ID]component.Config)
	c.tags = make(map[component.ID]map[string]string)
	// Iterate over raw configs and create a config for each.
	for id, rawCfg := range rawCfgs {
		hasTags := false
		if c.extractTags {
			if t, ok := rawCfg["tags"]; ok {
				tagMap, ok := t.(map[string]any)
				if !ok {
					return fmt.Errorf("error reading configuration for %q: \"tags\" must be a map of string keys to string values, got %T", id, t)
				}
				strMap := make(map[string]string, len(tagMap))
				for k, v := range tagMap {
					strMap[k] = fmt.Sprint(v)
				}
				c.tags[id] = strMap
				// Build a filtered copy of rawCfg excluding "tags" so the factory
				// unmarshal does not see it as an unknown key.
				hasTags = true
			}
		}
		// Find factory based on component kind and type that we read from config source.
		factory, ok := c.factories[id.Type()]
		if !ok {
			return errorUnknownType(id, maps.Keys(c.factories))
		}

		// Get the configuration from the confmap.Conf to preserve internal representation.
		// When tags were present, build a filtered sub-conf from the raw map (minus "tags")
		// so the factory unmarshal does not reject it as an unknown key.
		// When no tags are present, use conf.Sub which is a lightweight slice into the
		// already-parsed tree and avoids an extra allocation.
		var sub *confmap.Conf
		if hasTags {
			filtered := make(map[string]any, len(rawCfg))
			for k, v := range rawCfg {
				if k != "tags" {
					filtered[k] = v
				}
			}
			sub = confmap.NewFromStringMap(filtered)
		} else {
			var err error
			sub, err = conf.Sub(id.String())
			if err != nil {
				return errorUnmarshalError(id, err)
			}
		}

		// Create the default config for this component.
		cfg := factory.CreateDefaultConfig()

		// Now that the default config struct is created we can Unmarshal into it,
		// and it will apply user-defined config on top of the default.
		if err := sub.Unmarshal(&cfg); err != nil {
			return errorUnmarshalError(id, err)
		}

		c.cfgs[id] = cfg
	}

	return nil
}

func (c *Configs[F]) Configs() map[component.ID]component.Config {
	return c.cfgs
}

func (c *Configs[F]) Tags() map[component.ID]map[string]string {
	return c.tags
}

func errorUnknownType(id component.ID, factories []component.Type) error {
	if id.Type().String() == "logging" {
		return errors.New("the logging exporter has been deprecated, use the debug exporter instead")
	}
	return fmt.Errorf("unknown type: %q for id: %q (valid values: %v)", id.Type(), id, factories)
}

func errorUnmarshalError(id component.ID, err error) error {
	return fmt.Errorf("error reading configuration for %q: %w", id, err)
}
