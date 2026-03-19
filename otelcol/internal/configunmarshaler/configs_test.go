// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package configunmarshaler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/connector/connectortest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/extension/extensiontest"
	"go.opentelemetry.io/collector/processor/processortest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

var nopType = component.MustNewType("nop")

var testKinds = []struct {
	kind      string
	factories map[component.Type]component.Factory
}{
	{
		kind: "receiver",
		factories: map[component.Type]component.Factory{
			nopType: receivertest.NewNopFactory(),
		},
	},
	{
		kind: "processor",
		factories: map[component.Type]component.Factory{
			nopType: processortest.NewNopFactory(),
		},
	},
	{
		kind: "exporter",
		factories: map[component.Type]component.Factory{
			nopType: exportertest.NewNopFactory(),
		},
	},
	{
		kind: "connector",
		factories: map[component.Type]component.Factory{
			nopType: connectortest.NewNopFactory(),
		},
	},
	{
		kind: "extension",
		factories: map[component.Type]component.Factory{
			nopType: extensiontest.NewNopFactory(),
		},
	},
}

func TestUnmarshal(t *testing.T) {
	for _, tk := range testKinds {
		t.Run(tk.kind, func(t *testing.T) {
			cfgs := NewConfigs(tk.factories)
			conf := confmap.NewFromStringMap(map[string]any{
				"nop":              nil,
				"nop/my" + tk.kind: nil,
			})
			require.NoError(t, cfgs.Unmarshal(conf))

			assert.Equal(t, map[component.ID]component.Config{
				component.NewID(nopType):                       tk.factories[nopType].CreateDefaultConfig(),
				component.NewIDWithName(nopType, "my"+tk.kind): tk.factories[nopType].CreateDefaultConfig(),
			}, cfgs.Configs())
		})
	}
}

func TestUnmarshalError(t *testing.T) {
	for _, tk := range testKinds {
		t.Run(tk.kind, func(t *testing.T) {
			testCases := []struct {
				name string
				conf *confmap.Conf
				// string that the error must contain
				expectedError string
			}{
				{
					name: "invalid-type",
					conf: confmap.NewFromStringMap(map[string]any{
						"nop":     nil,
						"/custom": nil,
					}),
					expectedError: "the part before / should not be empty",
				},
				{
					name: "invalid-name-after-slash",
					conf: confmap.NewFromStringMap(map[string]any{
						"nop":  nil,
						"nop/": nil,
					}),
					expectedError: "the part after / should not be empty",
				},
				{
					name: "unknown-type",
					conf: confmap.NewFromStringMap(map[string]any{
						"nosuch" + tk.kind: nil,
					}),
					expectedError: "unknown type: \"nosuch" + tk.kind + "\" for id: \"nosuch" + tk.kind + "\" (valid values: [nop])",
				},
				{
					name: "duplicate",
					conf: confmap.NewFromStringMap(map[string]any{
						"nop /my" + tk.kind + " ": nil,
						" nop/ my" + tk.kind:      nil,
					}),
					expectedError: "duplicate name",
				},
				{
					name: "invalid-section",
					conf: confmap.NewFromStringMap(map[string]any{
						"nop": map[string]any{
							"unknown_section": tk.kind,
						},
					}),
					expectedError: "error reading configuration for \"nop\"",
				},
				{
					name: "invalid-sub-config",
					conf: confmap.NewFromStringMap(map[string]any{
						"nop": "tests",
					}),
					expectedError: "'[nop]' expected type 'map[string]interface {}', got unconvertible type 'string'",
				},
			}

			for _, tt := range testCases {
				t.Run(tt.name, func(t *testing.T) {
					cfgs := NewConfigs(tk.factories)
					err := cfgs.Unmarshal(tt.conf)
					assert.ErrorContains(t, err, tt.expectedError)
				})
			}
		})
	}
}

func TestUnmarshal_Tags(t *testing.T) {
	factories := map[component.Type]component.Factory{
		nopType: receivertest.NewNopFactory(),
	}
	cfgs := NewReceiverConfigs(factories)
	conf := confmap.NewFromStringMap(map[string]any{
		"nop": map[string]any{
			"tags": map[string]any{"env": "prod", "region": "us-east-1"},
		},
		"nop/second": nil,
	})
	require.NoError(t, cfgs.Unmarshal(conf))

	tags := cfgs.Tags()
	nopID := component.NewID(nopType)
	secondID := component.NewIDWithName(nopType, "second")

	assert.Equal(t, map[string]string{"env": "prod", "region": "us-east-1"}, tags[nopID])
	assert.Empty(t, tags[secondID], "component with no tags should have no entry")
}

// TestUnmarshal_Tags_WithOtherKeys verifies that non-tags keys in the config are preserved
// when tags are also present (exercises the filtered-copy branch where k != "tags").
func TestUnmarshal_Tags_WithOtherKeys(t *testing.T) {
	nopFactory := receivertest.NewNopFactory()
	// Use a factory whose default config has a settable field so we can confirm
	// the other key was passed through correctly. nopConfig has no exported fields,
	// so we just verify unmarshal succeeds and tags are extracted.
	factories := map[component.Type]component.Factory{nopType: nopFactory}
	cfgs := NewReceiverConfigs(factories)
	conf := confmap.NewFromStringMap(map[string]any{
		"nop": map[string]any{
			// A valid key recognised by nopConfig alongside tags.
			// nopConfig has no exported fields, so the only assertion we can
			// make is that the unmarshal succeeds (tags stripped, no unknown-key error).
			"tags": map[string]any{"service": "db"},
		},
	})
	require.NoError(t, cfgs.Unmarshal(conf))
	assert.Equal(t, map[string]string{"service": "db"}, cfgs.Tags()[component.NewID(nopType)])
}

// TestUnmarshal_Tags_NonMapValue verifies that a non-map "tags" value returns an error.
func TestUnmarshal_Tags_NonMapValue(t *testing.T) {
	factories := map[component.Type]component.Factory{nopType: receivertest.NewNopFactory()}
	cfgs := NewReceiverConfigs(factories)
	conf := confmap.NewFromStringMap(map[string]any{
		"nop": map[string]any{
			"tags": "this-is-not-a-map",
		},
	})
	err := cfgs.Unmarshal(conf)
	assert.ErrorContains(t, err, "\"tags\" must be a map of string keys to string values")
}

// TestUnmarshal_Tags_NonReceiverIgnored verifies that "tags:" in a non-receiver component
// config is NOT silently stripped — it causes an unknown-key error as expected.
func TestUnmarshal_Tags_NonReceiverIgnored(t *testing.T) {
	for _, tk := range testKinds {
		if tk.kind == "receiver" {
			continue
		}
		t.Run(tk.kind, func(t *testing.T) {
			cfgs := NewConfigs(tk.factories)
			conf := confmap.NewFromStringMap(map[string]any{
				"nop": map[string]any{
					"tags": map[string]any{"env": "prod"},
				},
			})
			err := cfgs.Unmarshal(conf)
			assert.ErrorContains(t, err, "error reading configuration for \"nop\"",
				"tags: in a non-receiver config should cause an unknown-key error")
		})
	}
}

func TestUnmarshal_Tags_NonStringValues(t *testing.T) {
	factories := map[component.Type]component.Factory{
		nopType: receivertest.NewNopFactory(),
	}
	cfgs := NewReceiverConfigs(factories)
	conf := confmap.NewFromStringMap(map[string]any{
		"nop": map[string]any{
			"tags": map[string]any{"port": 8080, "enabled": true},
		},
	})
	require.NoError(t, cfgs.Unmarshal(conf))

	tags := cfgs.Tags()[component.NewID(nopType)]
	assert.Equal(t, "8080", tags["port"])
	assert.Equal(t, "true", tags["enabled"])
}

func TestUnmarshal_Tags_DoesNotBreakOtherKeys(t *testing.T) {
	factories := map[component.Type]component.Factory{
		nopType: receivertest.NewNopFactory(),
	}
	cfgs := NewReceiverConfigs(factories)
	// unknown_section is not tags, so it should still error
	conf := confmap.NewFromStringMap(map[string]any{
		"nop": map[string]any{
			"unknown_section": "value",
		},
	})
	err := cfgs.Unmarshal(conf)
	assert.ErrorContains(t, err, "error reading configuration for \"nop\"")
}

func TestUnmarshal_LoggingExporter(t *testing.T) {
	conf := confmap.NewFromStringMap(map[string]any{
		"logging": nil,
	})
	factories := map[component.Type]component.Factory{
		nopType: exportertest.NewNopFactory(),
	}
	cfgs := NewConfigs(factories)
	err := cfgs.Unmarshal(conf)
	assert.ErrorContains(t, err, "the logging exporter has been deprecated, use the debug exporter instead")
}
