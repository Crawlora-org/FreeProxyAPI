package monitor

import (
	"strings"
	"testing"
	"time"
)

func probeExtrasConfig(extra string) string {
	return strings.Replace(validConfig, `"network_validation_enabled": false`, `"network_validation_enabled": false,`+extra, 1)
}

func TestLoadConfigProbeExtrasDefaults(t *testing.T) {
	config, err := LoadConfig(writeConfig(t, validConfig))
	if err != nil {
		t.Fatal(err)
	}
	if config.HTTPSProbeTarget != "" || config.HTTPSProbeEvery != 4 {
		t.Errorf("https defaults: target=%q every=%d", config.HTTPSProbeTarget, config.HTTPSProbeEvery)
	}
	if !config.ControlProbeEnabled || config.ControlProbeInterval != time.Minute || config.ControlProbeFailureThreshold != 3 {
		t.Errorf("control defaults: %+v", config)
	}
	if config.ClassificationMaxAge != 24*time.Hour {
		t.Errorf("classification_max_age = %s", config.ClassificationMaxAge)
	}
}

func TestLoadConfigProbeExtrasExplicit(t *testing.T) {
	config, err := LoadConfig(writeConfig(t, probeExtrasConfig(`
  "https_probe_target": "https://check.example.net/ok.txt",
  "https_probe_expected_body": "ok\n",
  "https_probe_every": 10,
  "control_probe_enabled": false,
  "control_probe_interval": "30s",
  "control_probe_failure_threshold": 5,
  "classification_max_age": "6h"`)))
	if err != nil {
		t.Fatal(err)
	}
	if config.HTTPSProbeTarget != "https://check.example.net/ok.txt" || config.HTTPSProbeExpectedBody != "ok" || config.HTTPSProbeEvery != 10 {
		t.Errorf("https: %+v", config)
	}
	if config.ControlProbeEnabled || config.ControlProbeInterval != 30*time.Second || config.ControlProbeFailureThreshold != 5 {
		t.Errorf("control: %+v", config)
	}
	if config.ClassificationMaxAge != 6*time.Hour {
		t.Errorf("classification_max_age = %s", config.ClassificationMaxAge)
	}
}

func TestLoadConfigProbeExtrasValidation(t *testing.T) {
	for name, extra := range map[string]string{
		"plain http target":   `"https_probe_target": "http://check.example.net/"`,
		"credentialed target": `"https_probe_target": "https://u:p@check.example.net/"`,
		"negative every":      `"https_probe_every": -1`,
		"huge every":          `"https_probe_every": 1001`,
		"short interval":      `"control_probe_interval": "500ms"`,
		"bad interval":        `"control_probe_interval": "soon"`,
		"threshold too high":  `"control_probe_failure_threshold": 101`,
		"negative threshold":  `"control_probe_failure_threshold": -1`,
		"bad max age":         `"classification_max_age": "-1h"`,
	} {
		if _, err := LoadConfig(writeConfig(t, probeExtrasConfig(extra))); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
