package config

import (
	"errors"
	"strings"
	"testing"
)

func configWithJob(provider string) *Config {
	cfg := getDefaultConfig()
	cfg.ClickHouse.Hosts = []string{"localhost"}
	cfg.Scheduler = &SchedulerConfig{
		Enabled: true,
		Jobs: []ScheduledJob{{
			Name:     provider + "_daily_sync",
			Provider: provider,
			Cron:     "0 2 * * *",
			Config:   JobConfig{SyncMode: "sync-optimal"},
		}},
	}
	return cfg
}

// A Secret that was never filled in looks exactly like "this cloud isn't
// connected", so the credential check alone lets it through. A scheduled job
// naming the provider is the missing signal that the keys were meant to be
// there — without this the Pod starts green and nothing syncs until someone
// notices the empty tables.
func TestValidateRejectsJobWhoseProviderHasNoCredentials(t *testing.T) {
	cfg := configWithJob("volcengine")

	err := cfg.ValidateConfig()
	if err == nil {
		t.Fatal("a volcengine job with no credentials passed validation")
	}
	if !errors.Is(err, ErrVolcEngineConfig) {
		t.Errorf("error = %v, want it to wrap ErrVolcEngineConfig", err)
	}
	// The message has to say which job, otherwise a config with a dozen jobs
	// gives no clue where to look.
	if !strings.Contains(err.Error(), "volcengine_daily_sync") {
		t.Errorf("error does not name the job: %v", err)
	}
}

func TestValidateRejectsAliCloudJobWithHalfTheCredentials(t *testing.T) {
	cfg := configWithJob("alicloud")
	cfg.CloudProviders.AliCloud.AccessKeyID = "id-only"

	if err := cfg.ValidateConfig(); !errors.Is(err, ErrAliCloudConfig) {
		t.Fatalf("error = %v, want it to wrap ErrAliCloudConfig", err)
	}
}

func TestValidateAcceptsJobWithCredentials(t *testing.T) {
	cfg := configWithJob("volcengine")
	cfg.CloudProviders.VolcEngine.AccessKey = "key"
	cfg.CloudProviders.VolcEngine.SecretKey = "secret"

	if err := cfg.ValidateConfig(); err != nil {
		t.Fatalf("a fully configured job was rejected: %v", err)
	}
}

// Nothing is scheduled, so nothing needs credentials — this is the config of a
// deployment that only ever runs `goscan --once` by hand.
func TestValidateSkipsCredentialCheckWhenSchedulerIsOff(t *testing.T) {
	cfg := configWithJob("volcengine")
	cfg.Scheduler.Enabled = false

	if err := cfg.ValidateConfig(); err != nil {
		t.Fatalf("a disabled scheduler still demanded credentials: %v", err)
	}
}

// A provider block left empty is "this cloud isn't connected", not an error;
// the sample config and the deployment ConfigMap both list all five clouds and
// fill in one or two.
func TestValidateSkipsProvidersWithoutCredentials(t *testing.T) {
	cfg := getDefaultConfig()
	cfg.ClickHouse.Hosts = []string{"localhost"}

	if err := cfg.ValidateConfig(); err != nil {
		t.Fatalf("empty provider blocks were treated as errors: %v", err)
	}
}

// The old values never worked at runtime, but failing them here would send an
// upgraded Pod into CrashLoopBackOff on a config that used to start.
func TestValidateToleratesLegacySyncModes(t *testing.T) {
	for _, mode := range LegacySyncModes {
		cfg := configWithJob("volcengine")
		cfg.CloudProviders.VolcEngine.AccessKey = "key"
		cfg.CloudProviders.VolcEngine.SecretKey = "secret"
		cfg.Scheduler.Jobs[0].Config.SyncMode = mode

		if err := cfg.ValidateConfig(); err != nil {
			t.Errorf("sync_mode %q was rejected: %v", mode, err)
		}
		if got, normalized := NormalizeSyncMode(mode); got != "standard" || !normalized {
			t.Errorf("NormalizeSyncMode(%q) = (%q, %v), want (\"standard\", true)", mode, got, normalized)
		}
	}
}

func TestValidateRejectsAnUnknownSyncMode(t *testing.T) {
	cfg := configWithJob("volcengine")
	cfg.CloudProviders.VolcEngine.AccessKey = "key"
	cfg.CloudProviders.VolcEngine.SecretKey = "secret"
	cfg.Scheduler.Jobs[0].Config.SyncMode = "whatever"

	if err := cfg.ValidateConfig(); err == nil {
		t.Fatal("an unknown sync_mode passed validation")
	}
}

// The modes the executor actually accepts must not be normalised away.
func TestNormalizeSyncModeLeavesCurrentModesAlone(t *testing.T) {
	for _, mode := range []string{"standard", "sync-optimal", "cost_report", ""} {
		if got, normalized := NormalizeSyncMode(mode); got != mode || normalized {
			t.Errorf("NormalizeSyncMode(%q) = (%q, %v), want it unchanged", mode, got, normalized)
		}
	}
}
