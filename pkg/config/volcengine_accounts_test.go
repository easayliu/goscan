package config

import (
	"strings"
	"testing"
)

func configWithVolcAccounts(t *testing.T) *Config {
	t.Helper()
	cfg := configWithJob("volcengine")
	cfg.Scheduler.Jobs = append(cfg.Scheduler.Jobs, ScheduledJob{
		Name: "avatar_daily_sync", Provider: "volcengine:avatar", Cron: "0 2 * * *",
		Config: JobConfig{SyncMode: "sync-optimal"},
	})
	ve := cfg.CloudProviders.VolcEngine
	ve.AccessKey, ve.SecretKey, ve.PayerID = "ak", "sk", "2100041849"
	ve.Accounts = []VolcEngineAccount{{Name: "avatar", AccessKey: "ak2", SecretKey: "sk2", PayerID: "2134017190"}}
	return cfg
}

func TestVolcEngineAccountsValid(t *testing.T) {
	if err := configWithVolcAccounts(t).ValidateConfig(); err != nil {
		t.Fatalf("valid multi-account config rejected: %v", err)
	}
}

// With accounts in the shared table, an account without a PayerID would clear
// whole months, other accounts' rows included.
func TestVolcEngineAccountsRequirePayerIDs(t *testing.T) {
	cfg := configWithVolcAccounts(t)
	cfg.CloudProviders.VolcEngine.PayerID = ""
	if err := cfg.ValidateConfig(); err == nil || !strings.Contains(err.Error(), "payer_id") {
		t.Errorf("main account without payer_id: err = %v", err)
	}

	cfg = configWithVolcAccounts(t)
	cfg.CloudProviders.VolcEngine.Accounts[0].PayerID = "2100041849"
	if err := cfg.ValidateConfig(); err == nil {
		t.Error("two accounts with the same payer_id passed validation")
	}

	cfg = configWithVolcAccounts(t)
	cfg.CloudProviders.VolcEngine.Accounts[0].PayerID = "1 OR 1=1"
	if err := cfg.ValidateConfig(); err == nil {
		t.Error("non-numeric payer_id passed validation")
	}
}

func TestVolcEngineJobForUnknownAccountRejected(t *testing.T) {
	cfg := configWithVolcAccounts(t)
	cfg.Scheduler.Jobs[1].Provider = "volcengine:nobody"
	if err := cfg.ValidateConfig(); err == nil || !strings.Contains(err.Error(), "nobody") {
		t.Errorf("job for an unconfigured account: err = %v", err)
	}
}

func TestVolcEngineAccountCredentialsFromEnv(t *testing.T) {
	t.Setenv("VOLCENGINE_AVATAR_ACCESS_KEY", "env-ak")
	t.Setenv("VOLCENGINE_AVATAR_SECRET_KEY", "env-sk")
	cfg := configWithVolcAccounts(t)
	a := &cfg.CloudProviders.VolcEngine.Accounts[0]
	a.AccessKey, a.SecretKey = "", ""
	mergeVolcEngineEnvVars(cfg)
	if a.AccessKey != "env-ak" || a.SecretKey != "env-sk" {
		t.Errorf("account credentials = %q/%q, want them from the environment", a.AccessKey, a.SecretKey)
	}
}

func TestForAccountKeepsSharedSettings(t *testing.T) {
	ve := configWithVolcAccounts(t).CloudProviders.VolcEngine
	ve.BillTable = "volcengine_bill"
	got, err := ve.ForAccount("avatar")
	if err != nil {
		t.Fatal(err)
	}
	if got.AccessKey != "ak2" || got.PayerID != "2134017190" || got.BillTable != "volcengine_bill" || len(got.Accounts) != 0 {
		t.Errorf("ForAccount = %+v", got)
	}
	if ve.AccessKey != "ak" || ve.PayerID != "2100041849" {
		t.Error("ForAccount changed the main account's settings")
	}
	if _, err := ve.ForAccount("nobody"); err == nil {
		t.Error("ForAccount of an unknown account succeeded")
	}
}
