package main

import (
	"testing"
)

func TestParseOptionsQualityFiltersDefaultsAndParsesEnvAndFlags(t *testing.T) {
	opts, err := parseOptions(nil, fakeEnv(credentialEnv()))
	if err != nil {
		t.Fatal(err)
	}
	if opts.ExitASN != "" || opts.ExcludeHosting || opts.StableExit || opts.MinSuccessStreak != 0 || opts.MinReliability7dPct != 0 || opts.MinReliability7dSamples != 0 || opts.TargetProfile != "" {
		t.Fatalf("quality filter defaults=%+v", opts)
	}
	env := credentialEnv()
	env["FREEPROXYAPI_EXIT_ASN"] = "AS64500"
	env["FREEPROXYAPI_EXCLUDE_HOSTING"] = "true"
	env["FREEPROXYAPI_STABLE_EXIT"] = "true"
	env["FREEPROXYAPI_MIN_SUCCESS_STREAK"] = "4"
	env["FREEPROXYAPI_MIN_RELIABILITY_7D_PCT"] = "95"
	env["FREEPROXYAPI_MIN_RELIABILITY_7D_SAMPLES"] = "20"
	env["FREEPROXYAPI_TARGET_PROFILE"] = "residential"
	opts, err = parseOptions(nil, fakeEnv(env))
	if err != nil || opts.ExitASN != "AS64500" || !opts.ExcludeHosting || !opts.StableExit || opts.MinSuccessStreak != 4 || opts.MinReliability7dPct != 95 || opts.MinReliability7dSamples != 20 || opts.TargetProfile != "residential" {
		t.Fatalf("quality filter env=%+v err=%v", opts, err)
	}
	opts, err = parseOptions([]string{
		"-exit-asn", "AS64501", "-exclude-hosting", "-stable-exit", "-min-success-streak", "6",
		"-min-reliability-7d-pct", "90", "-min-reliability-7d-samples", "12", "-target-profile", "mobile",
	}, fakeEnv(credentialEnv()))
	if err != nil || opts.ExitASN != "AS64501" || !opts.ExcludeHosting || !opts.StableExit || opts.MinSuccessStreak != 6 || opts.MinReliability7dPct != 90 || opts.MinReliability7dSamples != 12 || opts.TargetProfile != "mobile" {
		t.Fatalf("quality filter flags=%+v err=%v", opts, err)
	}
}

func TestParseOptionsRejectsOutOfRangeQualityFilters(t *testing.T) {
	for _, args := range [][]string{
		{"-min-success-streak", "-1"},
		{"-min-reliability-7d-pct", "101"},
		{"-min-reliability-7d-samples", "-1"},
	} {
		if _, err := parseOptions(args, fakeEnv(credentialEnv())); err == nil {
			t.Errorf("parseOptions(%v) accepted out-of-range value", args)
		}
	}
}
