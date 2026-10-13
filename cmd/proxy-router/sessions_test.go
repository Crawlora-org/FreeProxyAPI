package main

import (
	"strings"
	"testing"
	"time"
)

func TestParseOptionsNamedSessionsDefaultsAndOverrides(t *testing.T) {
	opts, err := parseOptions(nil, fakeEnv(credentialEnv()))
	if err != nil {
		t.Fatal(err)
	}
	if len(opts.Sessions) != 0 || opts.SessionTTL != 10*time.Minute || opts.SessionBasePort != 10000 || opts.SessionFailover {
		t.Fatalf("session defaults=%+v", opts)
	}
	env := credentialEnv()
	env["GOST_SESSIONS"] = "alpha,beta"
	env["GOST_SESSION_TTL"] = "5m"
	env["GOST_SESSION_BASE_PORT"] = "12000"
	env["GOST_SESSION_FAILOVER"] = "true"
	opts, err = parseOptions(nil, fakeEnv(env))
	if err != nil || strings.Join(opts.Sessions, ",") != "alpha,beta" || opts.SessionTTL != 5*time.Minute || opts.SessionBasePort != 12000 || !opts.SessionFailover {
		t.Fatalf("session environment=%+v err=%v", opts, err)
	}
	opts, err = parseOptions([]string{"-sessions", "gamma,delta", "-session-ttl", "2m", "-session-base-port", "13000", "-session-failover"}, fakeEnv(credentialEnv()))
	if err != nil || strings.Join(opts.Sessions, ",") != "gamma,delta" || opts.SessionTTL != 2*time.Minute || opts.SessionBasePort != 13000 || !opts.SessionFailover {
		t.Fatalf("session flags=%+v err=%v", opts, err)
	}
}

func TestParseOptionsRejectsInvalidNamedSessions(t *testing.T) {
	for _, args := range [][]string{
		{"-sessions", "bad.id"},
		{"-sessions", "same,same"},
		{"-sessions", "one", "-session-ttl", "0s"},
		{"-sessions", "one", "-session-base-port", "65536"},
	} {
		if _, err := parseOptions(args, fakeEnv(credentialEnv())); err == nil {
			t.Errorf("parseOptions(%v) accepted invalid session settings", args)
		}
	}
	env := credentialEnv()
	env["GOST_API_ADDRESS"] = "127.0.0.1:18080"
	if _, err := parseOptions([]string{"-sessions", "alpha", "-session-base-port", "18080"}, fakeEnv(env)); err == nil || !strings.Contains(err.Error(), "GOST API port") {
		t.Fatalf("session listener colliding with GOST API accepted: err=%v", err)
	}
}
