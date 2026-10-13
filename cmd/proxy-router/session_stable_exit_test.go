package main

import (
	"strings"
	"testing"
)

func TestParseOptionsSessionStableExitOptIn(t *testing.T) {
	opts, err := parseOptions(nil, fakeEnv(credentialEnv()))
	if err != nil {
		t.Fatal(err)
	}
	if opts.SessionStableExit {
		t.Fatal("session stable-exit must default to false")
	}
	env := credentialEnv()
	env["GOST_SESSION_STABLE_EXIT"] = "true"
	opts, err = parseOptions(nil, fakeEnv(env))
	if err != nil || !opts.SessionStableExit {
		t.Fatalf("environment opt-in=%t err=%v", opts.SessionStableExit, err)
	}
	opts, err = parseOptions([]string{"-session-stable-exit"}, fakeEnv(credentialEnv()))
	if err != nil || !opts.SessionStableExit {
		t.Fatalf("flag opt-in=%t err=%v", opts.SessionStableExit, err)
	}
	if opts, err = parseOptions([]string{"-session-stable-exit=false"}, fakeEnv(env)); err != nil || opts.SessionStableExit {
		t.Fatalf("flag should override environment: option=%t err=%v", opts.SessionStableExit, err)
	}
	env["GOST_SESSION_STABLE_EXIT"] = "sometimes"
	if _, err := parseOptions(nil, fakeEnv(env)); err == nil || !strings.Contains(err.Error(), "GOST_SESSION_STABLE_EXIT") {
		t.Fatalf("malformed environment value error=%v", err)
	}
}
