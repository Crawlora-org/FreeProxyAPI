package main

import "testing"

func TestParseOptionsExcludeTamperedDefaultsOn(t *testing.T) {
	opts, err := parseOptions(nil, fakeEnv(credentialEnv()))
	if err != nil {
		t.Fatal(err)
	}
	if !opts.ExcludeTampered {
		t.Fatal("exclude tampered must default to true")
	}
	env := credentialEnv()
	env["FREEPROXYAPI_EXCLUDE_TAMPERED"] = "false"
	if opts, err = parseOptions(nil, fakeEnv(env)); err != nil || opts.ExcludeTampered {
		t.Fatalf("env disable: opts=%+v err=%v", opts, err)
	}
	if opts, err = parseOptions([]string{"-exclude-tampered=false"}, fakeEnv(credentialEnv())); err != nil || opts.ExcludeTampered {
		t.Fatalf("flag disable: opts=%+v err=%v", opts, err)
	}
}

func TestParseOptionsStrictTamperAndHTTPSDefaultsOff(t *testing.T) {
	opts, err := parseOptions(nil, fakeEnv(credentialEnv()))
	if err != nil {
		t.Fatal(err)
	}
	if opts.RequireTamperChecked || opts.HTTPS {
		t.Fatalf("strict clean and HTTPS defaults must be false: %+v", opts)
	}
	env := credentialEnv()
	env["FREEPROXYAPI_REQUIRE_TAMPER_CHECKED"] = "true"
	env["FREEPROXYAPI_HTTPS"] = "true"
	if opts, err = parseOptions(nil, fakeEnv(env)); err != nil || !opts.RequireTamperChecked || !opts.HTTPS {
		t.Fatalf("environment enable: opts=%+v err=%v", opts, err)
	}
	if opts, err = parseOptions([]string{"-require-tamper-checked", "-https"}, fakeEnv(credentialEnv())); err != nil || !opts.RequireTamperChecked || !opts.HTTPS {
		t.Fatalf("flag enable: opts=%+v err=%v", opts, err)
	}
	if opts, err = parseOptions([]string{"-require-tamper-checked=false", "-https=false"}, fakeEnv(env)); err != nil || opts.RequireTamperChecked || opts.HTTPS {
		t.Fatalf("flags override environment: opts=%+v err=%v", opts, err)
	}
}
