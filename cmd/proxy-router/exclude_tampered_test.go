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
