package monitor

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

// adminTestRunner returns a runner backed by an in-memory Redis so every
// handler can run.
func adminTestRunner(t *testing.T, config Config) *Runner {
	t.Helper()
	mini := miniredis.RunT(t)
	redisStore, err := store.NewRedis("redis://"+mini.Addr()+"/0", "admin-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { redisStore.Close() })
	return &Runner{store: redisStore, config: config, metrics: newMetrics()}
}

func serveOn(handler http.Handler, path string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
	return response
}

func TestAdminListenerMovesOperatorEndpoints(t *testing.T) {
	health, err := startHealthServer("127.0.0.1:0", adminTestRunner(t, Config{AdminListenAddr: "127.0.0.1:0"}))
	if err != nil {
		t.Fatal(err)
	}
	defer health.Shutdown()
	if health.admin == nil || health.AdminAddr() == "" {
		t.Fatal("admin listener was not started")
	}
	for _, path := range []string{"/metrics", "/report", "/internal/api/v1/proxies"} {
		if got := serveOn(health.server.Handler, path).Code; got != http.StatusNotFound {
			t.Errorf("public %s = %d, want 404 when admin_listen_addr is set", path, got)
		}
	}
	for _, path := range []string{"/stats", "/proxies", "/livez", "/readyz", "/", "/dashboard"} {
		if got := serveOn(health.server.Handler, path).Code; got == http.StatusNotFound {
			t.Errorf("public %s disappeared from the public listener", path)
		}
	}
	if body := serveOn(health.admin.Handler, "/metrics"); body.Code != http.StatusOK || !strings.Contains(body.Body.String(), "freeproxyapi_") {
		t.Errorf("admin /metrics = %d", body.Code)
	}
	if got := serveOn(health.admin.Handler, "/report").Code; got == http.StatusNotFound {
		t.Error("admin /report missing")
	}
	if got := serveOn(health.admin.Handler, "/livez").Code; got != http.StatusOK {
		t.Errorf("admin /livez = %d", got)
	}
	// The admin listener must not serve the public API or pages.
	for _, path := range []string{"/proxies", "/stats", "/get", "/"} {
		if got := serveOn(health.admin.Handler, path).Code; got != http.StatusNotFound {
			t.Errorf("admin %s = %d, want 404", path, got)
		}
	}
	// A real request reaches the admin socket.
	resp, err := http.Get("http://" + health.AdminAddr() + "/livez")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("admin socket livez: %v %v", resp, err)
	}
	resp.Body.Close()
}

func TestWithoutAdminListenerOperatorEndpointsStayOnPublicListener(t *testing.T) {
	health, err := startHealthServer("127.0.0.1:0", adminTestRunner(t, Config{}))
	if err != nil {
		t.Fatal(err)
	}
	defer health.Shutdown()
	if health.admin != nil || health.AdminAddr() != "" {
		t.Fatal("admin listener started without admin_listen_addr")
	}
	for _, path := range []string{"/metrics", "/report"} {
		if got := serveOn(health.server.Handler, path).Code; got == http.StatusNotFound {
			t.Errorf("%s moved off the public listener without admin_listen_addr", path)
		}
	}
}

func TestAdminListenerBindFailureStopsStartup(t *testing.T) {
	first, err := startHealthServer("127.0.0.1:0", adminTestRunner(t, Config{AdminListenAddr: "127.0.0.1:0"}))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Shutdown()
	if _, err := startHealthServer("127.0.0.1:0", adminTestRunner(t, Config{AdminListenAddr: first.AdminAddr()})); err == nil {
		t.Fatal("startup succeeded although the admin address was already in use")
	}
}

func TestLoadConfigAdminListenAddr(t *testing.T) {
	config, err := LoadConfig(writeConfig(t, validConfig))
	if err != nil || config.AdminListenAddr != "" {
		t.Fatalf("default admin addr = %q, %v; want empty", config.AdminListenAddr, err)
	}
	body := `{"sources":["file:///data/proxies.txt"],"listen_addr":":8080","admin_listen_addr":" 127.0.0.1:9090 "}`
	config, err = LoadConfig(writeConfig(t, body))
	if err != nil || config.AdminListenAddr != "127.0.0.1:9090" {
		t.Fatalf("admin addr = %q, %v", config.AdminListenAddr, err)
	}
	t.Setenv("FREEPROXYAPI_ADMIN_LISTEN_ADDR", "127.0.0.1:9191")
	if config, err = LoadConfig(writeConfig(t, body)); err != nil || config.AdminListenAddr != "127.0.0.1:9191" {
		t.Fatalf("env admin addr = %q, %v", config.AdminListenAddr, err)
	}
	t.Setenv("FREEPROXYAPI_ADMIN_LISTEN_ADDR", "")
	for _, bad := range []string{"9090", "localhost", ":8080"} {
		cfg := `{"sources":["file:///data/proxies.txt"],"listen_addr":":8080","admin_listen_addr":"` + bad + `"}`
		if _, err := LoadConfig(writeConfig(t, cfg)); err == nil {
			t.Errorf("admin_listen_addr %q accepted", bad)
		}
	}
	if _, err := LoadConfig(writeConfig(t, `{"sources":["file:///data/proxies.txt"],"listen_addr":"-","admin_listen_addr":"127.0.0.1:9090"}`)); err == nil {
		t.Error("admin_listen_addr accepted with the public listener disabled")
	}
}

func TestExampleConfigsLoad(t *testing.T) {
	config, err := LoadConfig("../../examples/monitor.json")
	if err != nil {
		t.Fatal(err)
	}
	if config.AdminListenAddr != "127.0.0.1:9090" {
		t.Errorf("example admin_listen_addr = %q, want loopback", config.AdminListenAddr)
	}
}
