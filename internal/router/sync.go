package router

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

type SyncOptions struct {
	APIURL           string
	PublicAPI        bool
	TokenFile        string
	OutputFile       string
	GOSTAPIURL       string
	GOSTAPIAddress   string
	GOSTAPIUsername  string
	GOSTAPIPassword  string
	ProxyUsername    string
	ProxyPassword    string
	CountryField     string
	Countries        []string
	RefreshInterval  time.Duration
	RequestTimeout   time.Duration
	MinimumRatioPct  int64
	MaximumLatencyMs int64
	Country          string
	ExitCountry      string
	ASN              string
	Anonymity        string
	GeoMismatch      bool
	// ExcludeTampered sends exclude_tampered=true so proxies whose fresh echo
	// tamper check failed are not routed.
	ExcludeTampered bool
	Limit           int
}

type apiResponse struct {
	Proxies []store.QueryProxy `json:"proxies"`
}

func (o SyncOptions) defaults() SyncOptions {
	if o.APIURL == "" {
		if o.PublicAPI {
			o.APIURL = "https://freeproxyapi.crawlora.net/proxies"
		} else {
			o.APIURL = "http://freeproxyapi:8080/internal/api/v1/proxies"
		}
	}
	if o.TokenFile == "" {
		o.TokenFile = "/internal-api/token"
	}
	if o.OutputFile == "" {
		o.OutputFile = "/runtime/gost.json"
	}
	if o.GOSTAPIURL == "" {
		o.GOSTAPIURL = "http://127.0.0.1:18080"
	}
	if o.GOSTAPIAddress == "" {
		o.GOSTAPIAddress = "127.0.0.1:18080"
	}
	if o.RefreshInterval <= 0 {
		o.RefreshInterval = time.Minute
	}
	if o.RequestTimeout <= 0 {
		o.RequestTimeout = 15 * time.Second
	}
	if o.MinimumRatioPct <= 0 {
		o.MinimumRatioPct = 80
	}
	if o.Limit <= 0 {
		o.Limit = 1000
	}
	return o
}

// maxAPIResponseBytes bounds the FreeProxyAPI response body.
const maxAPIResponseBytes = 8 << 20

// syncer carries reload state between refreshes so GOST is reloaded only when
// the rendered config changed or the previous reload did not succeed.
type syncer struct {
	reloadPending bool
}

// newSyncer starts with a pending reload: GOST may still be running a
// bootstrap config even when the file on disk already matches.
func newSyncer() *syncer { return &syncer{reloadPending: true} }

func Run(ctx context.Context, opts SyncOptions) error {
	opts = opts.defaults()
	client := &http.Client{Timeout: opts.RequestTimeout}
	state := newSyncer()
	if err := writeBootstrapConfig(opts); err != nil {
		log.Printf("warning: GOST bootstrap config not written: %v", err)
	}
	for {
		if err := state.sync(ctx, client, opts); err != nil {
			log.Printf("warning: GOST config sync failed: %v", err)
		} else {
			log.Printf("GOST config sync complete")
		}
		timer := time.NewTimer(opts.RefreshInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

// writeBootstrapConfig gives GOST an API-only config to start from when no
// config exists yet, so it does not exit while FreeProxyAPI is unavailable. An
// existing file, such as a real config kept across a proxy-router restart, is
// left untouched.
func writeBootstrapConfig(opts SyncOptions) error {
	if _, err := os.Stat(opts.OutputFile); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat GOST config: %w", err)
	}
	config, err := BuildBootstrapConfig(BuildOptions{
		APIUsername: opts.GOSTAPIUsername,
		APIPassword: opts.GOSTAPIPassword,
		APIAddress:  opts.GOSTAPIAddress,
	})
	if err != nil {
		return err
	}
	if _, err := writeIfChanged(opts.OutputFile, config); err != nil {
		return err
	}
	log.Printf("wrote GOST bootstrap config")
	return nil
}

// SyncOnce performs one standalone refresh. Without state from earlier
// refreshes it always reloads GOST after writing the config; Run reloads only
// on changes or after a failed reload.
func SyncOnce(ctx context.Context, client *http.Client, opts SyncOptions) error {
	return newSyncer().sync(ctx, client, opts)
}

func (s *syncer) sync(ctx context.Context, client *http.Client, opts SyncOptions) error {
	opts = opts.defaults()
	var tokenValue string
	if !opts.PublicAPI {
		token, err := os.ReadFile(opts.TokenFile)
		if err != nil {
			return fmt.Errorf("read API token: %w", err)
		}
		tokenValue = strings.TrimSpace(string(token))
		if len(tokenValue) < 16 {
			return fmt.Errorf("API token is shorter than 16 characters")
		}
	}

	requestURL, err := url.Parse(opts.APIURL)
	if err != nil {
		return fmt.Errorf("parse API URL: %w", err)
	}
	query := requestURL.Query()
	if opts.Limit > 0 {
		query.Set("limit", strconv.Itoa(opts.Limit))
	}
	setOptionalQuery(query, "country", opts.Country)
	setOptionalQuery(query, "exit_country", opts.ExitCountry)
	setOptionalQuery(query, "asn", opts.ASN)
	setOptionalQuery(query, "anonymity", opts.Anonymity)
	if opts.GeoMismatch {
		query.Set("geo_mismatch", "true")
	}
	if opts.ExcludeTampered {
		query.Set("exclude_tampered", "true")
	}
	if opts.MinimumRatioPct > 0 {
		query.Set("min_ratio_pct", strconv.FormatInt(opts.MinimumRatioPct, 10))
	}
	if opts.MaximumLatencyMs > 0 {
		query.Set("max_latency_ms", strconv.FormatInt(opts.MaximumLatencyMs, 10))
	}
	requestURL.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL.String(), nil)
	if err != nil {
		return fmt.Errorf("create API request: %w", err)
	}
	if !opts.PublicAPI {
		request.Header.Set("Authorization", "Bearer "+tokenValue)
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("query FreeProxyAPI: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("query FreeProxyAPI returned HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxAPIResponseBytes+1))
	if err != nil {
		return fmt.Errorf("read FreeProxyAPI response: %w", err)
	}
	if len(body) > maxAPIResponseBytes {
		return fmt.Errorf("FreeProxyAPI response exceeds %d byte limit; lower the proxy limit", maxAPIResponseBytes)
	}
	var payload apiResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return fmt.Errorf("decode FreeProxyAPI response: %w", err)
	}
	config, err := BuildConfig(payload.Proxies, BuildOptions{
		CountryField:  opts.CountryField,
		Countries:     opts.Countries,
		ProxyUsername: opts.ProxyUsername,
		ProxyPassword: opts.ProxyPassword,
		APIUsername:   opts.GOSTAPIUsername,
		APIPassword:   opts.GOSTAPIPassword,
		APIAddress:    opts.GOSTAPIAddress,
	})
	if err != nil {
		return err
	}
	changed, err := writeIfChanged(opts.OutputFile, config)
	if err != nil {
		return err
	}
	if !changed && !s.reloadPending {
		return nil
	}
	// Keep the reload pending until it succeeds. This retries the initial
	// reload if GOST was still starting when the first config was written.
	s.reloadPending = true
	if err := reloadGOST(ctx, client, opts); err != nil {
		return err
	}
	s.reloadPending = false
	return nil
}

func setOptionalQuery(query url.Values, key, value string) {
	if value != "" {
		query.Set(key, value)
	}
}

func writeIfChanged(path string, data []byte) (bool, error) {
	if previous, err := os.ReadFile(path); err == nil && bytes.Equal(previous, data) {
		return false, nil
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return false, fmt.Errorf("create GOST config directory: %w", err)
	}
	temporary, err := os.CreateTemp(dir, ".gost-*.json")
	if err != nil {
		return false, fmt.Errorf("create temporary GOST config: %w", err)
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0600); err != nil {
		temporary.Close()
		return false, fmt.Errorf("protect temporary GOST config: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return false, fmt.Errorf("write temporary GOST config: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return false, fmt.Errorf("sync temporary GOST config: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return false, fmt.Errorf("close temporary GOST config: %w", err)
	}
	if err := os.Rename(temporaryName, path); err != nil {
		return false, fmt.Errorf("install GOST config: %w", err)
	}
	// Best effort: persist the rename itself.
	if directory, err := os.Open(dir); err == nil {
		_ = directory.Sync()
		_ = directory.Close()
	}
	return true, nil
}

func reloadGOST(ctx context.Context, client *http.Client, opts SyncOptions) error {
	reloadURL := strings.TrimRight(opts.GOSTAPIURL, "/") + "/config/reload"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, reloadURL, nil)
	if err != nil {
		return fmt.Errorf("create GOST reload request: %w", err)
	}
	request.SetBasicAuth(opts.GOSTAPIUsername, opts.GOSTAPIPassword)
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("reload GOST: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("reload GOST returned HTTP %d", response.StatusCode)
	}
	return nil
}
