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

// apiResponse is one page of the FreeProxyAPI response. HasMore is only sent by
// the public /proxies endpoint; the internal API returns everything in one
// response, so HasMore is false and the router makes a single request.
type apiResponse struct {
	Proxies []store.QueryProxy `json:"proxies"`
	HasMore bool               `json:"has_more"`
}

// maxFetchPages bounds how many pages one refresh follows. The server's page
// size can be as small as 100 and the total is capped by SyncOptions.Limit
// (1000 by default), so ten pages are normal and this is only a backstop.
const maxFetchPages = 50

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
	proxies, err := fetchProxies(ctx, client, opts, requestURL, query, tokenValue)
	if err != nil {
		return err
	}
	config, err := BuildConfig(proxies, BuildOptions{
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

// fetchProxies reads up to opts.Limit proxies, following the server's paging.
//
// The first request asks for the whole limit with no offset, exactly as before,
// so a server that returns everything in one response (the internal API, or a
// public API whose maximum page size is at least the limit) costs one request.
// If the response says has_more, the router continues from the number of
// proxies received until the limit is reached or the server has no more. That keeps it working when a
// server caps its page size below the limit, which would otherwise silently
// shrink the pool to one page.
//
// Any failed page fails the whole refresh: the caller then keeps the last
// generated GOST config instead of replacing it with a partial pool. Pages can
// overlap when the validated pool changes between requests, so proxies are
// deduplicated by URL.
func fetchProxies(ctx context.Context, client *http.Client, opts SyncOptions, base *url.URL, query url.Values, token string) ([]store.QueryProxy, error) {
	// The per-request timeout still applies to each page; this bounds the whole
	// refresh so a long chain of slow pages cannot outlast the refresh interval.
	ctx, cancel := context.WithTimeout(ctx, 4*opts.RequestTimeout)
	defer cancel()

	var proxies []store.QueryProxy
	seen := make(map[string]struct{})
	offset := 0
	for page := 1; page <= maxFetchPages; page++ {
		// The same limit on every page: the server clamps it to its page size and
		// caches by the clamped value, so every router asks for the same pages.
		pageQuery := cloneQuery(query)
		pageQuery.Set("limit", strconv.Itoa(opts.Limit))
		if offset > 0 {
			pageQuery.Set("offset", strconv.Itoa(offset))
		}
		payload, err := fetchPage(ctx, client, opts, base, pageQuery, token)
		if err != nil {
			if page > 1 {
				return nil, fmt.Errorf("page %d: %w", page, err)
			}
			return nil, err
		}
		for _, proxy := range payload.Proxies {
			if _, duplicate := seen[proxy.URL]; duplicate {
				continue
			}
			seen[proxy.URL] = struct{}{}
			proxies = append(proxies, proxy)
		}
		if len(proxies) >= opts.Limit {
			return proxies[:opts.Limit], nil
		}
		// Advance by what this page returned. An empty page cannot make
		// progress, so stop rather than repeat the same request.
		if !payload.HasMore || len(payload.Proxies) == 0 {
			return proxies, nil
		}
		offset += len(payload.Proxies)
	}
	return proxies, nil
}

func cloneQuery(query url.Values) url.Values {
	clone := make(url.Values, len(query)+2)
	for key, values := range query {
		clone[key] = append([]string(nil), values...)
	}
	return clone
}

// fetchPage requests and decodes one page.
func fetchPage(ctx context.Context, client *http.Client, opts SyncOptions, base *url.URL, query url.Values, token string) (apiResponse, error) {
	requestURL := *base
	requestURL.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL.String(), nil)
	if err != nil {
		return apiResponse{}, fmt.Errorf("create API request: %w", err)
	}
	if !opts.PublicAPI {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := client.Do(request)
	if err != nil {
		return apiResponse{}, fmt.Errorf("query FreeProxyAPI: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return apiResponse{}, fmt.Errorf("query FreeProxyAPI returned HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxAPIResponseBytes+1))
	if err != nil {
		return apiResponse{}, fmt.Errorf("read FreeProxyAPI response: %w", err)
	}
	if len(body) > maxAPIResponseBytes {
		return apiResponse{}, fmt.Errorf("FreeProxyAPI response exceeds %d byte limit; lower the proxy limit", maxAPIResponseBytes)
	}
	var payload apiResponse
	if err := json.Unmarshal(body, &payload); err != nil {
		return apiResponse{}, fmt.Errorf("decode FreeProxyAPI response: %w", err)
	}
	return payload, nil
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
