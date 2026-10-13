package store

import (
	"encoding/json"
	"strings"
)

const MaxTargetProfiles = 8

// TargetProfileResult is one observation of an operator-configured content
// contract. Unknown is distinct from a definite HTTP/content failure.
type TargetProfileResult struct {
	Fingerprint string `json:"fingerprint,omitempty"`
	State       string `json:"state"`
	CheckedAt   int64  `json:"checked_at_ms"`
	HTTPStatus  int    `json:"http_status,omitempty"`
	LatencyMs   int64  `json:"latency_ms,omitempty"`
	ErrorCode   string `json:"error_code,omitempty"`
}

type TargetProfileUpdate struct {
	Active map[string]string              `json:"active"`
	Checks map[string]TargetProfileResult `json:"checks"`
}

func encodeTargetProfileUpdate(update *TargetProfileUpdate) string {
	if update == nil || len(update.Active) == 0 || len(update.Active) > MaxTargetProfiles || len(update.Checks) > MaxTargetProfiles {
		return ""
	}
	encoded, err := json.Marshal(update)
	if err != nil {
		return ""
	}
	return string(encoded)
}

// FreshTargetProfiles returns only well-formed observations whose timestamps
// are usable. A filter fingerprint prevents a reused name matching an older
// URL or expected body.
func FreshTargetProfiles(encoded string, nowMs, maxAgeMs int64) map[string]TargetProfileResult {
	if encoded == "" || len(encoded) > 16<<10 {
		return nil
	}
	var results map[string]TargetProfileResult
	if json.Unmarshal([]byte(encoded), &results) != nil || len(results) > MaxTargetProfiles {
		return nil
	}
	for name, result := range results {
		if name == "" || len(name) > 32 || len(result.Fingerprint) != 64 || result.CheckedAt <= 0 || result.CheckedAt > nowMs ||
			(maxAgeMs > 0 && nowMs-result.CheckedAt > maxAgeMs) ||
			(result.State != "pass" && result.State != "fail" && result.State != "unknown") || len(result.ErrorCode) > 32 || strings.ContainsAny(name, " \r\n") {
			delete(results, name)
		}
	}
	if len(results) == 0 {
		return nil
	}
	return results
}

// targetProfileCompleteLua is appended to the lease-guarded completion script.
// The configured active set bounds persisted profiles after configuration changes.
const targetProfileCompleteLua = `
if ARGV[31] ~= nil and ARGV[31] ~= '' then
  local update = cjson.decode(ARGV[31])
  local previous = {}
  local raw = redis.call('HGET', key, 'target_profiles')
  if raw then
    local valid, parsed = pcall(cjson.decode, raw)
    if valid and type(parsed) == 'table' then previous = parsed end
  end
  local retained = {}
  local count = 0
  for name, fingerprint in pairs(update.active) do
    count = count + 1
    if count <= 8 then
      local result = update.checks[name] or previous[name]
      if type(result) == 'table' and result.fingerprint == fingerprint then
        retained[name] = result
      end
    end
  end
  if next(retained) == nil then
    redis.call('HSET', key, 'target_profiles', '{}')
  else
    redis.call('HSET', key, 'target_profiles', cjson.encode(retained))
  end
end
`
