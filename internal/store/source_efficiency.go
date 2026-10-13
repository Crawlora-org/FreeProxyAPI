package store

import "time"

// RoutingEligible reports whether an already-validated record has the strict
// evidence required by the live GOST router. Callers obtain these records from
// ValidatedDetails, so set membership is implied. A non-positive maxAge removes
// the age bound but never trusts missing or future timestamps.
func (v ValidatedInfo) RoutingEligible(now time.Time, maxAge time.Duration) bool {
	nowMS := now.UnixMilli()
	if nowMS <= 0 || v.OkRatioPct < 80 || v.SuccessStreak == nil || *v.SuccessStreak < 1 {
		return false
	}
	fresh := func(checkedAt int64) bool {
		if checkedAt <= 0 || checkedAt > nowMS {
			return false
		}
		return maxAge <= 0 || nowMS-checkedAt <= maxAge.Milliseconds()
	}
	return v.HTTPSOK != nil && *v.HTTPSOK && fresh(v.HTTPSCheckedAt) &&
		v.Tampered != nil && !*v.Tampered && fresh(v.TamperCheckedAt)
}
