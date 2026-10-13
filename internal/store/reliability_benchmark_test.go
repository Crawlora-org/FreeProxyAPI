package store

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

var reliabilityBenchmarkResult reliabilitySummary

func BenchmarkSummarizeReliability168SparseHours(b *testing.B) {
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC).UnixMilli()
	currentHour := now / int64(time.Hour/time.Millisecond)
	var encoded strings.Builder
	for i := int64(167); i >= 0; i-- {
		if encoded.Len() > 0 {
			encoded.WriteByte(',')
		}
		encoded.WriteString(strconv.FormatInt(currentHour-i, 10))
		encoded.WriteString(":3:2")
	}
	input := encoded.String()
	b.ReportAllocs()
	b.SetBytes(int64(len(input)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reliabilityBenchmarkResult = summarizeReliability(input, now)
	}
}
