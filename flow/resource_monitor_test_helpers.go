package flow

import (
	"testing"
	"time"
)

const (
	testSampleInterval    = 10 * time.Millisecond
	testStopTimeout       = 50 * time.Millisecond
	testStatsUpdateMargin = 20 * time.Millisecond
)

type mockCPUSampler struct {
	val float64
}

func (m *mockCPUSampler) Sample(_ time.Duration) float64 {
	return m.val
}

func (m *mockCPUSampler) Reset() {
	// No-op for mock
}

func (m *mockCPUSampler) IsInitialized() bool {
	return true
}

type assertError string

func (e assertError) Error() string { return string(e) }

func resetRegistry() {
	globalMonitorRegistry = &monitorRegistry{
		intervalRefs: make(map[time.Duration]int),
	}
}

func setupTest(t *testing.T) {
	t.Helper()
	resetRegistry()
	t.Cleanup(resetRegistry)
}

// assertValidStats checks that resource stats contain reasonable values.
func assertValidStats(t *testing.T, stats ResourceStats) {
	t.Helper()
	if stats.Timestamp.IsZero() {
		t.Error("Timestamp should not be zero")
	}
	if stats.GoroutineCount < 0 {
		t.Errorf("GoroutineCount should not be negative, got %d", stats.GoroutineCount)
	}
	if stats.MemoryUsedPercent < 0 || stats.MemoryUsedPercent > 200 { // Allow >100% for some systems
		t.Errorf("MemoryUsedPercent should be between 0-200, got %f", stats.MemoryUsedPercent)
	}
	if stats.CPUUsagePercent < 0 {
		t.Errorf("CPUUsagePercent should not be negative, got %f", stats.CPUUsagePercent)
	}
}
