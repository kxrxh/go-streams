package flow

import (
	"math"
	"testing"
	"time"

	"github.com/reugn/go-streams/internal/sysmonitor"
)

// Tests for monitor creation and sampling behavior.
func TestResourceMonitor_Initialization(t *testing.T) {
	setupTest(t)

	// Test with Heuristic preference - should use best available (measured if possible)
	rm := newResourceMonitor(time.Second, CPUUsageModeHeuristic, nil)
	if rm.sampler == nil {
		t.Error("Expected sampler to be initialized, got nil")
	}
	if rm.cpuMode != CPUUsageModeMeasured && rm.cpuMode != CPUUsageModeHeuristic {
		t.Errorf("Unexpected CPU mode: %v", rm.cpuMode)
	}
	rm.stop()

	// Test with Measured preference - should use measured if available
	rm2 := newResourceMonitor(time.Second, CPUUsageModeMeasured, nil)
	if rm2.sampler == nil {
		t.Error("Expected sampler to be initialized, got nil")
	}
	if rm2.cpuMode != CPUUsageModeMeasured && rm2.cpuMode != CPUUsageModeHeuristic {
		t.Errorf("Unexpected CPU mode: %v", rm2.cpuMode)
	}
	rm2.stop()
}

func TestResourceMonitor_Sample_MemoryReader(t *testing.T) {
	setupTest(t)

	expectedMem := 42.5
	mockMemReader := func() (float64, error) {
		return expectedMem, nil
	}

	// Create monitor with long interval to prevent auto-sampling during test
	rm := newResourceMonitor(time.Hour, CPUUsageModeHeuristic, mockMemReader)
	defer rm.stop()

	// Get initial stats before manual sample
	initialStats := rm.GetStats()

	// Trigger sample manually
	rm.sample()

	stats := rm.GetStats()

	if stats.MemoryUsedPercent != expectedMem {
		t.Errorf("Expected memory %f, got %f", expectedMem, stats.MemoryUsedPercent)
	}

	assertValidStats(t, stats)
	if !stats.Timestamp.After(initialStats.Timestamp) {
		t.Error("Timestamp should be updated after sampling")
	}
}

func TestResourceMonitor_Sample_CPUMock(t *testing.T) {
	setupTest(t)

	expectedCPU := 12.34
	mockSampler := &mockCPUSampler{val: expectedCPU}

	rm := newResourceMonitor(time.Hour, CPUUsageModeMeasured, nil)
	defer rm.stop()

	rm.sampler = mockSampler

	rm.sample()
	stats := rm.GetStats()

	if stats.CPUUsagePercent != expectedCPU {
		t.Errorf("Expected CPU %f, got %f", expectedCPU, stats.CPUUsagePercent)
	}
}

func TestResourceMonitor_Sample_HeuristicMode(t *testing.T) {
	setupTest(t)

	rm := newResourceMonitor(time.Hour, CPUUsageModeHeuristic, nil)
	defer rm.stop()

	rm.sampler = sysmonitor.NewGoroutineHeuristicSampler()
	rm.cpuMode = CPUUsageModeHeuristic

	rm.sample()
	stats := rm.GetStats()

	if stats.CPUUsagePercent <= 0 {
		t.Errorf("Expected positive CPU usage in heuristic mode, got %f", stats.CPUUsagePercent)
	}
	if stats.CPUUsagePercent > 100 {
		t.Errorf("Expected CPU usage <= 100%%, got %f", stats.CPUUsagePercent)
	}
}

func TestResourceMonitor_SetInterval_Dynamic(t *testing.T) {
	setupTest(t)

	rm := newResourceMonitor(10*time.Minute, CPUUsageModeHeuristic, nil)
	defer rm.stop()

	initialStats := rm.GetStats()

	newDuration := time.Millisecond
	rm.setInterval(newDuration)

	deadline := time.Now().Add(1 * time.Second)
	updated := false
	for time.Now().Before(deadline) {
		if rm.getSampleInterval() == newDuration {
			updated = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !updated {
		t.Error("Failed to update interval dynamically")
	}

	time.Sleep(time.Millisecond)
	currentStats := rm.GetStats()
	if !currentStats.Timestamp.After(initialStats.Timestamp) {
		t.Error("Expected immediate sample on interval switch")
	}
}

func TestMemoryFallback(t *testing.T) {
	setupTest(t)

	rm := newResourceMonitor(time.Hour, CPUUsageModeHeuristic, nil)
	defer rm.stop()

	rm.sample()
	stats := rm.GetStats()

	if stats.MemoryUsedPercent < 0 || stats.MemoryUsedPercent > 100 {
		t.Errorf("Expected memory percentage in range [0,100], got %f", stats.MemoryUsedPercent)
	}
	if stats.GoroutineCount <= 0 {
		t.Errorf("Expected goroutine count > 0, got %d", stats.GoroutineCount)
	}
	if stats.CPUUsagePercent < 0 {
		t.Errorf("Expected CPU usage >= 0, got %f", stats.CPUUsagePercent)
	}
	if stats.Timestamp.IsZero() {
		t.Error("Expected non-zero timestamp")
	}
}

func TestMemoryReaderError(t *testing.T) {
	setupTest(t)

	errReader := func() (float64, error) {
		return 0, assertError("fail")
	}

	rm := newResourceMonitor(time.Hour, CPUUsageModeHeuristic, errReader)
	defer rm.stop()

	initialStats := rm.GetStats()
	rm.sample()
	stats := rm.GetStats()

	if stats.MemoryUsedPercent < 0 {
		t.Errorf("Expected memory percentage >= 0, got %f", stats.MemoryUsedPercent)
	}
	if stats.MemoryUsedPercent > 100 {
		t.Errorf("Expected memory percentage <= 100, got %f", stats.MemoryUsedPercent)
	}
	if stats.GoroutineCount <= 0 {
		t.Errorf("Expected goroutine count > 0, got %d", stats.GoroutineCount)
	}
	if stats.CPUUsagePercent < 0 {
		t.Errorf("Expected CPU usage >= 0, got %f", stats.CPUUsagePercent)
	}
	if !stats.Timestamp.After(initialStats.Timestamp) {
		t.Error("Expected timestamp to be updated")
	}
}

func TestResourceMonitor_CPU_SamplerError(t *testing.T) {
	setupTest(t)

	rm := newResourceMonitor(time.Hour, CPUUsageModeMeasured, nil)
	defer rm.stop()

	failingSampler := &mockCPUSampler{val: -1.0}
	rm.sampler = failingSampler

	rm.sample()
	stats := rm.GetStats()

	if math.IsNaN(stats.CPUUsagePercent) {
		t.Error("Expected valid CPU usage value, got NaN")
	}
	if stats.GoroutineCount <= 0 {
		t.Errorf("Expected goroutine count > 0, got %d", stats.GoroutineCount)
	}
	if stats.MemoryUsedPercent < 0 {
		t.Errorf("Expected memory percentage >= 0, got %f", stats.MemoryUsedPercent)
	}
}

func TestResourceMonitor_SetMode(t *testing.T) {
	setupTest(t)

	rm := newResourceMonitor(time.Second, CPUUsageModeHeuristic, nil)
	defer rm.stop()

	initialMode := rm.GetMode()
	if initialMode != CPUUsageModeMeasured && initialMode != CPUUsageModeHeuristic {
		t.Errorf("Unexpected initial mode: %v", initialMode)
	}

	rm.SetMode(CPUUsageModeHeuristic)
	if rm.GetMode() != CPUUsageModeHeuristic {
		t.Errorf("Expected mode Heuristic after switching, got %v", rm.GetMode())
	}

	rm.SetMode(CPUUsageModeMeasured)
	if rm.GetMode() != CPUUsageModeMeasured {
		t.Errorf("Expected mode Measured after switching back, got %v", rm.GetMode())
	}

	rm.SetMode(CPUUsageModeMeasured)
	if rm.GetMode() != CPUUsageModeMeasured {
		t.Errorf("Expected mode to remain Measured, got %v", rm.GetMode())
	}
}

func TestResourceMonitor_Sample_RuntimeMemoryFallback(t *testing.T) {
	setupTest(t)

	rm := newResourceMonitor(time.Hour, CPUUsageModeHeuristic, nil)
	defer rm.stop()

	rm.memoryReaderInstance = nil

	initialStats := rm.GetStats()

	rm.sample()
	stats := rm.GetStats()

	assertValidStats(t, stats)
	if !stats.Timestamp.After(initialStats.Timestamp) {
		t.Error("Timestamp should be updated after sampling")
	}
	if stats.GoroutineCount <= 0 {
		t.Errorf("Expected goroutine count > 0, got %d", stats.GoroutineCount)
	}
	if stats.MemoryUsedPercent < 0 || stats.MemoryUsedPercent > 100 {
		t.Errorf("Expected memory percentage in range [0,100], got %f", stats.MemoryUsedPercent)
	}
}
