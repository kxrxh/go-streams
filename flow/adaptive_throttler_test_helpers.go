package flow

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type MockMonitor struct {
	getStatsReturns []ResourceStats
	getStatsIndex   int
	closeCalled     bool
}

func (m *MockMonitor) GetStats() ResourceStats {
	if m.getStatsIndex < len(m.getStatsReturns) {
		result := m.getStatsReturns[m.getStatsIndex]
		m.getStatsIndex++
		return result
	}
	return ResourceStats{}
}

func (m *MockMonitor) Close() {
	m.closeCalled = true
}

func (m *MockMonitor) ExpectGetStats(stats ...ResourceStats) {
	m.getStatsReturns = stats
	m.getStatsIndex = 0
}

type mockSink struct {
	in         chan any
	completion chan struct{}
	completed  atomic.Bool
}

func (m *mockSink) In() chan<- any {
	return m.in
}

func (m *mockSink) AwaitCompletion() {
	if !m.completed.Load() {
		if m.completed.CompareAndSwap(false, true) {
			if m.completion != nil {
				close(m.completion)
			}
		}
	}
}

type mockSinkWithChannelDrain struct {
	in   chan any
	done chan struct{}
}

func (m *mockSinkWithChannelDrain) In() chan<- any {
	return m.in
}

func (m *mockSinkWithChannelDrain) AwaitCompletion() {
	<-m.done
}

// newMockSinkWithChannelDrain creates a mock sink that drains the channel like real sinks do.
func newMockSinkWithChannelDrain() *mockSinkWithChannelDrain {
	sink := &mockSinkWithChannelDrain{
		in:   make(chan any),
		done: make(chan struct{}),
	}
	go func() {
		defer close(sink.done)
		for range sink.in {
			_ = struct{}{}
		}
	}()
	return sink
}

// createThrottlerWithLongInterval creates a throttler with default config but long sample interval.
func createThrottlerWithLongInterval(t *testing.T) *AdaptiveThrottler {
	t.Helper()
	config := DefaultAdaptiveThrottlerConfig()
	config.SampleInterval = 10 * time.Second
	at, err := NewAdaptiveThrottler(config)
	if err != nil {
		t.Fatalf("Failed to create throttler: %v", err)
	}
	return at
}

// collectDataFromChannel collects all data from a channel into a slice.
// If the channel is buffered, we reserve that capacity up front.
func collectDataFromChannel(ch <-chan any) []any {
	received := make([]any, 0, len(ch))
	for data := range ch {
		received = append(received, data)
	}
	return received
}

// collectDataFromChannelWithMutex collects data from channel using mutex for thread safety.
func collectDataFromChannelWithMutex(ch <-chan any, received *[]any, mu *sync.Mutex, done chan struct{}) {
	defer close(done)
	for data := range ch {
		mu.Lock()
		*received = append(*received, data)
		mu.Unlock()
	}
}

// verifyChannelClosed checks that a channel is closed within a timeout.
func verifyChannelClosed(t *testing.T, ch <-chan any, timeout time.Duration) {
	t.Helper()
	select {
	case _, ok := <-ch:
		if ok {
			t.Errorf("Channel should be closed")
		}
	case <-time.After(timeout):
		t.Errorf("Channel should be closed within %v", timeout)
	}
}

// createThrottlerForRateTesting creates a throttler with mock monitor for rate adjustment testing.
func createThrottlerForRateTesting(
	config *AdaptiveThrottlerConfig,
	initialRate float64,
) (*AdaptiveThrottler, *MockMonitor) {
	mockMonitor := &MockMonitor{}
	throttler := &AdaptiveThrottler{
		config:          *config,
		monitor:         mockMonitor,
		currentRateBits: math.Float64bits(initialRate),
	}
	return throttler, mockMonitor
}

// simulateRateAdjustments simulates a sequence of rate adjustments and returns the final rate.
// This is used to calculate expected rate ranges algorithmically instead of hardcoding them.
func simulateRateAdjustments(
	config *AdaptiveThrottlerConfig,
	initialRate float64,
	statsSequence []ResourceStats,
) float64 {
	currentRate := initialRate

	for _, stats := range statsSequence {
		currentRate = calculateNewRate(config, currentRate, stats)
	}

	return currentRate
}

// calculateExpectedRateRange calculates the expected final rate range for a sustained load scenario.
// Returns min and max expected rates based on the algorithm behavior.
func calculateExpectedRateRange(
	config *AdaptiveThrottlerConfig,
	initialRate float64,
	statsSequence []ResourceStats,
) (float64, float64) {
	finalRate := simulateRateAdjustments(config, initialRate, statsSequence)

	tolerance := 0.1 // 10% tolerance

	minExpected := finalRate * (1 - tolerance)
	maxExpected := finalRate * (1 + tolerance)

	if minExpected < float64(config.MinRate) {
		minExpected = float64(config.MinRate)
	}
	if maxExpected > float64(config.MaxRate) {
		maxExpected = float64(config.MaxRate)
	}

	return minExpected, maxExpected
}

// calculateExpectedRecoveryRate calculates the expected recovery rate after a high load followed by normal load.
// Returns the expected rate after the specified number of recovery cycles.
func calculateExpectedRecoveryRate(
	config *AdaptiveThrottlerConfig,
	initialRate float64,
	highLoadStats, normalLoadStats ResourceStats,
	recoveryCycles int,
) float64 {
	currentRate := initialRate

	currentRate = simulateSingleAdjustment(config, currentRate, highLoadStats)

	for i := 0; i < recoveryCycles+1; i++ { // +1 for the first normal load adjustment
		currentRate = simulateSingleAdjustment(config, currentRate, normalLoadStats)
	}

	return currentRate
}

// simulateSingleAdjustment simulates a single rate adjustment for given stats.
func simulateSingleAdjustment(config *AdaptiveThrottlerConfig, currentRate float64, stats ResourceStats) float64 {
	return calculateNewRate(config, currentRate, stats)
}

type mockInlet struct {
	in chan any
}

func (m *mockInlet) In() chan<- any {
	return m.in
}
