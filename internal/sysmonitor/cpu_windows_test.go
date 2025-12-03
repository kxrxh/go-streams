//go:build windows

package sysmonitor

import (
	"testing"
	"time"
)

// TestWindowsSamplerIntegration runs real calls against the OS.
func TestWindowsSamplerIntegration(t *testing.T) {
	sampler, err := newPlatformCPUSampler(nil)
	if err != nil {
		t.Fatalf("Failed to create Windows sampler: %v", err)
	}

	// 1. Initialize
	val := sampler.Sample(time.Second)
	if val != 0.0 {
		t.Errorf("Expected initial sample 0.0, got %f", val)
	}

	// 2. Generate Load (Busy Loop) to ensure non-zero CPU
	done := make(chan struct{})
	go func() {
		end := time.Now().Add(100 * time.Millisecond)
		for time.Now().Before(end) {
		}
		close(done)
	}()
	<-done

	// Sleep a tiny bit to ensure total elapsed > busy loop time
	time.Sleep(50 * time.Millisecond)

	// 3. Measure
	val = sampler.Sample(0)

	// Log result (useful for verification)
	t.Logf("Measured CPU Load: %f%%", val)

	if val <= 0.0 {
		t.Error("Expected non-zero CPU usage after busy loop")
	}
	if val > 100.0 {
		t.Errorf("CPU usage %f exceeds 100%%", val)
	}
}
