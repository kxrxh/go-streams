//go:build windows

package sysmonitor

import (
	"math"
	"testing"
)

func TestWindowsMemoryReaderIntegration(t *testing.T) {
	reader := newPlatformMemoryReader(nil).(*windowsMemoryReader)

	mem, err := reader.Sample()
	if err != nil {
		t.Fatalf("Failed to sample Windows memory: %v", err)
	}

	if mem.Total == 0 {
		t.Error("Expected non-zero total memory")
	}

	if mem.Available == 0 {
		t.Error("Expected non-zero available memory")
	}

	if mem.Available > mem.Total {
		t.Errorf("Available memory (%d) should not exceed total memory (%d)", mem.Available, mem.Total)
	}

	t.Logf("Windows Memory - Total: %d bytes, Available: %d bytes", mem.Total, mem.Available)
}

func TestNewProcessMemoryReader(t *testing.T) {
	// Test the public API function
	reader := NewProcessMemoryReader(nil)

	// Should return a windowsMemoryReader
	if reader == nil {
		t.Fatal("NewProcessMemoryReader returned nil")
	}
}

func TestWindowsMemoryReader_ReasonableValues(t *testing.T) {
	reader := newPlatformMemoryReader(nil).(*windowsMemoryReader)

	mem, err := reader.Sample()
	if err != nil {
		t.Fatalf("Failed to sample Windows memory: %v", err)
	}

	// Windows systems typically have at least 1GB RAM
	const minExpectedMemory = 1024 * 1024 * 1024 // 1GB in bytes
	if mem.Total < minExpectedMemory {
		t.Errorf("Total memory (%d bytes) seems unreasonably low, expected at least %d bytes", mem.Total, minExpectedMemory)
	}

	// Available memory should be less than or equal to total
	if mem.Available > mem.Total {
		t.Errorf("Available memory (%d) should not exceed total memory (%d)", mem.Available, mem.Total)
	}
}

func TestWindowsMemoryReader_Consistency(t *testing.T) {
	reader := newPlatformMemoryReader(nil).(*windowsMemoryReader)

	// Take multiple samples to ensure consistency
	var samples []SystemMemory
	for i := 0; i < 3; i++ {
		mem, err := reader.Sample()
		if err != nil {
			t.Fatalf("Failed to sample Windows memory on iteration %d: %v", i, err)
		}
		samples = append(samples, mem)
	}

	// Total memory should be consistent across samples
	for i := 1; i < len(samples); i++ {
		if samples[i].Total != samples[0].Total {
			t.Errorf("Total memory inconsistent: sample 0: %d, sample %d: %d",
				samples[0].Total, i, samples[i].Total)
		}
	}

	// Available memory should be reasonable (not wildly different)
	for i := 1; i < len(samples); i++ {
		availableDiff := math.Abs(float64(samples[i].Available) - float64(samples[0].Available))
		if availableDiff > float64(samples[0].Total/4) {
			t.Errorf("Available memory changed dramatically: sample 0: %d, sample %d: %d",
				samples[0].Available, i, samples[i].Available)
		}
	}
}
