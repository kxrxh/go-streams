package duckdb

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/reugn/go-streams"
)

func TestDuckDBSink_Interface(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "duckdb_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	os.Remove(tmpFile.Name())
	defer os.Remove(tmpFile.Name())

	db, err := sql.Open("duckdb", tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test_table (id INTEGER)")
	if err != nil {
		t.Fatal(err)
	}

	config := SinkConfig{TableName: "test_table"}
	sink := NewSink(context.Background(), db, config, nil)
	if sink == nil {
		t.Fatal("NewSink returned nil")
	}

	// Verify Sink interface compliance
	var _ streams.Sink = sink

	in := sink.In()
	if in == nil {
		t.Error("In() returned nil channel")
	}

	close(in)
	sink.AwaitCompletion()
}

func TestDuckDBSink_BasicInsert(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "duckdb_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	os.Remove(tmpFile.Name())
	defer os.Remove(tmpFile.Name())

	db, err := sql.Open("duckdb", tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test_table (id INTEGER, name VARCHAR, value DOUBLE)")
	if err != nil {
		t.Fatal(err)
	}

	config := SinkConfig{TableName: "test_table"}
	sink := NewSink(context.Background(), db, config, slog.Default())
	if sink == nil {
		t.Fatal("NewSink returned nil")
	}

	record := Record{"id": 1, "name": "test", "value": 42.5}
	sink.In() <- record
	close(sink.In())
	sink.AwaitCompletion()

	// Verify single record insertion
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test_table").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count records: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 record, got %d", count)
	}

	var id int
	var name string
	var value float64
	err = db.QueryRow("SELECT id, name, value FROM test_table WHERE id = ?", 1).Scan(&id, &name, &value)
	if err != nil {
		t.Fatalf("Failed to query record: %v", err)
	}
	if id != 1 || name != "test" || value != 42.5 {
		t.Errorf("Record mismatch: expected (1, 'test', 42.5), got (%d, '%s', %f)", id, name, value)
	}
}

func TestDuckDBSink_BatchInsert(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "duckdb_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	os.Remove(tmpFile.Name())
	defer os.Remove(tmpFile.Name())

	db, err := sql.Open("duckdb", tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test_table (id INTEGER, value VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}

	config := SinkConfig{TableName: "test_table", BatchSize: 3}
	sink := NewSink(context.Background(), db, config, slog.Default())
	if sink == nil {
		t.Fatal("NewSink returned nil")
	}

	records := []Record{
		{"id": 1, "value": "first"},
		{"id": 2, "value": "second"},
		{"id": 3, "value": "third"},
		{"id": 4, "value": "fourth"},
	}

	for _, record := range records {
		sink.In() <- record
	}
	close(sink.In())
	sink.AwaitCompletion()

	// Verify batch processing with overflow
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test_table").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count records: %v", err)
	}
	if count != 4 {
		t.Errorf("Expected 4 records, got %d", count)
	}

	rows, err := db.Query("SELECT id, value FROM test_table ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query records: %v", err)
	}
	defer rows.Close()

	expected := map[int]string{1: "first", 2: "second", 3: "third", 4: "fourth"}
	i := 0
	for rows.Next() {
		var id int
		var value string
		if err := rows.Scan(&id, &value); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		if expectedValue, exists := expected[id]; !exists || expectedValue != value {
			t.Errorf("Record %d mismatch: expected '%s', got '%s'", id, expectedValue, value)
		}
		i++
	}

	if i != 4 {
		t.Errorf("Expected 4 rows, got %d", i)
	}
}

func TestDuckDBSink_NilPointers(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "duckdb_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	os.Remove(tmpFile.Name())
	defer os.Remove(tmpFile.Name())

	db, err := sql.Open("duckdb", tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test_table (id INTEGER, name VARCHAR, optional BOOLEAN)")
	if err != nil {
		t.Fatal(err)
	}

	config := SinkConfig{TableName: "test_table"}
	sink := NewSink(context.Background(), db, config, slog.Default())
	if sink == nil {
		t.Fatal("NewSink returned nil")
	}

	// Test handling of nil pointer values
	type TestStructWithPointers struct {
		ID       *int    `json:"id"`
		Name     *string `json:"name"`
		Optional *bool   `json:"optional"`
	}

	input := TestStructWithPointers{ID: nil, Name: nil, Optional: nil}
	record := Record{"id": input.ID, "name": input.Name, "optional": input.Optional}
	sink.In() <- record
	close(sink.In())
	sink.AwaitCompletion()
}

func TestDuckDBSink_EmptyStruct(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "duckdb_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	os.Remove(tmpFile.Name())
	defer os.Remove(tmpFile.Name())

	db, err := sql.Open("duckdb", tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test_table (id INTEGER)")
	if err != nil {
		t.Fatal(err)
	}

	config := SinkConfig{TableName: "test_table"}
	sink := NewSink(context.Background(), db, config, slog.Default())
	if sink == nil {
		t.Fatal("NewSink returned nil")
	}

	// Test rejection of empty records
	record := Record{}
	sink.In() <- record
	close(sink.In())
	sink.AwaitCompletion()

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test_table").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count records: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected 0 records (empty record should be rejected), got %d", count)
	}
}

func TestDuckDBSink_ErrorChannel(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "duckdb_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	os.Remove(tmpFile.Name())
	defer os.Remove(tmpFile.Name())

	db, err := sql.Open("duckdb", tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test_table (id INTEGER)")
	if err != nil {
		t.Fatal(err)
	}

	config := SinkConfig{TableName: "test_table", EnableErrorChannel: true}
	sink := NewSink(context.Background(), db, config, slog.Default())
	if sink == nil {
		t.Fatal("NewSink returned nil")
	}

	// Verify error channel functionality
	errChan := sink.Errors()
	if errChan == nil {
		t.Error("Expected error channel to be available, got nil")
	}

	record := Record{}
	sink.In() <- record
	close(sink.In())
	sink.AwaitCompletion()

	select {
	case err := <-errChan:
		if err == nil {
			t.Error("Expected error from channel, got nil")
		} else if err.Error() != "empty record" {
			t.Errorf("Expected 'empty record' error, got: %v", err)
		}
	default:
		t.Error("Expected error to be sent to channel, but none received")
	}
}

func TestDuckDBSink_SpecialCharactersInNames(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "duckdb_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	os.Remove(tmpFile.Name())
	defer os.Remove(tmpFile.Name())

	db, err := sql.Open("duckdb", tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE "test-table" ("user-id" INTEGER, "user.name" VARCHAR, "user+tag" VARCHAR, "user_tag" VARCHAR)`)
	if err != nil {
		t.Fatal(err)
	}

	config := SinkConfig{TableName: "test-table"}
	sink := NewSink(context.Background(), db, config, slog.Default())
	if sink == nil {
		t.Fatal("NewSink returned nil")
	}

	// Test SQL identifier quoting with special characters
	record := Record{
		"user-id":   1,
		"user.name": "test@example.com",
		"user+tag":  "special",
		"user_tag":  "normal",
	}

	sink.In() <- record
	close(sink.In())
	sink.AwaitCompletion()
}

func TestDuckDBSink_ConfigurableChannelCapacity(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "duckdb_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	os.Remove(tmpFile.Name())
	defer os.Remove(tmpFile.Name())

	db, err := sql.Open("duckdb", tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test_table (id INTEGER, name VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}

	// Test custom channel capacity configuration
	config := SinkConfig{TableName: "test_table", ChannelCapacity: 50}
	sink := NewSink(context.Background(), db, config, slog.Default())
	if sink == nil {
		t.Fatal("NewSink returned nil")
	}

	records := []Record{{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}}
	for _, record := range records {
		sink.In() <- record
	}
	close(sink.In())
	sink.AwaitCompletion()

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test_table").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count records: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 records, got %d", count)
	}
}

func TestDuckDBSink_ExponentialBackoff(t *testing.T) {
	// Test exponential backoff calculation
	testCases := []struct {
		attempt       int
		initialDelay  time.Duration
		maxDelay      time.Duration
		expectedDelay time.Duration
	}{
		{1, 100 * time.Millisecond, 30 * time.Second, 100 * time.Millisecond},
		{2, 100 * time.Millisecond, 30 * time.Second, 200 * time.Millisecond},
		{3, 100 * time.Millisecond, 30 * time.Second, 400 * time.Millisecond},
		{4, 100 * time.Millisecond, 30 * time.Second, 800 * time.Millisecond},
		{10, 100 * time.Millisecond, 30 * time.Second, 30 * time.Second}, // capped at max
	}

	for _, tc := range testCases {
		actualDelay := calculateRetryDelay(tc.attempt, tc.initialDelay, tc.maxDelay)
		if actualDelay != tc.expectedDelay {
			t.Errorf("calculateRetryDelay(%d, %v, %v) = %v, expected %v",
				tc.attempt, tc.initialDelay, tc.maxDelay, actualDelay, tc.expectedDelay)
		}
	}
}

func TestDuckDBSink_ContextCancellation(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "duckdb_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	os.Remove(tmpFile.Name())
	defer os.Remove(tmpFile.Name())

	db, err := sql.Open("duckdb", tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec("CREATE TABLE test_table (id INTEGER, name VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}

	// Test graceful shutdown via context cancellation
	config := SinkConfig{TableName: "test_table", BatchSize: 10}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sink := NewSink(ctx, db, config, slog.Default())
	if sink == nil {
		t.Fatal("NewSink returned nil")
	}
	defer sink.AwaitCompletion()

	// Send records before cancellation
	for i := 0; i < 5; i++ {
		sink.In() <- Record{"id": i, "name": "test"}
	}

	cancel()
	close(sink.In())
	sink.AwaitCompletion()
}
