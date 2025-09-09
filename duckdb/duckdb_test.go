package duckdb

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"testing"

	"github.com/reugn/go-streams"
)

func TestDuckDBSink_Interface(t *testing.T) {
	// Create a temporary database file path for testing
	tmpFile, err := os.CreateTemp("", "duckdb_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	// Remove the file so DuckDB can create it fresh
	os.Remove(tmpFile.Name())
	defer os.Remove(tmpFile.Name())

	// Open database connection
	db, err := sql.Open("duckdb", tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create table manually
	_, err = db.Exec("CREATE TABLE test_table (id INTEGER)")
	if err != nil {
		t.Fatal(err)
	}

	config := SinkConfig{
		TableName: "test_table",
	}

	sink := NewSink(context.Background(), db, config, nil)
	if sink == nil {
		t.Fatal("NewSink returned nil")
	}

	// Test interface compliance
	var _ streams.Sink = sink

	// Test channels
	in := sink.In()
	if in == nil {
		t.Error("In() returned nil channel")
	}

	// Close input channel to trigger completion
	close(in)
	sink.AwaitCompletion()
}

func TestDuckDBSink_BasicInsert(t *testing.T) {
	// Create a temporary database file path for testing
	tmpFile, err := os.CreateTemp("", "duckdb_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	// Remove the file so DuckDB can create it fresh
	os.Remove(tmpFile.Name())
	defer os.Remove(tmpFile.Name())

	// Open database connection
	db, err := sql.Open("duckdb", tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create table manually
	_, err = db.Exec("CREATE TABLE test_table (id INTEGER, name VARCHAR, value DOUBLE)")
	if err != nil {
		t.Fatal(err)
	}

	config := SinkConfig{
		TableName: "test_table",
	}

	sink := NewSink(context.Background(), db, config, slog.Default())
	if sink == nil {
		t.Fatal("NewSink returned nil")
	}

	// Test inserting a record
	record := Record{
		"id":    1,
		"name":  "test",
		"value": 42.5,
	}

	sink.In() <- record

	// Close and wait for completion
	close(sink.In())
	sink.AwaitCompletion()

	// Verify the record was inserted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test_table").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count records: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1 record, got %d", count)
	}

	// Verify the record content
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
	// Create a temporary database file path for testing
	tmpFile, err := os.CreateTemp("", "duckdb_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	// Remove the file so DuckDB can create it fresh
	os.Remove(tmpFile.Name())
	defer os.Remove(tmpFile.Name())

	// Open database connection
	db, err := sql.Open("duckdb", tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create table manually
	_, err = db.Exec("CREATE TABLE test_table (id INTEGER, value VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}

	config := SinkConfig{
		TableName: "test_table",
		BatchSize: 3,
	}

	sink := NewSink(context.Background(), db, config, slog.Default())
	if sink == nil {
		t.Fatal("NewSink returned nil")
	}

	// Send multiple records
	records := []Record{
		{"id": 1, "value": "first"},
		{"id": 2, "value": "second"},
		{"id": 3, "value": "third"},
		{"id": 4, "value": "fourth"},
	}

	for _, record := range records {
		sink.In() <- record
	}

	// Close and wait for completion
	close(sink.In())
	sink.AwaitCompletion()

	// Verify all records were inserted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test_table").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count records: %v", err)
	}
	if count != 4 {
		t.Errorf("Expected 4 records, got %d", count)
	}

	// Verify specific records
	rows, err := db.Query("SELECT id, value FROM test_table ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query records: %v", err)
	}
	defer rows.Close()

	expected := map[int]string{
		1: "first",
		2: "second",
		3: "third",
		4: "fourth",
	}

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
	// Create a temporary database file path for testing
	tmpFile, err := os.CreateTemp("", "duckdb_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	os.Remove(tmpFile.Name())
	defer os.Remove(tmpFile.Name())

	// Open database connection
	db, err := sql.Open("duckdb", tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create table manually
	_, err = db.Exec("CREATE TABLE test_table (id INTEGER, name VARCHAR, optional BOOLEAN)")
	if err != nil {
		t.Fatal(err)
	}

	config := SinkConfig{
		TableName: "test_table",
	}

	sink := NewSink(context.Background(), db, config, slog.Default())
	if sink == nil {
		t.Fatal("NewSink returned nil")
	}

	type TestStructWithPointers struct {
		ID       *int    `json:"id"`
		Name     *string `json:"name"`
		Optional *bool   `json:"optional"`
	}

	// Test with nil pointers
	input := TestStructWithPointers{
		ID:       nil,
		Name:     nil,
		Optional: nil,
	}

	// Convert struct to Record (map[string]any) as expected by the sink
	record := Record{
		"id":       input.ID,
		"name":     input.Name,
		"optional": input.Optional,
	}
	sink.In() <- record

	// Close and wait for completion
	close(sink.In())
	sink.AwaitCompletion()
}

func TestDuckDBSink_EmptyStruct(t *testing.T) {
	// Create a temporary database file path for testing
	tmpFile, err := os.CreateTemp("", "duckdb_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	os.Remove(tmpFile.Name())
	defer os.Remove(tmpFile.Name())

	// Open database connection
	db, err := sql.Open("duckdb", tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create table manually
	_, err = db.Exec("CREATE TABLE test_table (id INTEGER)")
	if err != nil {
		t.Fatal(err)
	}

	config := SinkConfig{
		TableName: "test_table",
	}

	sink := NewSink(context.Background(), db, config, slog.Default())
	if sink == nil {
		t.Fatal("NewSink returned nil")
	}

	type EmptyStruct struct{}

	// Convert empty struct to empty Record (map[string]any) as expected by the sink
	record := Record{}
	sink.In() <- record

	// Close and wait for completion
	close(sink.In())
	sink.AwaitCompletion()

	// Verify no records were inserted (empty record should be rejected)
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
	// Create a temporary database file path for testing
	tmpFile, err := os.CreateTemp("", "duckdb_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	os.Remove(tmpFile.Name())
	defer os.Remove(tmpFile.Name())

	// Open database connection
	db, err := sql.Open("duckdb", tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create table manually
	_, err = db.Exec("CREATE TABLE test_table (id INTEGER)")
	if err != nil {
		t.Fatal(err)
	}

	config := SinkConfig{
		TableName:          "test_table",
		EnableErrorChannel: true,
	}

	sink := NewSink(context.Background(), db, config, slog.Default())
	if sink == nil {
		t.Fatal("NewSink returned nil")
	}

	// Test that error channel is available
	errChan := sink.Errors()
	if errChan == nil {
		t.Error("Expected error channel to be available, got nil")
	}

	// Send an empty record which should cause an error
	record := Record{}
	sink.In() <- record

	// Close and wait for completion
	close(sink.In())
	sink.AwaitCompletion()

	// Check if error was sent to channel
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
	// Create a temporary database file path for testing
	tmpFile, err := os.CreateTemp("", "duckdb_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	os.Remove(tmpFile.Name())
	defer os.Remove(tmpFile.Name())

	// Open database connection
	db, err := sql.Open("duckdb", tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create table manually with quoted name
	_, err = db.Exec(`CREATE TABLE "test-table" ("user-id" INTEGER, "user.name" VARCHAR, "user+tag" VARCHAR, "user_tag" VARCHAR)`)
	if err != nil {
		t.Fatal(err)
	}

	config := SinkConfig{
		TableName: "test-table", // Special characters in table name
	}

	sink := NewSink(context.Background(), db, config, slog.Default())
	if sink == nil {
		t.Fatal("NewSink returned nil")
	}

	// Test with special characters in field names
	record := Record{
		"user-id":   1,
		"user.name": "test@example.com",
		"user+tag":  "special",
		"user_tag":  "normal",
	}

	sink.In() <- record

	// Close and wait for completion
	close(sink.In())
	sink.AwaitCompletion()
}

func TestDuckDBSink_ContextCancellation(t *testing.T) {
	// Create a temporary database file path for testing
	tmpFile, err := os.CreateTemp("", "duckdb_test_*.db")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	os.Remove(tmpFile.Name())
	defer os.Remove(tmpFile.Name())

	// Open database connection
	db, err := sql.Open("duckdb", tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create table manually
	_, err = db.Exec("CREATE TABLE test_table (id INTEGER, name VARCHAR)")
	if err != nil {
		t.Fatal(err)
	}

	config := SinkConfig{
		TableName: "test_table",
		BatchSize: 10, // Use batching to test buffer flushing on cancellation
	}

	// Create a cancellable context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sink := NewSink(ctx, db, config, slog.Default())
	if sink == nil {
		t.Fatal("NewSink returned nil")
	}
	defer sink.AwaitCompletion()

	// Send a few records
	for i := 0; i < 5; i++ {
		record := Record{
			"id":   i,
			"name": "test",
		}
		sink.In() <- record
	}

	// Cancel the context (simulating graceful shutdown)
	cancel()

	// Close the input channel
	close(sink.In())

	// Wait for completion - should handle cancellation gracefully
	sink.AwaitCompletion()
}
