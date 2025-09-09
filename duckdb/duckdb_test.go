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
