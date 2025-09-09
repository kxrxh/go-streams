package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"
)

func BenchmarkDuckDBSink_SingleInsert(b *testing.B) {
	// Create a temporary database file for benchmarking
	tmpFile, err := os.CreateTemp("", "bench_duckdb_*.db")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()
	os.Remove(tmpFile.Name()) // Remove so DuckDB can create fresh

	// Open database connection
	db, err := sql.Open("duckdb", tmpFile.Name())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Create table manually
	_, err = db.Exec("CREATE TABLE bench_single (id INTEGER, name VARCHAR, value DOUBLE, active BOOLEAN, timestamp TIMESTAMP)")
	if err != nil {
		b.Fatal(err)
	}

	config := SinkConfig{
		TableName: "bench_single",
		BatchSize: 1, // Force single inserts
	}

	sink := NewSink(context.Background(), db, config, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))
	if sink == nil {
		b.Fatal("NewSink returned nil")
	}
	defer sink.AwaitCompletion()

	// Prepare test data
	record := Record{
		"id":        1,
		"name":      "benchmark_test",
		"value":     42.5,
		"timestamp": time.Now(),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Update the ID for each iteration to avoid primary key conflicts
		record["id"] = i
		sink.In() <- record
	}

	close(sink.In())
}

func BenchmarkDuckDBSink_BatchInsert(b *testing.B) {
	// Create a temporary database file for benchmarking
	tmpFile, err := os.CreateTemp("", "bench_duckdb_batch_*.db")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()
	os.Remove(tmpFile.Name())

	// Open database connection
	db, err := sql.Open("duckdb", tmpFile.Name())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Create table manually
	_, err = db.Exec("CREATE TABLE bench_batch (id INTEGER, name VARCHAR, value DOUBLE, active BOOLEAN, timestamp TIMESTAMP)")
	if err != nil {
		b.Fatal(err)
	}

	config := SinkConfig{
		TableName: "bench_batch",
		BatchSize: 100,
	}

	sink := NewSink(context.Background(), db, config, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))
	if sink == nil {
		b.Fatal("NewSink returned nil")
	}
	defer sink.AwaitCompletion()

	// Prepare test data
	baseRecord := Record{
		"name":      "benchmark_test",
		"value":     42.5,
		"timestamp": time.Now(),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		record := make(Record)
		for k, v := range baseRecord {
			record[k] = v
		}
		record["id"] = i
		sink.In() <- record
	}

	close(sink.In())
}

func BenchmarkDuckDBSink_InMemory(b *testing.B) {
	// Open database connection
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Create table manually
	_, err = db.Exec("CREATE TABLE bench_memory (id INTEGER, name VARCHAR, value DOUBLE, active BOOLEAN, timestamp TIMESTAMP)")
	if err != nil {
		b.Fatal(err)
	}

	config := SinkConfig{
		TableName: "bench_memory",
		BatchSize: 50,
	}

	sink := NewSink(context.Background(), db, config, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))
	if sink == nil {
		b.Fatal("NewSink returned nil")
	}
	defer sink.AwaitCompletion()

	baseRecord := Record{
		"id":        1,
		"name":      "memory_test",
		"value":     123.45,
		"timestamp": time.Now(),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Create a copy of the record to avoid concurrent map access
		record := make(Record, len(baseRecord))
		for k, v := range baseRecord {
			record[k] = v
		}
		record["id"] = i
		sink.In() <- record
	}

	close(sink.In())
}

func BenchmarkDuckDBSink_StructData(b *testing.B) {
	// Create a temporary database file for benchmarking
	tmpFile, err := os.CreateTemp("", "bench_duckdb_struct_*.db")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()
	os.Remove(tmpFile.Name())

	// Open database connection
	db, err := sql.Open("duckdb", tmpFile.Name())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Create table manually
	_, err = db.Exec("CREATE TABLE bench_struct (id INTEGER, name VARCHAR, value DOUBLE, active BOOLEAN, created_at TIMESTAMP)")
	if err != nil {
		b.Fatal(err)
	}

	config := SinkConfig{
		TableName: "bench_struct",
		BatchSize: 25,
	}

	sink := NewSink(context.Background(), db, config, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))
	if sink == nil {
		b.Fatal("NewSink returned nil")
	}
	defer sink.AwaitCompletion()

	type BenchmarkStruct struct {
		ID        int       `json:"id"`
		Name      string    `json:"name"`
		Value     float64   `json:"value"`
		Active    bool      `json:"active"`
		CreatedAt time.Time `json:"created_at"`
	}

	data := BenchmarkStruct{
		Name:      "struct_benchmark",
		Value:     99.99,
		Active:    true,
		CreatedAt: time.Now(),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		data.ID = i
		// Convert struct to Record (map[string]any) as expected by the sink
		record := Record{
			"id":         data.ID,
			"name":       data.Name,
			"value":      data.Value,
			"active":     data.Active,
			"created_at": data.CreatedAt,
		}
		sink.In() <- record
	}

	close(sink.In())
}

func BenchmarkDuckDBSink_ConcurrentInserts(b *testing.B) {
	// Create a temporary database file for benchmarking
	tmpFile, err := os.CreateTemp("", "bench_duckdb_concurrent_*.db")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()
	os.Remove(tmpFile.Name())

	// Open database connection
	db, err := sql.Open("duckdb", tmpFile.Name())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Create table manually
	_, err = db.Exec("CREATE TABLE bench_concurrent (id INTEGER, worker INTEGER, name VARCHAR, value DOUBLE, timestamp TIMESTAMP)")
	if err != nil {
		b.Fatal(err)
	}

	config := SinkConfig{
		TableName: "bench_concurrent",
		BatchSize: 50,
	}

	sink := NewSink(context.Background(), db, config, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))
	if sink == nil {
		b.Fatal("NewSink returned nil")
	}
	defer sink.AwaitCompletion()

	// Use multiple goroutines to simulate concurrent inserts
	numWorkers := 10
	recordsPerWorker := b.N / numWorkers

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			startID := workerID * recordsPerWorker

			for i := 0; i < recordsPerWorker; i++ {
				record := Record{
					"id":        startID + i,
					"worker":    workerID,
					"name":      fmt.Sprintf("worker_%d_record_%d", workerID, i),
					"value":     float64(i) * 1.5,
					"timestamp": time.Now(),
				}
				sink.In() <- record
			}
		}(w)
	}

	wg.Wait()
	close(sink.In())
}

func BenchmarkDuckDBSink_LargeRecords(b *testing.B) {
	// Create a temporary database file for benchmarking
	tmpFile, err := os.CreateTemp("", "bench_duckdb_large_*.db")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()
	os.Remove(tmpFile.Name())

	// Open database connection
	db, err := sql.Open("duckdb", tmpFile.Name())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Create table manually with many columns
	_, err = db.Exec(`
		CREATE TABLE bench_large (
			id INTEGER, name VARCHAR, email VARCHAR, address VARCHAR, phone VARCHAR,
			city VARCHAR, state VARCHAR, zip VARCHAR, country VARCHAR, notes VARCHAR
		)
	`)
	if err != nil {
		b.Fatal(err)
	}

	config := SinkConfig{
		TableName: "bench_large",
		BatchSize: 10, // Smaller batch for large records
	}

	sink := NewSink(context.Background(), db, config, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))
	if sink == nil {
		b.Fatal("NewSink returned nil")
	}
	defer sink.AwaitCompletion()

	// Create a large record with many fields (using only the columns defined in the table)
	baseRecord := make(Record)
	baseRecord["id"] = 1
	baseRecord["name"] = "large_record_benchmark"
	baseRecord["email"] = "benchmark@example.com"
	baseRecord["address"] = "123 Benchmark Street, Suite 100"
	baseRecord["phone"] = "+1-555-0123"
	baseRecord["city"] = "Benchmark City"
	baseRecord["state"] = "BS"
	baseRecord["zip"] = "12345"
	baseRecord["country"] = "Benchmark Country"

	// Add large text content to simulate complex data
	baseRecord["notes"] = "This is a large notes field with lots of text to simulate real-world data. " +
		"Multiple sentences here to make it larger. " +
		"Additional content for testing purposes. " +
		"More text to increase the size of this field significantly. " +
		"Continuing to add more content to make this a truly large record for benchmarking purposes."

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Create a copy of the record to avoid concurrent map access
		recordCopy := make(Record, len(baseRecord))
		for k, v := range baseRecord {
			recordCopy[k] = v
		}
		recordCopy["id"] = i
		sink.In() <- recordCopy
	}

	close(sink.In())
}

func BenchmarkDuckDBSink_TypeInference(b *testing.B) {
	// Open database connection
	db, err := sql.Open("duckdb", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Create table manually with columns for type inference testing
	_, err = db.Exec("CREATE TABLE bench_types (id INTEGER, name VARCHAR, value DOUBLE, active BOOLEAN, timestamp TIMESTAMP, int_val INTEGER, int64_val BIGINT, float_val DOUBLE, bool_val BOOLEAN, string_val VARCHAR, time_val TIMESTAMP)")
	if err != nil {
		b.Fatal(err)
	}

	config := SinkConfig{
		TableName: "bench_types",
		BatchSize: 1,
	}

	sink := NewSink(context.Background(), db, config, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))
	if sink == nil {
		b.Fatal("NewSink returned nil")
	}
	defer sink.AwaitCompletion()

	// Test different data types - use consistent schema for all records
	testRecords := []Record{
		{"id": 1, "int_val": int(42), "int64_val": int64(9223372036854775807), "float_val": 3.14159, "bool_val": true, "string_val": "test", "time_val": time.Now()},
		{"id": 2, "int_val": int32(123), "int64_val": int64(456), "float_val": float32(2.718), "bool_val": false, "string_val": "another test", "time_val": time.Now()},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		record := make(Record)
		for k, v := range testRecords[i%len(testRecords)] {
			record[k] = v
		}
		record["id"] = i
		sink.In() <- record
	}

	close(sink.In())
}

// Benchmark to compare batch sizes
func BenchmarkDuckDBSink_BatchSizeComparison(b *testing.B) {
	batchSizes := []int{1, 10, 50, 100, 500}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
			// Create a temporary database file for benchmarking
			tmpFile, err := os.CreateTemp("", fmt.Sprintf("bench_batch_%d_*.db", batchSize))
			if err != nil {
				b.Fatal(err)
			}
			defer os.Remove(tmpFile.Name())
			tmpFile.Close()
			os.Remove(tmpFile.Name())

			// Open database connection
			db, err := sql.Open("duckdb", tmpFile.Name())
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			// Create table manually
			_, err = db.Exec(fmt.Sprintf("CREATE TABLE bench_batch_%d (id INTEGER, name VARCHAR, value DOUBLE, active BOOLEAN, timestamp TIMESTAMP)", batchSize))
			if err != nil {
				b.Fatal(err)
			}

			config := SinkConfig{
				TableName: fmt.Sprintf("bench_batch_%d", batchSize),
				BatchSize: batchSize,
			}

			sink := NewSink(context.Background(), db, config, slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})))
			if sink == nil {
				b.Fatal("NewSink returned nil")
			}
			defer sink.AwaitCompletion()

			baseRecord := Record{
				"id":        1,
				"name":      "batch_test",
				"value":     42.0,
				"timestamp": time.Now(),
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Create a copy of the record to avoid concurrent map access
				recordCopy := make(Record, len(baseRecord))
				for k, v := range baseRecord {
					recordCopy[k] = v
				}
				recordCopy["id"] = i
				sink.In() <- recordCopy
			}

			close(sink.In())
		})
	}
}
