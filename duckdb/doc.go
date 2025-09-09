// Package duckdb provides a DuckDB sink connector for the go-streams library.
//
// The DuckDB sink allows streaming data pipelines to efficiently write data
// to DuckDB databases. It supports various data types including maps, structs,
// and JSON, with configurable batching for optimal performance.
//
// Example usage:
//
//	import (
//		"context"
//		"database/sql"
//		"log/slog"
//
//		"github.com/reugn/go-streams"
//		"github.com/reugn/go-streams/duckdb"
//		_ "github.com/marcboeker/go-duckdb"
//	)
//
//	// Open database connection (caller manages connection)
//	db, err := sql.Open("duckdb", "path/to/database.db")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer db.Close()
//
//	// Create table if it doesn't exist
//	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS events (id INTEGER, data VARCHAR)`)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	config := duckdb.SinkConfig{
//		TableName:         "events",
//		BatchSize:         100,                    // Batch every 100 records
//		MaxRetries:        3,                      // Retry failed operations up to 3 times
//		ChannelCapacity:   200,                    // Input channel capacity (BatchSize*2 default)
//		InitialRetryDelay: 100 * time.Millisecond, // Initial retry delay (100ms default)
//		MaxRetryDelay:     30 * time.Second,       // Maximum retry delay (30s default)
//	}
//
//	sink := duckdb.NewSink(context.Background(), db, config, slog.Default())
//	source.Via(flow).To(sink)
//	sink.AwaitCompletion()
package duckdb
