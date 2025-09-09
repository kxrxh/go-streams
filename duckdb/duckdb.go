package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/reugn/go-streams"
)

// SinkConfig contains the configuration for the DuckDB sink connector.
type SinkConfig struct {
	// TableName specifies the target table name.
	TableName string
	// BatchSize controls the size of the batch when writing records.
	// Default is 1 (no batching).
	BatchSize int
	// MaxRetries specifies the maximum number of retries for failed operations.
	// Default is 3.
	MaxRetries int
}

// Record represents a single record to be inserted into DuckDB.
// It contains the data as a map of column names to values.
type Record map[string]any

// DuckDBSink represents a DuckDB sink connector.
type DuckDBSink struct {
	db     *sql.DB
	config SinkConfig
	in     chan any
	done   chan struct{}
	logger *slog.Logger

	// context and cancellation
	ctx    context.Context
	cancel context.CancelFunc

	// batching fields
	buffer []Record
}

var _ streams.Sink = (*DuckDBSink)(nil)

// quoteIdentifier safely quotes a SQL identifier to prevent SQL injection.
// Uses DuckDB's double-quote syntax for identifiers.
func quoteIdentifier(identifier string) string {
	// Escape any internal double quotes by doubling them
	escaped := strings.ReplaceAll(identifier, `"`, `""`)

	// Wrap in double quotes - this is the safest way to handle identifiers
	return `"` + escaped + `"`
}

// NewSink returns a new DuckDBSink connector.
// The database connection must be managed by the caller.
// If ctx is nil, context.Background() will be used.
func NewSink(ctx context.Context, db *sql.DB, config SinkConfig, logger ...*slog.Logger) *DuckDBSink {
	var log *slog.Logger
	if len(logger) > 0 && logger[0] != nil {
		log = logger[0].With(slog.Group("connector",
			slog.String("name", "duckdb"),
			slog.String("type", "sink")))
	} else {
		log = slog.New(slog.DiscardHandler) // Disabled logger by default
	}

	// Set defaults
	if config.BatchSize < 1 {
		config.BatchSize = 1
	}
	if config.MaxRetries < 1 {
		config.MaxRetries = 3
	}

	// Handle context
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)

	sink := &DuckDBSink{
		db:     db,
		config: config,
		in:     make(chan any, config.BatchSize*2),
		done:   make(chan struct{}),
		logger: log,
		ctx:    ctx,
		cancel: cancel,
	}

	if config.BatchSize > 1 {
		sink.buffer = make([]Record, 0, config.BatchSize)
	}

	// Start processing incoming data
	go sink.processStream()

	return sink
}

// processStream processes the incoming data stream.
func (d *DuckDBSink) processStream() {
	defer close(d.done)

	for {
		select {
		case msg, ok := <-d.in:
			if !ok {
				// Channel closed, flush remaining records and exit
				d.flushBuffer()
				return
			}
			if err := d.processMessage(msg); err != nil {
				d.logger.Error("Failed to process message", slog.Any("error", err))
				// Stop on first error for simplicity
				d.flushBuffer()
				return
			}
		case <-d.ctx.Done():
			d.logger.Info("Context cancelled, stopping processing", slog.Any("error", d.ctx.Err()))
			// Flush any remaining records before exiting
			d.flushBuffer()
			return
		}
	}
}

// processMessage handles individual incoming messages.
func (d *DuckDBSink) processMessage(msg any) error {
	var record Record

	switch v := msg.(type) {
	case Record:
		record = v
	case map[string]any:
		record = Record(v)
	default:
		return fmt.Errorf("unsupported message type %T, expected Record or map[string]any", msg)
	}

	return d.insertRecord(record)
}

// insertRecord inserts a single record into DuckDB.
func (d *DuckDBSink) insertRecord(record Record) error {
	if d.config.BatchSize > 1 {
		// Add to batch
		d.buffer = append(d.buffer, record)
		if len(d.buffer) >= d.config.BatchSize {
			return d.flushBuffer()
		}
		return nil
	}

	// Insert immediately
	return d.insertSingleRecord(record)
}

// insertSingleRecord performs a single record insert.
func (d *DuckDBSink) insertSingleRecord(record Record) error {
	if len(record) == 0 {
		return fmt.Errorf("empty record")
	}

	return d.insertRecordSimple(record)
}

// insertRecordSimple performs a simple record insert.
func (d *DuckDBSink) insertRecordSimple(record Record) error {
	// Build column names and placeholders
	var columns []string
	var placeholders []string
	var values []any

	for col, val := range record {
		columns = append(columns, quoteIdentifier(col))
		placeholders = append(placeholders, "?")
		values = append(values, val)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quoteIdentifier(d.config.TableName),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	// Retry logic
	var lastErr error
	for attempt := 0; attempt <= d.config.MaxRetries; attempt++ {
		_, err := d.db.Exec(query, values...)
		if err == nil {
			return nil
		}

		lastErr = err
		if attempt < d.config.MaxRetries {
			d.logger.Warn("Insert failed, retrying",
				slog.Int("attempt", attempt+1),
				slog.Any("error", err))
			time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
		}
	}

	return fmt.Errorf("failed to insert record after %d attempts: %w", d.config.MaxRetries+1, lastErr)
}

// flushBuffer flushes all buffered records in a batch.
func (d *DuckDBSink) flushBuffer() error {
	if len(d.buffer) == 0 {
		return nil
	}

	d.logger.Debug("Flushing batch", slog.Int("size", len(d.buffer)))

	// Use transaction for atomic batch operations
	tx, err := d.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	for _, record := range d.buffer {
		if err := d.insertRecordInTransaction(tx, record); err != nil {
			return err
		}
	}

	// Clear buffer
	d.buffer = d.buffer[:0]

	return tx.Commit()
}

// insertRecordInTransaction inserts a record within a transaction.
func (d *DuckDBSink) insertRecordInTransaction(tx *sql.Tx, record Record) error {
	// Build column names and placeholders
	var columns []string
	var placeholders []string
	var values []any

	for col, val := range record {
		columns = append(columns, quoteIdentifier(col))
		placeholders = append(placeholders, "?")
		values = append(values, val)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quoteIdentifier(d.config.TableName),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	_, err := tx.Exec(query, values...)
	return err
}

// In returns the input channel of the DuckDBSink connector.
func (d *DuckDBSink) In() chan<- any {
	return d.in
}

// AwaitCompletion blocks until the DuckDBSink has processed all received data.
func (d *DuckDBSink) AwaitCompletion() {
	<-d.done
}

// Close gracefully shuts down the DuckDBSink by cancelling the context.
// This will cause the processing goroutine to stop and flush any remaining records.
// After calling Close, no more data should be sent to the input channel.
func (d *DuckDBSink) Close() error {
	if d.cancel != nil {
		d.cancel()
	}
	return nil
}
