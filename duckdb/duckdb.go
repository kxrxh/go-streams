package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"sort"
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
	// EnableErrorChannel enables error reporting through an error channel.
	// Default is false.
	EnableErrorChannel bool
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

	// error reporting
	errChan chan error
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
		// Use a default logger that writes to stderr with INFO level
		log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})).With(slog.Group("connector",
			slog.String("name", "duckdb"),
			slog.String("type", "sink")))
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

	if config.EnableErrorChannel {
		sink.errChan = make(chan error, 1) // Buffered channel to avoid blocking
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
				if err := d.flushBuffer(); err != nil && d.errChan != nil {
					select {
					case d.errChan <- err:
					default:
					}
				}
				return
			}
			if err := d.processMessage(msg); err != nil {
				d.logger.Error("Failed to process message", slog.Any("error", err))
				// Send error to channel if enabled
				if d.errChan != nil {
					select {
					case d.errChan <- err:
					default:
					}
				}
				// Stop on first error for simplicity
				if flushErr := d.flushBuffer(); flushErr != nil && d.errChan != nil {
					select {
					case d.errChan <- flushErr:
					default:
					}
				}
				return
			}
		case <-d.ctx.Done():
			d.logger.Info("Context cancelled, stopping processing", slog.Any("error", d.ctx.Err()))
			// Flush any remaining records before exiting
			if err := d.flushBuffer(); err != nil && d.errChan != nil {
				select {
				case d.errChan <- err:
				default:
				}
			}
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

	// Retry logic for batch operations
	var lastErr error
	for attempt := 0; attempt <= d.config.MaxRetries; attempt++ {
		// Use transaction for atomic batch operations
		tx, err := d.db.Begin()
		if err != nil {
			lastErr = fmt.Errorf("failed to begin transaction: %w", err)
			if attempt < d.config.MaxRetries {
				d.logger.Warn("Batch transaction begin failed, retrying",
					slog.Int("attempt", attempt+1),
					slog.Any("error", err))
				time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
				continue
			}
			return lastErr
		}
		defer tx.Rollback()

		// Try optimized multi-row insert if all records have the same schema
		batchErr := d.insertBatchOptimized(tx)

		if batchErr != nil {
			lastErr = batchErr
			if attempt < d.config.MaxRetries {
				d.logger.Warn("Batch insert failed, retrying",
					slog.Int("attempt", attempt+1),
					slog.Any("error", batchErr))
				time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
				continue
			}
			return fmt.Errorf("failed to flush batch after %d attempts: %w", d.config.MaxRetries+1, lastErr)
		}

		// Commit the transaction
		if err := tx.Commit(); err != nil {
			lastErr = fmt.Errorf("failed to commit transaction: %w", err)
			if attempt < d.config.MaxRetries {
				d.logger.Warn("Batch commit failed, retrying",
					slog.Int("attempt", attempt+1),
					slog.Any("error", err))
				time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
				continue
			}
			return fmt.Errorf("failed to commit batch after %d attempts: %w", d.config.MaxRetries+1, lastErr)
		}

		// Success - clear buffer and return
		d.buffer = d.buffer[:0]
		return nil
	}

	return fmt.Errorf("failed to flush batch after %d attempts: %w", d.config.MaxRetries+1, lastErr)
}

// insertBatchOptimized attempts to insert all records in a batch using optimized methods.
// If all records have the same column schema, uses a single multi-row INSERT.
// Otherwise, falls back to individual inserts.
func (d *DuckDBSink) insertBatchOptimized(tx *sql.Tx) error {
	if len(d.buffer) == 0 {
		return nil
	}

	// Check if all records have the same column set
	firstRecord := d.buffer[0]
	firstColumns := make([]string, 0, len(firstRecord))
	for col := range firstRecord {
		firstColumns = append(firstColumns, col)
	}

	// Sort columns for consistent ordering
	sort.Strings(firstColumns)

	// Check if all records have the same columns
	allSameSchema := true
	for _, record := range d.buffer[1:] {
		recordColumns := make([]string, 0, len(record))
		for col := range record {
			recordColumns = append(recordColumns, col)
		}
		sort.Strings(recordColumns)

		if len(recordColumns) != len(firstColumns) {
			allSameSchema = false
			break
		}
		for i, col := range recordColumns {
			if col != firstColumns[i] {
				allSameSchema = false
				break
			}
		}
		if !allSameSchema {
			break
		}
	}

	if allSameSchema && len(d.buffer) > 1 {
		// Use optimized multi-row insert
		return d.insertBatchMultiRow(tx, firstColumns)
	}

	// Fall back to individual inserts
	for _, record := range d.buffer {
		if err := d.insertRecordInTransaction(tx, record); err != nil {
			return err
		}
	}

	return nil
}

// insertBatchMultiRow performs a single multi-row INSERT for better performance
func (d *DuckDBSink) insertBatchMultiRow(tx *sql.Tx, columns []string) error {
	if len(d.buffer) == 0 || len(columns) == 0 {
		return nil
	}

	// Build column names
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = quoteIdentifier(col)
	}

	// Build placeholders for all rows
	var allPlaceholders []string
	var allValues []any

	for _, record := range d.buffer {
		var rowPlaceholders []string
		for _, col := range columns {
			rowPlaceholders = append(rowPlaceholders, "?")
			if val, exists := record[col]; exists {
				allValues = append(allValues, val)
			} else {
				allValues = append(allValues, nil) // NULL for missing columns
			}
		}
		allPlaceholders = append(allPlaceholders, "("+strings.Join(rowPlaceholders, ", ")+")")
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		quoteIdentifier(d.config.TableName),
		strings.Join(quotedColumns, ", "),
		strings.Join(allPlaceholders, ", "))

	_, err := tx.Exec(query, allValues...)
	return err
}

// insertRecordInTransaction inserts a record within a transaction.
func (d *DuckDBSink) insertRecordInTransaction(tx *sql.Tx, record Record) error {
	if len(record) == 0 {
		return fmt.Errorf("empty record")
	}

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

// Errors returns the error channel for error reporting.
// Returns nil if error reporting is not enabled.
func (d *DuckDBSink) Errors() <-chan error {
	return d.errChan
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
	if d.errChan != nil {
		close(d.errChan)
	}
	return nil
}
