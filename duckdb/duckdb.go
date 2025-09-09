package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/reugn/go-streams"
)

// SinkConfig holds configuration for the DuckDB sink.
type SinkConfig struct {
	TableName          string        // Target table name
	BatchSize          int           // Records per batch (1 = no batching)
	MaxRetries         int           // Max retry attempts (3 default)
	EnableErrorChannel bool          // Enable error reporting channel
	ChannelCapacity    int           // Input channel capacity (BatchSize*2 default)
	InitialRetryDelay  time.Duration // Initial delay for retry backoff (100ms default)
	MaxRetryDelay      time.Duration // Maximum delay for retry backoff (30s default)
}

// Record represents a database row as column-value pairs.
type Record map[string]any

// DuckDBSink provides a sink for writing records to DuckDB.
type DuckDBSink struct {
	db     *sql.DB
	config SinkConfig
	in     chan any
	done   chan struct{}
	logger *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc

	buffer  []Record
	errChan chan error
}

var _ streams.Sink = (*DuckDBSink)(nil)

// Object pools for reducing memory allocations
var (
	stringSlicePool = sync.Pool{
		New: func() any { return make([]string, 0, 16) },
	}
	stringBuilderPool = sync.Pool{
		New: func() any { return &strings.Builder{} },
	}
	interfaceSlicePool = sync.Pool{
		New: func() any { return make([]any, 0, 16) },
	}
)

// quoteIdentifier quotes SQL identifiers using DuckDB's double-quote syntax.
func quoteIdentifier(identifier string) string {
	if !strings.Contains(identifier, `"`) {
		return `"` + identifier + `"`
	}

	builder := stringBuilderPool.Get().(*strings.Builder)
	defer func() {
		builder.Reset()
		stringBuilderPool.Put(builder)
	}()

	builder.Grow(len(identifier) + 2)
	builder.WriteByte('"')

	for _, r := range identifier {
		builder.WriteRune(r)
		if r == '"' {
			builder.WriteByte('"')
		}
	}

	builder.WriteByte('"')
	return builder.String()
}

// calculateRetryDelay computes exponential backoff delay with jitter.
// Uses exponential backoff: initialDelay * 2^(attempt-1), capped at maxDelay.
func calculateRetryDelay(attempt int, initialDelay, maxDelay time.Duration) time.Duration {
	if attempt <= 1 {
		return initialDelay
	}

	// Calculate exponential delay: initialDelay * 2^(attempt-1)
	delay := initialDelay * time.Duration(1<<(attempt-1))

	// Cap at maximum delay
	if delay > maxDelay {
		delay = maxDelay
	}

	return delay
}

// NewSink creates a new DuckDB sink.
// Caller must manage the database connection.
// Uses context.Background() if ctx is nil.
func NewSink(ctx context.Context, db *sql.DB, config SinkConfig, logger ...*slog.Logger) *DuckDBSink {
	var log *slog.Logger
	if len(logger) > 0 && logger[0] != nil {
		log = logger[0].With(slog.Group("connector",
			slog.String("name", "duckdb"),
			slog.String("type", "sink")))
	} else {
		log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})).With(slog.Group("connector",
			slog.String("name", "duckdb"),
			slog.String("type", "sink")))
	}

	if config.BatchSize < 1 {
		config.BatchSize = 1
	}
	if config.MaxRetries < 1 {
		config.MaxRetries = 3
	}
	if config.ChannelCapacity < 1 {
		config.ChannelCapacity = config.BatchSize * 2
	}
	if config.InitialRetryDelay <= 0 {
		config.InitialRetryDelay = 100 * time.Millisecond
	}
	if config.MaxRetryDelay <= 0 {
		config.MaxRetryDelay = 30 * time.Second
	}

	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)

	sink := &DuckDBSink{
		db:     db,
		config: config,
		in:     make(chan any, config.ChannelCapacity),
		done:   make(chan struct{}),
		logger: log,
		ctx:    ctx,
		cancel: cancel,
	}

	if config.BatchSize > 1 {
		sink.buffer = make([]Record, 0, config.BatchSize)
	}

	if config.EnableErrorChannel {
		sink.errChan = make(chan error, 1)
	}

	go sink.processStream()
	return sink
}

func (d *DuckDBSink) processStream() {
	defer close(d.done)

	for {
		select {
		case msg, ok := <-d.in:
			if !ok {
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
				if d.errChan != nil {
					select {
					case d.errChan <- err:
					default:
					}
				}
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

// processMessage converts and validates input messages.
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

// insertRecord handles record insertion with batching support.
func (d *DuckDBSink) insertRecord(record Record) error {
	if d.config.BatchSize > 1 {
		d.buffer = append(d.buffer, record)
		if len(d.buffer) >= d.config.BatchSize {
			return d.flushBuffer()
		}
		return nil
	}

	return d.insertSingleRecord(record)
}

// insertSingleRecord handles immediate record insertion.
func (d *DuckDBSink) insertSingleRecord(record Record) error {
	if len(record) == 0 {
		return fmt.Errorf("empty record")
	}
	return d.insertRecordSimple(record)
}

// insertRecordSimple executes a single INSERT query with retries.
func (d *DuckDBSink) insertRecordSimple(record Record) error {
	recordLen := len(record)
	if recordLen == 0 {
		return fmt.Errorf("empty record")
	}

	columns := stringSlicePool.Get().([]string)[:0]
	placeholders := stringSlicePool.Get().([]string)[:0]
	values := interfaceSlicePool.Get().([]any)[:0]

	defer func() {
		stringSlicePool.Put(columns[:0])
		stringSlicePool.Put(placeholders[:0])
		interfaceSlicePool.Put(values[:0])
	}()

	if cap(columns) < recordLen {
		columns = make([]string, 0, recordLen)
	}
	if cap(placeholders) < recordLen {
		placeholders = make([]string, 0, recordLen)
	}
	if cap(values) < recordLen {
		values = make([]any, 0, recordLen)
	}

	for col, val := range record {
		columns = append(columns, quoteIdentifier(col))
		placeholders = append(placeholders, "?")
		values = append(values, val)
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		quoteIdentifier(d.config.TableName),
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	var lastErr error
	for attempt := 0; attempt <= d.config.MaxRetries; attempt++ {
		_, err := d.db.Exec(query, values...)
		if err == nil {
			return nil
		}

		lastErr = err
		if attempt < d.config.MaxRetries {
			delay := calculateRetryDelay(attempt+1, d.config.InitialRetryDelay, d.config.MaxRetryDelay)
			d.logger.Warn("Insert failed, retrying",
				slog.Int("attempt", attempt+1),
				slog.Duration("retry_delay", delay),
				slog.Any("error", err))
			time.Sleep(delay)
		}
	}

	return fmt.Errorf("failed to insert record after %d attempts: %w", d.config.MaxRetries+1, lastErr)
}

// flushBuffer commits buffered records as a batch with retries.
func (d *DuckDBSink) flushBuffer() error {
	if len(d.buffer) == 0 {
		return nil
	}

	d.logger.Debug("Flushing batch", slog.Int("size", len(d.buffer)))

	var lastErr error
	for attempt := 0; attempt <= d.config.MaxRetries; attempt++ {
		tx, err := d.db.Begin()
		if err != nil {
			lastErr = fmt.Errorf("failed to begin transaction: %w", err)
			if attempt < d.config.MaxRetries {
				delay := calculateRetryDelay(attempt+1, d.config.InitialRetryDelay, d.config.MaxRetryDelay)
				d.logger.Warn("Batch transaction begin failed, retrying",
					slog.Int("attempt", attempt+1),
					slog.Duration("retry_delay", delay),
					slog.Any("error", err))
				time.Sleep(delay)
				continue
			}
			return lastErr
		}
		defer tx.Rollback()

		batchErr := d.insertBatchOptimized(tx)
		if batchErr != nil {
			lastErr = batchErr
			if attempt < d.config.MaxRetries {
				delay := calculateRetryDelay(attempt+1, d.config.InitialRetryDelay, d.config.MaxRetryDelay)
				d.logger.Warn("Batch insert failed, retrying",
					slog.Int("attempt", attempt+1),
					slog.Duration("retry_delay", delay),
					slog.Any("error", batchErr))
				time.Sleep(delay)
				continue
			}
			return fmt.Errorf("failed to flush batch after %d attempts: %w", d.config.MaxRetries+1, lastErr)
		}

		if err := tx.Commit(); err != nil {
			lastErr = fmt.Errorf("failed to commit transaction: %w", err)
			if attempt < d.config.MaxRetries {
				delay := calculateRetryDelay(attempt+1, d.config.InitialRetryDelay, d.config.MaxRetryDelay)
				d.logger.Warn("Batch commit failed, retrying",
					slog.Int("attempt", attempt+1),
					slog.Duration("retry_delay", delay),
					slog.Any("error", err))
				time.Sleep(delay)
				continue
			}
			return fmt.Errorf("failed to commit batch after %d attempts: %w", d.config.MaxRetries+1, lastErr)
		}

		d.buffer = d.buffer[:0]
		return nil
	}

	return fmt.Errorf("failed to flush batch after %d attempts: %w", d.config.MaxRetries+1, lastErr)
}

// insertBatchOptimized chooses between multi-row INSERT or individual inserts based on schema compatibility.
func (d *DuckDBSink) insertBatchOptimized(tx *sql.Tx) error {
	if len(d.buffer) == 0 {
		return nil
	}

	if len(d.buffer) == 1 {
		return d.insertRecordInTransaction(tx, d.buffer[0])
	}

	firstRecord := d.buffer[0]
	firstLen := len(firstRecord)
	firstColumns := stringSlicePool.Get().([]string)[:0]
	recordColumns := stringSlicePool.Get().([]string)[:0]

	defer func() {
		stringSlicePool.Put(firstColumns[:0])
		stringSlicePool.Put(recordColumns[:0])
	}()

	if cap(firstColumns) < firstLen {
		firstColumns = make([]string, 0, firstLen)
	}
	if cap(recordColumns) < firstLen {
		recordColumns = make([]string, 0, firstLen)
	}

	for col := range firstRecord {
		firstColumns = append(firstColumns, col)
	}
	sort.Strings(firstColumns)
	allSameSchema := true

	for _, record := range d.buffer[1:] {
		if len(record) != firstLen {
			allSameSchema = false
			break
		}

		recordColumns = recordColumns[:0]
		for col := range record {
			recordColumns = append(recordColumns, col)
		}
		sort.Strings(recordColumns)

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

	if allSameSchema {
		return d.insertBatchMultiRow(tx, firstColumns)
	}

	for _, record := range d.buffer {
		if err := d.insertRecordInTransaction(tx, record); err != nil {
			return err
		}
	}

	return nil
}

// insertBatchMultiRow executes a single multi-row INSERT query.
func (d *DuckDBSink) insertBatchMultiRow(tx *sql.Tx, columns []string) error {
	if len(d.buffer) == 0 || len(columns) == 0 {
		return nil
	}

	bufferLen := len(d.buffer)
	columnsLen := len(columns)
	quotedColumns := make([]string, columnsLen)
	allPlaceholders := stringSlicePool.Get().([]string)[:0]
	allValues := interfaceSlicePool.Get().([]any)[:0]
	rowPlaceholders := stringSlicePool.Get().([]string)[:0]

	defer func() {
		stringSlicePool.Put(allPlaceholders[:0])
		interfaceSlicePool.Put(allValues[:0])
		stringSlicePool.Put(rowPlaceholders[:0])
	}()

	if cap(allPlaceholders) < bufferLen {
		allPlaceholders = make([]string, 0, bufferLen)
	}
	if cap(allValues) < bufferLen*columnsLen {
		allValues = make([]any, 0, bufferLen*columnsLen)
	}
	if cap(rowPlaceholders) < columnsLen {
		rowPlaceholders = make([]string, 0, columnsLen)
	}

	for i, col := range columns {
		quotedColumns[i] = quoteIdentifier(col)
	}

	for _, record := range d.buffer {
		rowPlaceholders = rowPlaceholders[:0]
		for _, col := range columns {
			rowPlaceholders = append(rowPlaceholders, "?")
			if val, exists := record[col]; exists {
				allValues = append(allValues, val)
			} else {
				allValues = append(allValues, nil)
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

// insertRecordInTransaction executes a single INSERT within a transaction.
func (d *DuckDBSink) insertRecordInTransaction(tx *sql.Tx, record Record) error {
	if len(record) == 0 {
		return fmt.Errorf("empty record")
	}

	recordLen := len(record)
	columns := stringSlicePool.Get().([]string)[:0]
	placeholders := stringSlicePool.Get().([]string)[:0]
	values := interfaceSlicePool.Get().([]any)[:0]

	defer func() {
		stringSlicePool.Put(columns[:0])
		stringSlicePool.Put(placeholders[:0])
		interfaceSlicePool.Put(values[:0])
	}()

	if cap(columns) < recordLen {
		columns = make([]string, 0, recordLen)
	}
	if cap(placeholders) < recordLen {
		placeholders = make([]string, 0, recordLen)
	}
	if cap(values) < recordLen {
		values = make([]any, 0, recordLen)
	}

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

// In returns the input channel for sending records.
func (d *DuckDBSink) In() chan<- any {
	return d.in
}

// Errors returns the error reporting channel, or nil if disabled.
func (d *DuckDBSink) Errors() <-chan error {
	return d.errChan
}

// AwaitCompletion blocks until all processing is complete.
func (d *DuckDBSink) AwaitCompletion() {
	<-d.done
}

// Close shuts down the sink and flushes remaining records.
func (d *DuckDBSink) Close() error {
	if d.cancel != nil {
		d.cancel()
	}
	if d.errChan != nil {
		close(d.errChan)
	}
	return nil
}
