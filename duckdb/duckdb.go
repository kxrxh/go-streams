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

	_ "github.com/marcboeker/go-duckdb"
	"github.com/reugn/go-streams"
)

const (
	DefaultBatchSize          = 1
	ChannelCapacityMultiplier = 2
	DefaultSlicePoolCapacity  = 16

	LogLevelInfo = slog.LevelInfo
)

// SinkConfig holds configuration for the DuckDB sink.
type SinkConfig struct {
	TableName       string // Target table name; supports schema.table format
	BatchSize       int    // Records per batch (1 = no batching)
	ChannelCapacity int    // Input channel buffer size
}

// Record represents a database row as column-value pairs.
type Record map[string]any

// DuckDBSink provides a sink for writing records to DuckDB.
// Supports batching, prepared statement caching, and concurrent operation.
type DuckDBSink struct {
	db          *sql.DB
	config      SinkConfig
	in          chan any
	done        chan struct{}
	logger      *slog.Logger
	ctx         context.Context
	cancel      context.CancelFunc
	buffer      []Record
	stmtCacheMu sync.RWMutex
	stmtCache   map[string]*sql.Stmt
	columnCache sync.Map
	closeOnce   sync.Once
}

var _ streams.Sink = (*DuckDBSink)(nil)

var (
	stringSlicePool = sync.Pool{
		New: func() any { return make([]string, 0, DefaultSlicePoolCapacity) },
	}
	stringBuilderPool = sync.Pool{
		New: func() any { return &strings.Builder{} },
	}
	interfaceSlicePool = sync.Pool{
		New: func() any { return make([]any, 0, DefaultSlicePoolCapacity) },
	}
)

// NewSink creates a new DuckDB sink.
// Caller must manage the database connection.
// Uses context.Background() if ctx is nil.
func NewSink(ctx context.Context, db *sql.DB, config SinkConfig, logger ...*slog.Logger) *DuckDBSink {
	var sinkLogger *slog.Logger
	if len(logger) > 0 && logger[0] != nil {
		sinkLogger = logger[0].With(slog.Group("connector",
			slog.String("name", "duckdb"),
			slog.String("type", "sink")))
	} else {
		sinkLogger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: LogLevelInfo,
		})).With(slog.Group("connector",
			slog.String("name", "duckdb"),
			slog.String("type", "sink")))
	}

	if config.BatchSize < 1 {
		config.BatchSize = DefaultBatchSize
	}
	if config.ChannelCapacity < 1 {
		config.ChannelCapacity = config.BatchSize * ChannelCapacityMultiplier
	}

	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)

	sink := &DuckDBSink{
		db:        db,
		config:    config,
		in:        make(chan any, config.ChannelCapacity),
		done:      make(chan struct{}),
		logger:    sinkLogger,
		ctx:       ctx,
		cancel:    cancel,
		stmtCache: make(map[string]*sql.Stmt),
	}

	if config.BatchSize > 1 {
		sink.buffer = make([]Record, 0, config.BatchSize)
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
				if err := d.flushBuffer(); err != nil {
					d.logger.Error("Failed to flush buffer on shutdown", slog.Any("error", err))
				}
				return
			}
			if err := d.processMessage(msg); err != nil {
				d.logger.Error("Failed to process message", slog.Any("error", err))
				_ = d.flushBuffer()
				return
			}
		case <-d.ctx.Done():
			d.logger.Info("Context cancelled", slog.Any("error", d.ctx.Err()))
			if err := d.flushBuffer(); err != nil {
				d.logger.Error("Failed to flush buffer on cancellation", slog.Any("error", err))
			}
			return
		}
	}
}

func (d *DuckDBSink) processMessage(inputMessage any) error {
	var record Record

	switch msg := inputMessage.(type) {
	case Record:
		record = msg
	case map[string]any:
		record = Record(msg)
	default:
		return fmt.Errorf("unsupported message type %T, expected Record or map[string]any", inputMessage)
	}

	return d.insertRecord(record)
}

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

func (d *DuckDBSink) insertSingleRecord(record Record) error {
	if len(record) == 0 {
		return fmt.Errorf("empty record")
	}
	cols, vals := d.cachedColumnsAndValues(record)
	err := d.execInsertSingle(d.db, cols, vals)
	stringSlicePool.Put(cols[:0])
	interfaceSlicePool.Put(vals[:0])
	return err
}

func (d *DuckDBSink) flushBuffer() error {
	if len(d.buffer) == 0 {
		return nil
	}

	tx, err := d.db.BeginTx(d.ctx, nil)
	if err != nil {
		d.logger.Error("Failed to begin transaction", slog.Any("error", err))
		d.buffer = d.buffer[:0]
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	if err := d.insertBatchOptimized(tx); err != nil {
		d.logger.Error("Batch insert failed", slog.Any("error", err))
		d.buffer = d.buffer[:0]
		return fmt.Errorf("batch insert: %w", err)
	}

	if err := tx.Commit(); err != nil {
		d.logger.Error("Commit failed", slog.Any("error", err))
		d.buffer = d.buffer[:0]
		return fmt.Errorf("commit: %w", err)
	}

	d.buffer = d.buffer[:0]
	return nil
}

func (d *DuckDBSink) insertBatchOptimized(tx *sql.Tx) error {
	if len(d.buffer) == 0 {
		return nil
	}
	if len(d.buffer) == 1 {
		return d.insertSingleBufferedRecord(tx)
	}
	if d.allRecordsHaveSameSchema() {
		return d.insertBatchWithSameSchema(tx)
	}
	return d.insertBatchIndividual(tx)
}

func (d *DuckDBSink) insertSingleBufferedRecord(tx *sql.Tx) error {
	cols, vals := stableColumnsAndValues(d.buffer[0])
	return d.execInsertTx(tx, cols, [][]any{vals})
}

func (d *DuckDBSink) allRecordsHaveSameSchema() bool {
	if len(d.buffer) < 2 {
		return true
	}
	firstCols := keys(d.buffer[0])
	defer stringSlicePool.Put(firstCols[:0])
	firstHash := columnHash(firstCols)

	for _, record := range d.buffer[1:] {
		if len(record) != len(d.buffer[0]) {
			return false
		}
		recCols := keys(record)
		recHash := columnHash(recCols)
		stringSlicePool.Put(recCols[:0])
		if recHash != firstHash {
			return false
		}
	}
	return true
}

func (d *DuckDBSink) insertBatchWithSameSchema(tx *sql.Tx) error {
	firstCols := keys(d.buffer[0])
	defer stringSlicePool.Put(firstCols[:0])

	sortedCols := d.getOrCacheSortedColumns(firstCols)
	defer stringSlicePool.Put(sortedCols[:0])

	rows := d.prepareRowsForBatchInsert(sortedCols)
	defer func() {
		for _, row := range rows {
			interfaceSlicePool.Put(row[:0])
		}
	}()
	return d.execInsertTx(tx, sortedCols, rows)
}

func (d *DuckDBSink) getOrCacheSortedColumns(columns []string) []string {
	hashKey := columnHash(columns)
	if cached, exists := d.columnCache.Load(hashKey); exists {
		return cached.([]string)
	}
	sort.Strings(columns)
	cachedCols := make([]string, len(columns))
	copy(cachedCols, columns)
	d.columnCache.LoadOrStore(hashKey, cachedCols)
	return columns
}

func (d *DuckDBSink) prepareRowsForBatchInsert(sortedCols []string) [][]any {
	rows := make([][]any, len(d.buffer))
	for i, record := range d.buffer {
		row := interfaceSlicePool.Get().([]any)
		if cap(row) < len(sortedCols) {
			row = make([]any, len(sortedCols))
			interfaceSlicePool.Put(row[:0])
		} else {
			row = row[:len(sortedCols)]
		}
		for j, col := range sortedCols {
			row[j] = record[col]
		}
		rows[i] = row
	}
	return rows
}

func (d *DuckDBSink) insertBatchIndividual(tx *sql.Tx) error {
	for _, record := range d.buffer {
		cols, vals := stableColumnsAndValues(record)
		if err := d.execInsertTx(tx, cols, [][]any{vals}); err != nil {
			stringSlicePool.Put(cols[:0])
			interfaceSlicePool.Put(vals[:0])
			return err
		}
		stringSlicePool.Put(cols[:0])
		interfaceSlicePool.Put(vals[:0])
	}
	return nil
}

func (d *DuckDBSink) execInsertSingle(db *sql.DB, columns []string, values []any) error {
	query, key := d.buildInsertQuery(columns, 1)
	stmt, err := d.getOrPrepare(db, key, query)
	if err != nil {
		return err
	}
	_, err = stmt.ExecContext(d.ctx, values...)
	if err != nil {
		d.logger.Error("Insert failed", slog.Any("error", err))
		return fmt.Errorf("insert: %w", err)
	}
	return nil
}

func (d *DuckDBSink) execInsertTx(tx *sql.Tx, columns []string, rows [][]any) error {
	query, _ := d.buildInsertQuery(columns, len(rows))
	stmt, err := tx.PrepareContext(d.ctx, query)
	if err != nil {
		return fmt.Errorf("prepare statement: %w", err)
	}
	defer stmt.Close()

	args := flattenRowArgs(rows)
	_, err = stmt.ExecContext(d.ctx, args...)
	if err != nil {
		interfaceSlicePool.Put(args[:0])
		return fmt.Errorf("execute insert: %w", err)
	}
	interfaceSlicePool.Put(args[:0])
	return nil
}

func (d *DuckDBSink) buildInsertQuery(columns []string, nrows int) (query string, key string) {
	query = d.buildInsertSQL(columns, nrows)
	key = d.buildInsertCacheKey(columns, nrows)
	return
}

func (d *DuckDBSink) buildInsertSQL(columns []string, nrows int) string {
	builder := stringBuilderPool.Get().(*strings.Builder)
	defer func() {
		builder.Reset()
		stringBuilderPool.Put(builder)
	}()

	capacity := d.calculateInsertQueryCapacity(columns, nrows)
	builder.Grow(capacity)
	d.buildInsertClause(builder, columns)
	d.buildValuesClause(builder, columns, nrows)
	return builder.String()
}

func (d *DuckDBSink) buildInsertCacheKey(columns []string, nrows int) string {
	keyBuilder := stringBuilderPool.Get().(*strings.Builder)
	defer func() {
		keyBuilder.Reset()
		stringBuilderPool.Put(keyBuilder)
	}()

	schema, table := splitQualified(d.config.TableName)
	keyCapacity := len(schema) + len(table) + len(columns)*10 + 10
	keyBuilder.Grow(keyCapacity)

	keyBuilder.WriteString(schema)
	keyBuilder.WriteByte('.')
	keyBuilder.WriteString(table)
	keyBuilder.WriteByte('|')

	for i, c := range columns {
		if i > 0 {
			keyBuilder.WriteByte(',')
		}
		keyBuilder.WriteString(c)
	}
	keyBuilder.WriteByte('|')
	keyBuilder.WriteString(fmt.Sprintf("%d", nrows))
	return keyBuilder.String()
}

func (d *DuckDBSink) calculateInsertQueryCapacity(columns []string, nrows int) int {
	baseSize := 20
	tableSize := len(d.config.TableName) * 2
	columnsSize := len(columns) * 10
	valuesSize := nrows * len(columns) * 3
	return baseSize + tableSize + columnsSize + valuesSize
}

func (d *DuckDBSink) buildInsertClause(builder *strings.Builder, columns []string) {
	builder.WriteString("INSERT INTO ")
	schema, table := splitQualified(d.config.TableName)
	builder.WriteString(quoteIdentifier(schema))
	builder.WriteByte('.')
	builder.WriteString(quoteIdentifier(table))
	builder.WriteString(" (")

	for i, c := range columns {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(quoteIdentifier(c))
	}
	builder.WriteString(") VALUES ")
}

func (d *DuckDBSink) buildValuesClause(builder *strings.Builder, columns []string, nrows int) {
	for r := 0; r < nrows; r++ {
		if r > 0 {
			builder.WriteString(", ")
		}
		builder.WriteByte('(')
		for i := 0; i < len(columns); i++ {
			if i > 0 {
				builder.WriteString(", ")
			}
			builder.WriteByte('?')
		}
		builder.WriteByte(')')
	}
}

func (d *DuckDBSink) getOrPrepare(db *sql.DB, key, query string) (*sql.Stmt, error) {
	d.stmtCacheMu.RLock()
	stmt, ok := d.stmtCache[key]
	d.stmtCacheMu.RUnlock()
	if ok {
		return stmt, nil
	}
	newStmt, err := db.PrepareContext(d.ctx, query)
	if err != nil {
		return nil, fmt.Errorf("prepare statement: %w", err)
	}
	d.stmtCacheMu.Lock()
	if existing, ok := d.stmtCache[key]; ok {
		d.stmtCacheMu.Unlock()
		_ = newStmt.Close()
		return existing, nil
	}
	d.stmtCache[key] = newStmt
	d.stmtCacheMu.Unlock()
	return newStmt, nil
}

// In returns the input channel for sending records.
func (d *DuckDBSink) In() chan<- any { return d.in }

// AwaitCompletion blocks until all processing is complete.
func (d *DuckDBSink) AwaitCompletion() { <-d.done }

func (d *DuckDBSink) Close() error {
	var retErr error
	d.closeOnce.Do(func() {
		select {
		case <-d.done:
		default:
			close(d.in)
		}
		<-d.done

		d.stmtCacheMu.Lock()
		for k, st := range d.stmtCache {
			if st != nil {
				if err := st.Close(); err != nil && retErr == nil {
					retErr = fmt.Errorf("close statement: %w", err)
				}
			}
			delete(d.stmtCache, k)
		}
		d.stmtCacheMu.Unlock()

		if d.cancel != nil {
			d.cancel()
		}
	})
	return retErr
}
