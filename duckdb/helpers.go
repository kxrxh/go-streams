package duckdb

import (
	"hash/fnv"
	"sort"
	"strings"
)

func quoteIdentifier(identifier string) string {
	if !strings.Contains(identifier, `"`) {
		return `"` + identifier + `"`
	}

	requiredCapacity := len(identifier) + 2
	for _, character := range identifier {
		if character == '"' {
			requiredCapacity++
		}
	}

	builder := stringBuilderPool.Get().(*strings.Builder)
	defer func() {
		builder.Reset()
		stringBuilderPool.Put(builder)
	}()

	builder.Grow(requiredCapacity)
	builder.WriteByte('"')

	for _, character := range identifier {
		builder.WriteRune(character)
		if character == '"' {
			builder.WriteByte('"')
		}
	}

	builder.WriteByte('"')
	return builder.String()
}

func splitQualified(name string) (schema, table string) {
	dotIndex := strings.LastIndexByte(name, '.')
	if dotIndex == -1 {
		return "main", name
	}
	return name[:dotIndex], name[dotIndex+1:]
}

func keys(m map[string]any) []string {
	out := stringSlicePool.Get().([]string)
	out = out[:0]
	for k := range m {
		out = append(out, k)
	}
	return out
}

func stableColumnsAndValues(record Record) ([]string, []any) {
	cols := keys(record)
	sort.Strings(cols)
	vals := interfaceSlicePool.Get().([]any)
	vals = vals[:0]
	for _, c := range cols {
		vals = append(vals, record[c])
	}
	return cols, vals
}

func flattenRowArgs(rows [][]any) []any {
	if len(rows) == 0 {
		return nil
	}

	total := 0
	for _, r := range rows {
		total += len(r)
	}

	out := interfaceSlicePool.Get().([]any)
	if cap(out) < total {
		out = make([]any, 0, total)
		interfaceSlicePool.Put(out[:0])
	} else {
		out = out[:0]
	}

	for _, r := range rows {
		out = append(out, r...)
	}
	return out
}

func columnHash(columns []string) uint64 {
	tmp := make([]string, len(columns))
	copy(tmp, columns)
	sort.Strings(tmp)

	hasher := fnv.New64a()
	for _, col := range tmp {
		hasher.Write([]byte(col))
	}
	return hasher.Sum64()
}

func (d *DuckDBSink) cachedColumnsAndValues(record Record) ([]string, []any) {
	columnNames := keys(record)
	cacheKey := columnHash(columnNames)

	if cachedColumns, exists := d.columnCache.Load(cacheKey); exists {
		sortedColumnNames := cachedColumns.([]string)
		values := interfaceSlicePool.Get().([]any)

		if cap(values) < len(sortedColumnNames) {
			values = make([]any, len(sortedColumnNames))
			interfaceSlicePool.Put(values[:0])
		} else {
			values = values[:len(sortedColumnNames)]
		}

		for index, columnName := range sortedColumnNames {
			values[index] = record[columnName]
		}

		stringSlicePool.Put(columnNames[:0])
		return sortedColumnNames, values
	}

	sort.Strings(columnNames)
	cachedColumns := make([]string, len(columnNames))
	copy(cachedColumns, columnNames)
	d.columnCache.LoadOrStore(cacheKey, cachedColumns)

	values := interfaceSlicePool.Get().([]any)
	if cap(values) < len(columnNames) {
		values = make([]any, len(columnNames))
		interfaceSlicePool.Put(values[:0])
	} else {
		values = values[:len(columnNames)]
	}
	for index, columnName := range columnNames {
		values[index] = record[columnName]
	}

	return columnNames, values
}
