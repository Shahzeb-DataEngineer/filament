package motherduck

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	duckdb "github.com/marcboeker/go-duckdb/v2"

	"github.com/galaxy-io/filament"
	"github.com/galaxy-io/filament/arrowbatch"
	"github.com/galaxy-io/filament/internal/arrowtext"
	"github.com/galaxy-io/filament/rowmodel"
)

// Apply validates the requested write policy and appends one Arrow batch.
func (s *Sink) Apply(ctx context.Context, batch *arrowbatch.Batch, opts filament.ApplyOptions) (filament.WriteReceipt, error) {
	if batch == nil {
		return filament.WriteReceipt{}, fmt.Errorf("%s sink: write requires a record batch", s.Name())
	}
	if want := s.modeFor(batch.Resource); opts.Policy.Capability.Mode != want {
		return filament.WriteReceipt{}, fmt.Errorf("%s sink: apply policy %q does not match resource policy %q", s.Name(), opts.Policy.Capability.Mode, want)
	}
	switch opts.Policy.Capability.Mode {
	case filament.WriteReplace:
		policy := opts.Policy
		policy.Capability.AcceptsOps = []rowmodel.Operation{rowmodel.OpInsert}
		if err := policy.ValidateBatch(batch.Resource, batch); err != nil {
			return filament.WriteReceipt{}, fmt.Errorf("%s sink: %w", s.Name(), err)
		}
	case filament.WriteAppend:
		if err := opts.Policy.ValidateBatch(batch.Resource, batch); err != nil {
			return filament.WriteReceipt{}, fmt.Errorf("%s sink: %w", s.Name(), err)
		}
	case filament.WriteUpsert:
		policy := opts.Policy
		policy.Capability.AcceptsOps = []rowmodel.Operation{rowmodel.OpInsert, rowmodel.OpUpdate}
		if err := policy.ValidateBatch(batch.Resource, batch); err != nil {
			return filament.WriteReceipt{}, fmt.Errorf("%s sink: %w", s.Name(), err)
		}
	default:
		return filament.WriteReceipt{}, fmt.Errorf("%s sink: write policy %q is not implemented", s.Name(), opts.Policy.Capability.Mode)
	}
	return s.write(ctx, batch)
}

func (s *Sink) write(ctx context.Context, batch *arrowbatch.Batch) (filament.WriteReceipt, error) {
	if s.db == nil {
		return filament.WriteReceipt{}, fmt.Errorf("%s sink: write before open", s.Name())
	}
	if batch.Rows() == nil {
		return filament.WriteReceipt{}, fmt.Errorf("%s sink: write requires a record batch", s.Name())
	}
	s.tablesMu.RLock()
	tbl := s.tables[batch.Resource]
	s.tablesMu.RUnlock()
	if tbl == nil {
		return filament.WriteReceipt{}, fmt.Errorf("%s sink: no schema ensured for resource %q", s.Name(), batch.Resource)
	}
	conn, err := s.take(ctx)
	if err != nil {
		return filament.WriteReceipt{}, fmt.Errorf("%s sink: acquire connection: %w", s.Name(), err)
	}
	defer s.put(conn)
	if err := ctx.Err(); err != nil {
		return filament.WriteReceipt{}, err
	}
	rows := batch.Rows()
	writeCRC := batch.IntegrityCRC()
	if timeNeedsStatement(batch) {
		// The appender cannot carry a TIME at or past 24:00:00 (see
		// timeNeedsStatement), so the whole batch goes through prepared
		// INSERTs instead. The appender keeps serving every other batch.
		if err := s.insertRows(ctx, conn, tbl, batch); err != nil {
			return filament.WriteReceipt{}, fmt.Errorf("%s sink: insert %s seq %d: %w", s.Name(), batch.Resource, batch.Seq, err)
		}
	} else {
		appender := conn.appenders[batch.Resource]
		if appender == nil {
			appender, err = duckdb.NewAppender(conn.conn, s.database, s.schema, tbl.writeName)
			if err != nil {
				return filament.WriteReceipt{}, fmt.Errorf("%s sink: create appender for %s: %w", s.Name(), batch.Resource, err)
			}
			conn.appenders[batch.Resource] = appender
		}
		if err := appendRows(appender, batch, tbl.columns); err != nil {
			s.discardAppender(conn, batch.Resource)
			return filament.WriteReceipt{}, fmt.Errorf("%s sink: append %s seq %d: %w", s.Name(), batch.Resource, batch.Seq, err)
		}
		if err := appender.Flush(); err != nil {
			s.discardAppender(conn, batch.Resource)
			return filament.WriteReceipt{}, fmt.Errorf("%s sink: flush %s seq %d: %w", s.Name(), batch.Resource, batch.Seq, err)
		}
	}
	s.written.Add(int64(batch.NumRows()))
	return filament.WriteReceipt{
		URI: s.tableURI(batch.Resource), Rows: batch.NumRows(), Bytes: arrowbatch.Bytes(rows), WriteCRC: writeCRC,
	}, nil
}

// appendRows hands every row of the batch to the appender, column by column
// through the converters chosen from the Arrow schema. With destination
// columns, each value goes to its column's position and columns the source
// lacks are written as NULL.
func appendRows(appender *duckdb.Appender, batch *arrowbatch.Batch, destination []string) error {
	rows := batch.Rows()
	cols := rows.Columns()
	fields := rows.Schema().Fields()
	fns := make([]valueFn, len(cols))
	positions := make([]int, len(cols))
	for i, f := range fields {
		fns[i] = valueFor(f)
		positions[i] = i
	}
	width := len(cols)
	if destination != nil {
		index := make(map[string]int, len(destination))
		for i, name := range destination {
			index[strings.ToLower(name)] = i
		}
		for i, f := range fields {
			position, ok := index[strings.ToLower(f.Name)]
			if !ok {
				return fmt.Errorf("column %q is missing from the destination", f.Name)
			}
			positions[i] = position
		}
		width = len(destination)
	}
	values := make([]driver.Value, width)
	for i := range batch.NumRows() {
		for c, col := range cols {
			if col.IsNull(i) {
				values[positions[c]] = nil
				continue
			}
			values[positions[c]] = fns[c](col, i)
		}
		if err := appender.AppendRow(values...); err != nil {
			return fmt.Errorf("row %d: %w", i, err)
		}
	}
	return nil
}

// timeNeedsStatement reports whether the batch holds a TIME value the
// appender cannot carry. go-duckdb binds TIME only from time.Time, and it
// reads the time of day off that value, so a TIME at or past 24:00:00 wraps
// to midnight with no error: Postgres stores 24:00:00 as a real value, and
// the MotherDuck sink would store it as 00:00:00. Such batches go through
// prepared INSERTs with the time as text instead. Values past 24:00:00 have
// no DuckDB TIME form, so the INSERT fails loudly rather than wrapping.
func timeNeedsStatement(batch *arrowbatch.Batch) bool {
	rows := batch.Rows()
	if rows == nil {
		return false
	}
	for i, f := range rows.Schema().Fields() {
		if f.Type.ID() != arrow.TIME64 {
			continue
		}
		col := rows.Columns()[i].(*array.Time64)
		for r := range batch.NumRows() {
			if col.IsNull(r) {
				continue
			}
			us := time64Micros(f.Type.(*arrow.Time64Type).Unit, int64(col.Value(r)))
			if us < 0 || us >= arrowtext.MicrosPerDay {
				return true
			}
		}
	}
	return false
}

// time64Micros converts an Arrow Time64 value to microseconds since
// midnight for the storage unit the column carries.
func time64Micros(unit arrow.TimeUnit, value int64) int64 {
	switch unit {
	case arrow.Second:
		return value * arrowtext.MicrosPerSecond
	case arrow.Millisecond:
		return value * (arrowtext.MicrosPerSecond / 1000)
	case arrow.Nanosecond:
		return value / 1000
	default: // arrow.Microsecond
		return value
	}
}

// insertRows writes the batch with prepared INSERTs instead of the appender.
// TIME columns travel as text with an explicit ::TIME cast, which keeps
// 24:00:00 intact; every other column keeps the appender's converter.
func (s *Sink) insertRows(ctx context.Context, conn *pooledConnection, tbl *table, batch *arrowbatch.Batch) error {
	rows := batch.Rows()
	cols := rows.Columns()
	fields := rows.Schema().Fields()
	fns := make([]valueFn, len(cols))
	positions := make([]int, len(cols))
	for i, f := range fields {
		fns[i] = valueFor(f)
		positions[i] = i
	}
	width := len(cols)
	if tbl.columns != nil {
		index := make(map[string]int, len(tbl.columns))
		for i, name := range tbl.columns {
			index[strings.ToLower(name)] = i
		}
		for i, f := range fields {
			position, ok := index[strings.ToLower(f.Name)]
			if !ok {
				return fmt.Errorf("column %q is missing from the destination", f.Name)
			}
			positions[i] = position
		}
		width = len(tbl.columns)
	}
	// Destination column names in destination order: the destination's own
	// names where the live table was inspected, else the source field names.
	// The stored names matter here because the INSERT quotes them, and a
	// live table may hold mixed-case columns the source lowercased.
	names := make([]string, width)
	if tbl.columns != nil {
		copy(names, tbl.columns)
	} else {
		for i, f := range fields {
			names[i] = f.Name
		}
	}
	quoted := make([]string, width)
	placeholders := make([]string, width)
	for i, name := range names {
		quoted[i] = quoteIdent(name)
		placeholders[i] = "?"
	}
	for c, f := range fields {
		if f.Type.ID() == arrow.TIME64 {
			placeholders[positions[c]] = "?::TIME"
		}
	}
	query := "INSERT INTO " + tbl.writeTo + " (" + strings.Join(quoted, ", ") +
		") VALUES (" + strings.Join(placeholders, ", ") + ")"
	preparer, ok := conn.conn.(driver.ConnPrepareContext)
	if !ok {
		return fmt.Errorf("%s sink: connection does not support prepared statements", s.Name())
	}
	stmt, err := preparer.PrepareContext(ctx, query)
	if err != nil {
		return fmt.Errorf("%s sink: prepare insert for %s: %w", s.Name(), batch.Resource, err)
	}
	defer func() { _ = stmt.Close() }()
	execer, ok := stmt.(driver.StmtExecContext)
	if !ok {
		return fmt.Errorf("%s sink: prepared statement does not support execution with context", s.Name())
	}
	values := make([]driver.Value, width)
	args := make([]driver.NamedValue, width)
	for i := range batch.NumRows() {
		for c, col := range cols {
			if col.IsNull(i) {
				values[positions[c]] = nil
				continue
			}
			if fields[c].Type.ID() == arrow.TIME64 {
				us := time64Micros(fields[c].Type.(*arrow.Time64Type).Unit, int64(col.(*array.Time64).Value(i)))
				values[positions[c]] = string(arrowtext.AppendTimeOfDay(nil, us))
				continue
			}
			values[positions[c]] = fns[c](col, i)
		}
		for j, value := range values {
			args[j] = driver.NamedValue{Ordinal: j + 1, Value: value}
		}
		if _, err := execer.ExecContext(ctx, args); err != nil {
			return fmt.Errorf("row %d: %w", i, err)
		}
	}
	return nil
}

func (s *Sink) discardAppender(conn *pooledConnection, resource string) {
	if appender := conn.appenders[resource]; appender != nil {
		_ = appender.Close()
		delete(conn.appenders, resource)
	}
}

func (s *Sink) tableURI(resource string) string {
	return fmt.Sprintf("md://%s/%s.%s", s.database, s.schema, resource)
}
