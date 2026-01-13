package query

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/axiomhq/axiom-go/axiom/query"
	"golang.org/x/sync/singleflight"

	"github.com/axiomhq/axiom-fs/internal/axiomclient"
	"github.com/axiomhq/axiom-fs/internal/cache"
)

type Executor struct {
	client           axiomclient.API
	cache            *cache.Cache
	defaultRange     string
	defaultLimit     int
	maxCacheBytes    int
	maxInMemoryBytes int
	tempDir          string
	sf               singleflight.Group
}

type ExecOptions struct {
	UseCache        bool
	EnsureTimeRange bool
	EnsureLimit     bool
}

type Runner interface {
	ExecuteAPL(ctx context.Context, apl, format string, opts ExecOptions) ([]byte, error)
	ExecuteAPLResult(ctx context.Context, apl, format string, opts ExecOptions) (ResultData, error)
	QueryAPL(ctx context.Context, apl string, opts ExecOptions) (*query.Result, error)
}

type ResultData struct {
	Bytes []byte
	File  *os.File
	Size  int64
}

func NewExecutor(client axiomclient.API, c *cache.Cache, defaultRange string, defaultLimit int, maxCacheBytes int, maxInMemoryBytes int, tempDir string) *Executor {
	return &Executor{
		client:           client,
		cache:            c,
		defaultRange:     defaultRange,
		defaultLimit:     defaultLimit,
		maxCacheBytes:    maxCacheBytes,
		maxInMemoryBytes: maxInMemoryBytes,
		tempDir:          tempDir,
	}
}

func (e *Executor) QueryAPL(ctx context.Context, apl string, opts ExecOptions) (*query.Result, error) {
	if opts.EnsureTimeRange {
		apl = ensureTimeRange(apl, e.defaultRange)
	}
	if opts.EnsureLimit {
		apl = ensureLimit(apl, e.defaultLimit)
	}
	return e.client.QueryAPL(ctx, apl)
}

func (e *Executor) ExecuteAPL(ctx context.Context, apl, format string, opts ExecOptions) ([]byte, error) {
	if opts.EnsureTimeRange {
		apl = ensureTimeRange(apl, e.defaultRange)
	}
	if opts.EnsureLimit {
		apl = ensureLimit(apl, e.defaultLimit)
	}
	key := cacheKey(apl, format)

	if opts.UseCache && e.cache != nil {
		if data, ok := e.cache.Get(key); ok {
			return data, nil
		}
	}

	value, err, _ := e.sf.Do(key, func() (any, error) {
		result, err := e.client.QueryAPL(ctx, apl)
		if err != nil {
			return nil, err
		}
		data, err := encodeResult(result, format)
		if err != nil {
			return nil, err
		}
		if opts.UseCache && e.cache != nil {
			e.cache.Set(key, data)
		}
		return data, nil
	})
	if err != nil {
		return nil, err
	}
	return value.([]byte), nil
}

func (e *Executor) ExecuteAPLResult(ctx context.Context, apl, format string, opts ExecOptions) (ResultData, error) {
	if opts.EnsureTimeRange {
		apl = ensureTimeRange(apl, e.defaultRange)
	}
	if opts.EnsureLimit {
		apl = ensureLimit(apl, e.defaultLimit)
	}
	key := cacheKey(apl, format)

	if opts.UseCache && e.cache != nil {
		if data, ok := e.cache.Get(key); ok {
			return ResultData{Bytes: data, Size: int64(len(data))}, nil
		}
	}

	value, err, _ := e.sf.Do(key, func() (any, error) {
		result, err := e.client.QueryAPL(ctx, apl)
		if err != nil {
			return nil, err
		}
		writer, err := newSpillWriter(e.maxInMemoryBytes, e.tempDir)
		if err != nil {
			return nil, err
		}
		if err := encodeResultToWriter(result, format, writer); err != nil {
			writer.cleanup()
			return nil, err
		}
		if writer.file == nil {
			data := writer.buffer.Bytes()
			if opts.UseCache && e.cache != nil && e.shouldCache(len(data)) {
				e.cache.Set(key, data)
			}
			return ResultData{Bytes: data, Size: int64(len(data))}, nil
		}
		size, _ := writer.file.Seek(0, io.SeekEnd)
		_, _ = writer.file.Seek(0, io.SeekStart)
		return ResultData{File: writer.file, Size: size}, nil
	})
	if err != nil {
		return ResultData{}, err
	}
	return value.(ResultData), nil
}

func encodeResult(result *query.Result, format string) ([]byte, error) {
	if len(result.Tables) == 0 {
		switch format {
		case "json":
			return []byte("[]\n"), nil
		case "csv":
			return []byte{}, nil
		default:
			return []byte{}, nil
		}
	}

	table := result.Tables[0]
	switch format {
	case "ndjson":
		return encodeNDJSON(table)
	case "json":
		return encodeJSON(table)
	case "csv":
		return encodeCSV(table)
	default:
		return nil, fmt.Errorf("unsupported format: %s", format)
	}
}

func encodeResultToWriter(result *query.Result, format string, w io.Writer) error {
	if len(result.Tables) == 0 {
		switch format {
		case "json":
			_, err := io.WriteString(w, "[]\n")
			return err
		default:
			return nil
		}
	}

	table := result.Tables[0]
	switch format {
	case "ndjson":
		return encodeNDJSONToWriter(table, w)
	case "json":
		return encodeJSONToWriter(table, w)
	case "csv":
		return encodeCSVToWriter(table, w)
	default:
		return fmt.Errorf("unsupported format: %s", format)
	}
}

func encodeNDJSON(table query.Table) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for row := range table.Rows() {
		entry := make(map[string]any, len(table.Fields))
		for i, field := range table.Fields {
			if i < len(row) {
				entry[field.Name] = row[i]
			}
		}
		if err := enc.Encode(entry); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func encodeNDJSONToWriter(table query.Table, w io.Writer) error {
	enc := json.NewEncoder(w)
	for row := range table.Rows() {
		entry := make(map[string]any, len(table.Fields))
		for i, field := range table.Fields {
			if i < len(row) {
				entry[field.Name] = row[i]
			}
		}
		if err := enc.Encode(entry); err != nil {
			return err
		}
	}
	return nil
}

func encodeJSON(table query.Table) ([]byte, error) {
	rows := make([]map[string]any, 0)
	for row := range table.Rows() {
		entry := make(map[string]any, len(table.Fields))
		for i, field := range table.Fields {
			if i < len(row) {
				entry[field.Name] = row[i]
			}
		}
		rows = append(rows, entry)
	}
	data, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func encodeJSONToWriter(table query.Table, w io.Writer) error {
	rows := make([]map[string]any, 0)
	for row := range table.Rows() {
		entry := make(map[string]any, len(table.Fields))
		for i, field := range table.Fields {
			if i < len(row) {
				entry[field.Name] = row[i]
			}
		}
		rows = append(rows, entry)
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(rows)
}

func encodeCSV(table query.Table) ([]byte, error) {
	var buf bytes.Buffer
	writer := csv.NewWriter(&buf)
	header := make([]string, 0, len(table.Fields))
	for _, field := range table.Fields {
		header = append(header, field.Name)
	}
	if err := writer.Write(header); err != nil {
		return nil, err
	}
	for row := range table.Rows() {
		record := make([]string, len(table.Fields))
		for i := range table.Fields {
			if i < len(row) {
				record[i] = stringify(row[i])
			}
		}
		if err := writer.Write(record); err != nil {
			return nil, err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeCSVToWriter(table query.Table, w io.Writer) error {
	writer := csv.NewWriter(w)
	header := make([]string, 0, len(table.Fields))
	for _, field := range table.Fields {
		header = append(header, field.Name)
	}
	if err := writer.Write(header); err != nil {
		return err
	}
	for row := range table.Rows() {
		record := make([]string, len(table.Fields))
		for i := range table.Fields {
			if i < len(row) {
				record[i] = stringify(row[i])
			}
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func stringify(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprint(v)
	}
}

func ensureTimeRange(apl, defaultRange string) string {
	if strings.Contains(apl, "_time between") {
		return apl
	}
	rangeExpr := fmt.Sprintf("where _time between (ago(%s) .. now())", defaultRange)
	if strings.Contains(apl, "|") {
		return insertPipeline(apl, rangeExpr)
	}
	return apl + "\n| " + rangeExpr
}

func ensureLimit(apl string, defaultLimit int) string {
	if defaultLimit <= 0 {
		return apl
	}
	lower := strings.ToLower(apl)
	if strings.Contains(lower, " take ") || strings.Contains(lower, "| take") || strings.Contains(lower, " top ") {
		return apl
	}
	return apl + fmt.Sprintf("\n| take %d", defaultLimit)
}

func insertPipeline(apl, clause string) string {
	parts := strings.SplitN(apl, "|", 2)
	if len(parts) < 2 {
		return apl + "\n| " + clause
	}
	head := strings.TrimRight(parts[0], " \n")
	rest := strings.TrimLeft(parts[1], " \n")
	return fmt.Sprintf("%s\n| %s\n| %s", head, clause, rest)
}

func cacheKey(apl, format string) string {
	return fmt.Sprintf("%s|%s", apl, format)
}

func BuildErrorAPL(apl string, err error) []byte {
	payload := map[string]any{
		"apl":   apl,
		"error": "",
		"ok":    err == nil,
		"at":    time.Now().UTC().Format(time.RFC3339Nano),
	}
	if err != nil {
		payload["error"] = err.Error()
	}
	data, _ := json.MarshalIndent(payload, "", "  ")
	return append(data, '\n')
}

func ValidateAPL(apl string) error {
	if strings.TrimSpace(apl) == "" {
		return errors.New("apl is empty")
	}
	return nil
}

func (e *Executor) shouldCache(size int) bool {
	if e.maxCacheBytes > 0 && size > e.maxCacheBytes {
		return false
	}
	return true
}

type spillWriter struct {
	limit   int
	buffer  *bytes.Buffer
	file    *os.File
	size    int
	tempDir string
}

func newSpillWriter(limit int, tempDir string) (*spillWriter, error) {
	return &spillWriter{
		limit:   limit,
		buffer:  &bytes.Buffer{},
		tempDir: tempDir,
	}, nil
}

func (w *spillWriter) Write(p []byte) (int, error) {
	if w.file != nil {
		n, err := w.file.Write(p)
		w.size += n
		return n, err
	}
	if w.limit > 0 && w.buffer.Len()+len(p) > w.limit {
		file, err := os.CreateTemp(w.tempDir, "axiom-fs-*")
		if err != nil {
			return 0, err
		}
		if _, err := file.Write(w.buffer.Bytes()); err != nil {
			_ = file.Close()
			_ = os.Remove(file.Name())
			return 0, err
		}
		w.size = w.buffer.Len()
		w.buffer.Reset()
		w.file = file
	}
	if w.file != nil {
		n, err := w.file.Write(p)
		w.size += n
		return n, err
	}
	return w.buffer.Write(p)
}

func (w *spillWriter) cleanup() {
	if w.file != nil {
		name := w.file.Name()
		_ = w.file.Close()
		_ = os.Remove(name)
	}
}
