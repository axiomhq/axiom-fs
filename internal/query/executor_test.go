package query

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/axiomhq/axiom-fs/internal/axiomclient"
)

func TestEnsureTimeRange(t *testing.T) {
	tests := []struct {
		name         string
		apl          string
		defaultRange string
		wantContains string
		wantMissing  string
	}{
		{
			name:         "adds range when missing simple query",
			apl:          "['logs']",
			defaultRange: "1h",
			wantContains: "ago(1h)",
		},
		{
			name:         "adds range when missing with pipe",
			apl:          "['logs'] | where status >= 500",
			defaultRange: "24h",
			wantContains: "ago(24h)",
		},
		{
			name:         "does not duplicate when present",
			apl:          "['logs'] | where _time between (ago(2h) .. now())",
			defaultRange: "1h",
			wantContains: "ago(2h)",
			wantMissing:  "ago(1h)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ensureTimeRange(tt.apl, tt.defaultRange)
			if !strings.Contains(got, tt.wantContains) {
				t.Errorf("ensureTimeRange() = %q, want to contain %q", got, tt.wantContains)
			}
			if tt.wantMissing != "" && strings.Contains(got, tt.wantMissing) {
				t.Errorf("ensureTimeRange() = %q, should not contain %q", got, tt.wantMissing)
			}
		})
	}
}

func TestEnsureLimit(t *testing.T) {
	tests := []struct {
		name         string
		apl          string
		defaultLimit int
		wantContains string
		wantMissing  string
	}{
		{
			name:         "adds limit when missing",
			apl:          "['logs']",
			defaultLimit: 100,
			wantContains: "take 100",
		},
		{
			name:         "respects existing take",
			apl:          "['logs'] | take 50",
			defaultLimit: 100,
			wantMissing:  "take 100",
		},
		{
			name:         "respects existing top",
			apl:          "['logs'] | top 10 by count",
			defaultLimit: 100,
			wantMissing:  "take 100",
		},
		{
			name:         "zero limit does nothing",
			apl:          "['logs']",
			defaultLimit: 0,
			wantMissing:  "take",
		},
		{
			name:         "negative limit does nothing",
			apl:          "['logs']",
			defaultLimit: -1,
			wantMissing:  "take",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ensureLimit(tt.apl, tt.defaultLimit)
			if tt.wantContains != "" && !strings.Contains(got, tt.wantContains) {
				t.Errorf("ensureLimit() = %q, want to contain %q", got, tt.wantContains)
			}
			if tt.wantMissing != "" && strings.Contains(got, tt.wantMissing) {
				t.Errorf("ensureLimit() = %q, should not contain %q", got, tt.wantMissing)
			}
		})
	}
}

func TestInsertPipeline(t *testing.T) {
	tests := []struct {
		name   string
		apl    string
		clause string
		want   string
	}{
		{
			name:   "inserts after first pipe",
			apl:    "['logs'] | where status >= 500",
			clause: "where _time > ago(1h)",
			want:   "['logs']\n| where _time > ago(1h)\n| where status >= 500",
		},
		{
			name:   "appends when no pipe",
			apl:    "['logs']",
			clause: "where _time > ago(1h)",
			want:   "['logs']\n| where _time > ago(1h)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := insertPipeline(tt.apl, tt.clause)
			if got != tt.want {
				t.Errorf("insertPipeline() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCacheKey(t *testing.T) {
	tests := []struct {
		apl    string
		format string
		want   string
	}{
		{"['logs']", "json", "['logs']|json"},
		{"['logs'] | take 10", "csv", "['logs'] | take 10|csv"},
		{"", "ndjson", "|ndjson"},
	}

	for _, tt := range tests {
		t.Run(tt.apl+"_"+tt.format, func(t *testing.T) {
			got := cacheKey(tt.apl, tt.format)
			if got != tt.want {
				t.Errorf("cacheKey() = %q, want %q", got, tt.want)
			}
		})
	}
}

func makeTestTable(fields []string, rows [][]any) axiomclient.QueryTable {
	qFields := make([]axiomclient.QueryField, len(fields))
	for i, name := range fields {
		qFields[i] = axiomclient.QueryField{Name: name}
	}
	columns := make([][]any, len(fields))
	for i := range fields {
		columns[i] = make([]any, len(rows))
	}
	for rowIdx, row := range rows {
		for colIdx, val := range row {
			if colIdx < len(columns) {
				columns[colIdx][rowIdx] = val
			}
		}
	}
	return axiomclient.QueryTable{
		Fields:  qFields,
		Columns: columns,
	}
}

func TestEncodeResult(t *testing.T) {
	t.Run("empty result ndjson", func(t *testing.T) {
		result := &axiomclient.QueryResult{Tables: nil}
		got, err := encodeResult(result, "ndjson")
		if err != nil {
			t.Fatalf("encodeResult() error = %v", err)
		}
		if len(got) != 0 {
			t.Errorf("encodeResult() = %q, want empty", got)
		}
	})

	t.Run("empty result json", func(t *testing.T) {
		result := &axiomclient.QueryResult{Tables: nil}
		got, err := encodeResult(result, "json")
		if err != nil {
			t.Fatalf("encodeResult() error = %v", err)
		}
		if string(got) != "[]\n" {
			t.Errorf("encodeResult() = %q, want %q", got, "[]\n")
		}
	})

	t.Run("empty result csv", func(t *testing.T) {
		result := &axiomclient.QueryResult{Tables: nil}
		got, err := encodeResult(result, "csv")
		if err != nil {
			t.Fatalf("encodeResult() error = %v", err)
		}
		if len(got) != 0 {
			t.Errorf("encodeResult() = %q, want empty", got)
		}
	})

	t.Run("ndjson with data", func(t *testing.T) {
		table := makeTestTable([]string{"name", "value"}, [][]any{
			{"foo", 1},
			{"bar", 2},
		})
		result := &axiomclient.QueryResult{Tables: []axiomclient.QueryTable{table}}
		got, err := encodeResult(result, "ndjson")
		if err != nil {
			t.Fatalf("encodeResult() error = %v", err)
		}
		lines := strings.Split(strings.TrimSpace(string(got)), "\n")
		if len(lines) != 2 {
			t.Fatalf("expected 2 lines, got %d: %q", len(lines), got)
		}
		var row1 map[string]any
		if err := json.Unmarshal([]byte(lines[0]), &row1); err != nil {
			t.Fatalf("unmarshal line 1: %v", err)
		}
		if row1["name"] != "foo" {
			t.Errorf("row1[name] = %v, want foo", row1["name"])
		}
	})

	t.Run("json with data", func(t *testing.T) {
		table := makeTestTable([]string{"id"}, [][]any{
			{100},
			{200},
		})
		result := &axiomclient.QueryResult{Tables: []axiomclient.QueryTable{table}}
		got, err := encodeResult(result, "json")
		if err != nil {
			t.Fatalf("encodeResult() error = %v", err)
		}
		var rows []map[string]any
		if err := json.Unmarshal(got, &rows); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows, got %d", len(rows))
		}
	})

	t.Run("csv with data", func(t *testing.T) {
		table := makeTestTable([]string{"a", "b"}, [][]any{
			{"x", "y"},
		})
		result := &axiomclient.QueryResult{Tables: []axiomclient.QueryTable{table}}
		got, err := encodeResult(result, "csv")
		if err != nil {
			t.Fatalf("encodeResult() error = %v", err)
		}
		lines := strings.Split(strings.TrimSpace(string(got)), "\n")
		if len(lines) != 2 {
			t.Fatalf("expected 2 lines (header + data), got %d", len(lines))
		}
		if lines[0] != "a,b" {
			t.Errorf("header = %q, want a,b", lines[0])
		}
		if lines[1] != "x,y" {
			t.Errorf("data = %q, want x,y", lines[1])
		}
	})

	t.Run("unsupported format", func(t *testing.T) {
		table := makeTestTable([]string{"a"}, [][]any{{"x"}})
		result := &axiomclient.QueryResult{Tables: []axiomclient.QueryTable{table}}
		_, err := encodeResult(result, "xml")
		if err == nil {
			t.Error("expected error for unsupported format")
		}
	})
}

func TestEncodeResultToWriter(t *testing.T) {
	t.Run("empty result json", func(t *testing.T) {
		result := &axiomclient.QueryResult{Tables: nil}
		var buf bytes.Buffer
		err := encodeResultToWriter(result, "json", &buf)
		if err != nil {
			t.Fatalf("encodeResultToWriter() error = %v", err)
		}
		if buf.String() != "[]\n" {
			t.Errorf("got %q, want %q", buf.String(), "[]\n")
		}
	})

	t.Run("empty result ndjson", func(t *testing.T) {
		result := &axiomclient.QueryResult{Tables: nil}
		var buf bytes.Buffer
		err := encodeResultToWriter(result, "ndjson", &buf)
		if err != nil {
			t.Fatalf("encodeResultToWriter() error = %v", err)
		}
		if buf.Len() != 0 {
			t.Errorf("got %q, want empty", buf.String())
		}
	})

	t.Run("ndjson with data", func(t *testing.T) {
		table := makeTestTable([]string{"x"}, [][]any{{42}})
		result := &axiomclient.QueryResult{Tables: []axiomclient.QueryTable{table}}
		var buf bytes.Buffer
		err := encodeResultToWriter(result, "ndjson", &buf)
		if err != nil {
			t.Fatalf("encodeResultToWriter() error = %v", err)
		}
		if !strings.Contains(buf.String(), "42") {
			t.Errorf("output should contain 42: %q", buf.String())
		}
	})

	t.Run("csv with data", func(t *testing.T) {
		table := makeTestTable([]string{"col"}, [][]any{{"val"}})
		result := &axiomclient.QueryResult{Tables: []axiomclient.QueryTable{table}}
		var buf bytes.Buffer
		err := encodeResultToWriter(result, "csv", &buf)
		if err != nil {
			t.Fatalf("encodeResultToWriter() error = %v", err)
		}
		if !strings.Contains(buf.String(), "col") || !strings.Contains(buf.String(), "val") {
			t.Errorf("output missing expected content: %q", buf.String())
		}
	})

	t.Run("unsupported format", func(t *testing.T) {
		table := makeTestTable([]string{"a"}, [][]any{{"x"}})
		result := &axiomclient.QueryResult{Tables: []axiomclient.QueryTable{table}}
		var buf bytes.Buffer
		err := encodeResultToWriter(result, "yaml", &buf)
		if err == nil {
			t.Error("expected error for unsupported format")
		}
	})
}

func TestStringify(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{"string", "hello", "hello"},
		{"bytes", []byte("world"), "world"},
		{"int", 42, "42"},
		{"float", 3.14, "3.14"},
		{"bool", true, "true"},
		{"nil", nil, "<nil>"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := stringify(tt.value)
			if got != tt.want {
				t.Errorf("stringify(%v) = %q, want %q", tt.value, got, tt.want)
			}
		})
	}
}

func TestValidateAPL(t *testing.T) {
	tests := []struct {
		name    string
		apl     string
		wantErr bool
	}{
		{"valid query", "['logs']", false},
		{"empty string", "", true},
		{"whitespace only", "   \t\n  ", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateAPL(tt.apl)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateAPL() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestBuildErrorAPL(t *testing.T) {
	t.Run("with error", func(t *testing.T) {
		got := BuildErrorAPL("['logs']", errTest)
		var payload map[string]any
		if err := json.Unmarshal(got, &payload); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if payload["apl"] != "['logs']" {
			t.Errorf("apl = %v", payload["apl"])
		}
		if payload["ok"] != false {
			t.Errorf("ok = %v, want false", payload["ok"])
		}
		if payload["error"] != "test error" {
			t.Errorf("error = %v", payload["error"])
		}
		if payload["at"] == nil {
			t.Error("at should be set")
		}
	})

	t.Run("without error", func(t *testing.T) {
		got := BuildErrorAPL("['logs']", nil)
		var payload map[string]any
		if err := json.Unmarshal(got, &payload); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if payload["ok"] != true {
			t.Errorf("ok = %v, want true", payload["ok"])
		}
		if payload["error"] != "" {
			t.Errorf("error = %v, want empty", payload["error"])
		}
	})
}

type testError struct{}

func (testError) Error() string { return "test error" }

var errTest = testError{}
