package compiler

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"
)

func TestCompileSegments_DefaultRange(t *testing.T) {
	query, err := CompileSegments("logs", []string{
		"where", "status>=500",
		"result.csv",
	}, Options{})
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}

	if query.Format != "csv" {
		t.Fatalf("format = %q, want csv", query.Format)
	}
	if !strings.Contains(query.APL, "where _time between (ago(1h) .. now())") {
		t.Fatalf("missing default range in APL: %s", query.APL)
	}
	if !strings.Contains(query.APL, "where status>=500") {
		t.Fatalf("missing where clause in APL: %s", query.APL)
	}
	if !strings.Contains(query.APL, "take 10000") {
		t.Fatalf("missing default limit in APL: %s", query.APL)
	}
}

func TestCompileSegments_RangeAgo(t *testing.T) {
	query, err := CompileSegments("logs", []string{
		"range", "ago", "24h",
		"result.ndjson",
	}, Options{})
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if strings.Count(query.APL, "where _time between") != 1 {
		t.Fatalf("expected single range filter, got: %s", query.APL)
	}
	if !strings.Contains(query.APL, "ago(24h)") {
		t.Fatalf("missing ago range in APL: %s", query.APL)
	}
	if !strings.Contains(query.APL, "take 10000") {
		t.Fatalf("missing default limit in APL: %s", query.APL)
	}
}

func TestCompileSegments_RangeFromTo(t *testing.T) {
	query, err := CompileSegments("logs", []string{
		"range", "from", "2025-01-01T00:00:00Z", "to", "2025-01-02T00:00:00Z",
		"result.json",
	}, Options{})
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if !strings.Contains(query.APL, `datetime("2025-01-01T00:00:00Z")`) {
		t.Fatalf("missing from datetime: %s", query.APL)
	}
	if !strings.Contains(query.APL, `datetime("2025-01-02T00:00:00Z")`) {
		t.Fatalf("missing to datetime: %s", query.APL)
	}
	if !strings.Contains(query.APL, "take 10000") {
		t.Fatalf("missing default limit in APL: %s", query.APL)
	}
}

func TestCompileSegments_SummarizeBy(t *testing.T) {
	query, err := CompileSegments("logs", []string{
		"summarize", "count()", "by", "service,endpoint",
		"order", "count_:desc",
		"result.ndjson",
	}, Options{})
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if !strings.Contains(query.APL, "summarize count() by service,endpoint") {
		t.Fatalf("missing summarize by: %s", query.APL)
	}
	if !strings.Contains(query.APL, "order by count_ desc") {
		t.Fatalf("missing order by: %s", query.APL)
	}
	if !strings.Contains(query.APL, "take 10000") {
		t.Fatalf("missing default limit in APL: %s", query.APL)
	}
}

func TestDecodeExpr_Base64(t *testing.T) {
	encoded := base64.RawURLEncoding.EncodeToString([]byte("status>=500"))
	query, err := CompileSegments("logs", []string{
		"where", encoded,
		"result.ndjson",
	}, Options{})
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if !strings.Contains(query.APL, "where status>=500") {
		t.Fatalf("base64 decode failed: %s", query.APL)
	}
	if !strings.Contains(query.APL, "take 10000") {
		t.Fatalf("missing default limit in APL: %s", query.APL)
	}
}

func TestDecodeExpr_URLEncoded(t *testing.T) {
	query, err := CompileSegments("logs", []string{
		"where", "status%3E%3D500",
		"result.ndjson",
	}, Options{})
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if !strings.Contains(query.APL, "where status>=500") {
		t.Fatalf("url decode failed: %s", query.APL)
	}
	if !strings.Contains(query.APL, "take 10000") {
		t.Fatalf("missing default limit in APL: %s", query.APL)
	}
}

func TestCompileSegments_UnknownSegment(t *testing.T) {
	_, err := CompileSegments("logs", []string{
		"wat",
	}, Options{})
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestCompileQueryPath(t *testing.T) {
	query, err := CompileQueryPath("/mnt/axiom/logs/q/limit/1/result.ndjson", Options{})
	if err != nil {
		t.Fatalf("compile failed: %v", err)
	}
	if query.Dataset != "logs" {
		t.Fatalf("dataset = %q, want logs", query.Dataset)
	}
	if !strings.Contains(query.APL, "take 1") {
		t.Fatalf("missing take in APL: %s", query.APL)
	}
}

func TestCompileSegments_ErrorCases(t *testing.T) {
	tests := []struct {
		name     string
		dataset  string
		segments []string
		wantErr  string
	}{
		{
			name:     "empty dataset name",
			dataset:  "",
			segments: []string{"result.ndjson"},
			wantErr:  "dataset is required",
		},
		{
			name:     "range with invalid mode",
			dataset:  "logs",
			segments: []string{"range", "invalid", "1h"},
			wantErr:  "range mode unsupported",
		},
		{
			name:     "range/from without to",
			dataset:  "logs",
			segments: []string{"range", "from", "2025-01-01T00:00:00Z"},
			wantErr:  "range/from missing to",
		},
		{
			name:     "range/from with wrong keyword",
			dataset:  "logs",
			segments: []string{"range", "from", "2025-01-01T00:00:00Z", "until", "2025-01-02T00:00:00Z"},
			wantErr:  "range/from missing to",
		},
		{
			name:     "where without expression",
			dataset:  "logs",
			segments: []string{"where"},
			wantErr:  "where missing expression",
		},
		{
			name:     "search without term",
			dataset:  "logs",
			segments: []string{"search"},
			wantErr:  "search missing term",
		},
		{
			name:     "summarize without aggregation",
			dataset:  "logs",
			segments: []string{"summarize"},
			wantErr:  "summarize missing agg",
		},
		{
			name:     "summarize/by without fields",
			dataset:  "logs",
			segments: []string{"summarize", "count()", "by"},
			wantErr:  "summarize/by missing fields",
		},
		{
			name:     "project without fields",
			dataset:  "logs",
			segments: []string{"project"},
			wantErr:  "project missing fields",
		},
		{
			name:     "project-away without fields",
			dataset:  "logs",
			segments: []string{"project-away"},
			wantErr:  "project-away missing fields",
		},
		{
			name:     "order without field:dir",
			dataset:  "logs",
			segments: []string{"order"},
			wantErr:  "order missing field:dir",
		},
		{
			name:     "order with invalid dir",
			dataset:  "logs",
			segments: []string{"order", "field:up"},
			wantErr:  "dir must be asc or desc",
		},
		{
			name:     "order with missing dir",
			dataset:  "logs",
			segments: []string{"order", "field"},
			wantErr:  "expected field:dir",
		},
		{
			name:     "order with empty field",
			dataset:  "logs",
			segments: []string{"order", ":desc"},
			wantErr:  "field and dir required",
		},
		{
			name:     "order with empty dir",
			dataset:  "logs",
			segments: []string{"order", "field:"},
			wantErr:  "field and dir required",
		},
		{
			name:     "limit without value",
			dataset:  "logs",
			segments: []string{"limit"},
			wantErr:  "limit missing value",
		},
		{
			name:     "limit with negative value",
			dataset:  "logs",
			segments: []string{"limit", "-5"},
			wantErr:  "limit invalid",
		},
		{
			name:     "limit with non-numeric value",
			dataset:  "logs",
			segments: []string{"limit", "abc"},
			wantErr:  "limit invalid",
		},
		{
			name:     "top without by",
			dataset:  "logs",
			segments: []string{"top", "10", "field:desc"},
			wantErr:  "top requires n/by/field:dir",
		},
		{
			name:     "top with missing arguments",
			dataset:  "logs",
			segments: []string{"top", "10"},
			wantErr:  "top requires n/by/field:dir",
		},
		{
			name:     "top with invalid n",
			dataset:  "logs",
			segments: []string{"top", "abc", "by", "field:desc"},
			wantErr:  "top invalid",
		},
		{
			name:     "top with negative n",
			dataset:  "logs",
			segments: []string{"top", "-5", "by", "field:desc"},
			wantErr:  "top invalid",
		},
		{
			name:     "format with invalid format name",
			dataset:  "logs",
			segments: []string{"format", "xml"},
			wantErr:  "format invalid",
		},
		{
			name:     "format without value",
			dataset:  "logs",
			segments: []string{"format"},
			wantErr:  "format missing value",
		},
		{
			name:     "result. with invalid extension",
			dataset:  "logs",
			segments: []string{"result.xml"},
			wantErr:  "result extension invalid",
		},
		{
			name:     "range missing arguments",
			dataset:  "logs",
			segments: []string{"range"},
			wantErr:  "range missing arguments",
		},
		{
			name:     "range ago missing duration",
			dataset:  "logs",
			segments: []string{"range", "ago"},
			wantErr:  "range missing arguments",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := CompileSegments(tc.dataset, tc.segments, Options{})
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %q, want containing %q", err.Error(), tc.wantErr)
			}
		})
	}
}

func TestCompileSegments_LimitConstraints(t *testing.T) {
	t.Run("MaxLimit enforcement on limit", func(t *testing.T) {
		_, err := CompileSegments("logs", []string{"limit", "1000", "result.ndjson"}, Options{MaxLimit: 100})
		if err == nil {
			t.Fatal("expected error for limit exceeding max")
		}
		if !strings.Contains(err.Error(), "limit exceeds max") {
			t.Fatalf("error = %q, want containing 'limit exceeds max'", err.Error())
		}
	})

	t.Run("MaxLimit enforcement on top", func(t *testing.T) {
		_, err := CompileSegments("logs", []string{"top", "500", "by", "count_:desc", "result.ndjson"}, Options{MaxLimit: 100})
		if err == nil {
			t.Fatal("expected error for top exceeding max")
		}
		if !strings.Contains(err.Error(), "limit exceeds max") {
			t.Fatalf("error = %q, want containing 'limit exceeds max'", err.Error())
		}
	})

	t.Run("limit within MaxLimit is allowed", func(t *testing.T) {
		query, err := CompileSegments("logs", []string{"limit", "50", "result.ndjson"}, Options{MaxLimit: 100})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strings.Contains(query.APL, "take 50") {
			t.Fatalf("expected take 50 in APL: %s", query.APL)
		}
	})
}

func TestCompileSegments_RangeConstraints(t *testing.T) {
	t.Run("MaxRange enforcement for ago", func(t *testing.T) {
		_, err := CompileSegments("logs", []string{"range", "ago", "48h", "result.ndjson"}, Options{MaxRange: 24 * time.Hour})
		if err == nil {
			t.Fatal("expected error for range exceeding max")
		}
		if !strings.Contains(err.Error(), "range exceeds max") {
			t.Fatalf("error = %q, want containing 'range exceeds max'", err.Error())
		}
	})

	t.Run("MaxRange enforcement for from/to", func(t *testing.T) {
		_, err := CompileSegments("logs", []string{
			"range", "from", "2025-01-01T00:00:00Z", "to", "2025-01-03T00:00:00Z",
			"result.ndjson",
		}, Options{MaxRange: 24 * time.Hour})
		if err == nil {
			t.Fatal("expected error for range exceeding max")
		}
		if !strings.Contains(err.Error(), "range exceeds max") {
			t.Fatalf("error = %q, want containing 'range exceeds max'", err.Error())
		}
	})

	t.Run("range/from/to where end is before start", func(t *testing.T) {
		_, err := CompileSegments("logs", []string{
			"range", "from", "2025-01-02T00:00:00Z", "to", "2025-01-01T00:00:00Z",
			"result.ndjson",
		}, Options{MaxRange: 24 * time.Hour})
		if err == nil {
			t.Fatal("expected error for end before start")
		}
		if !strings.Contains(err.Error(), "end before start") {
			t.Fatalf("error = %q, want containing 'end before start'", err.Error())
		}
	})

	t.Run("range within MaxRange is allowed", func(t *testing.T) {
		query, err := CompileSegments("logs", []string{"range", "ago", "12h", "result.ndjson"}, Options{MaxRange: 24 * time.Hour})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !strings.Contains(query.APL, "ago(12h)") {
			t.Fatalf("expected ago(12h) in APL: %s", query.APL)
		}
	})

	t.Run("invalid ago duration format", func(t *testing.T) {
		_, err := CompileSegments("logs", []string{"range", "ago", "invalid", "result.ndjson"}, Options{MaxRange: 24 * time.Hour})
		if err == nil {
			t.Fatal("expected error for invalid duration")
		}
		if !strings.Contains(err.Error(), "invalid duration") {
			t.Fatalf("error = %q, want containing 'invalid duration'", err.Error())
		}
	})

	t.Run("invalid from time format", func(t *testing.T) {
		_, err := CompileSegments("logs", []string{
			"range", "from", "not-a-time", "to", "2025-01-02T00:00:00Z",
			"result.ndjson",
		}, Options{MaxRange: 24 * time.Hour})
		if err == nil {
			t.Fatal("expected error for invalid from time")
		}
		if !strings.Contains(err.Error(), "range/from invalid time") {
			t.Fatalf("error = %q, want containing 'range/from invalid time'", err.Error())
		}
	})

	t.Run("invalid to time format", func(t *testing.T) {
		_, err := CompileSegments("logs", []string{
			"range", "from", "2025-01-01T00:00:00Z", "to", "not-a-time",
			"result.ndjson",
		}, Options{MaxRange: 24 * time.Hour})
		if err == nil {
			t.Fatal("expected error for invalid to time")
		}
		if !strings.Contains(err.Error(), "range/to invalid time") {
			t.Fatalf("error = %q, want containing 'range/to invalid time'", err.Error())
		}
	})
}

func TestCompileSegments_EdgeCases(t *testing.T) {
	t.Run("multiple where clauses in sequence", func(t *testing.T) {
		query, err := CompileSegments("logs", []string{
			"where", "status>=400",
			"where", "service=='api'",
			"where", "duration>1000",
			"result.ndjson",
		}, Options{})
		if err != nil {
			t.Fatalf("compile failed: %v", err)
		}
		if strings.Count(query.APL, "where status>=400") != 1 {
			t.Fatalf("expected single where status>=400: %s", query.APL)
		}
		if strings.Count(query.APL, "where service=='api'") != 1 {
			t.Fatalf("expected single where service=='api': %s", query.APL)
		}
		if strings.Count(query.APL, "where duration>1000") != 1 {
			t.Fatalf("expected single where duration>1000: %s", query.APL)
		}
	})

	t.Run("project-away command", func(t *testing.T) {
		query, err := CompileSegments("logs", []string{
			"project-away", "secret,password,token",
			"result.ndjson",
		}, Options{})
		if err != nil {
			t.Fatalf("compile failed: %v", err)
		}
		if !strings.Contains(query.APL, "project-away secret,password,token") {
			t.Fatalf("expected project-away in APL: %s", query.APL)
		}
	})

	t.Run("search with special characters requiring escaping", func(t *testing.T) {
		query, err := CompileSegments("logs", []string{
			"search", `error "failed" with\slash`,
			"result.ndjson",
		}, Options{})
		if err != nil {
			t.Fatalf("compile failed: %v", err)
		}
		if !strings.Contains(query.APL, `\\`) {
			t.Fatalf("expected escaped backslash in APL: %s", query.APL)
		}
		if !strings.Contains(query.APL, `\"`) {
			t.Fatalf("expected escaped quotes in APL: %s", query.APL)
		}
	})

	t.Run("empty segments list returns dataset query with defaults", func(t *testing.T) {
		query, err := CompileSegments("logs", []string{}, Options{})
		if err != nil {
			t.Fatalf("compile failed: %v", err)
		}
		if query.Dataset != "logs" {
			t.Fatalf("dataset = %q, want logs", query.Dataset)
		}
		if !strings.Contains(query.APL, "['logs']") {
			t.Fatalf("expected dataset in APL: %s", query.APL)
		}
		if !strings.Contains(query.APL, "ago(1h)") {
			t.Fatalf("expected default range in APL: %s", query.APL)
		}
		if !strings.Contains(query.APL, "take 10000") {
			t.Fatalf("expected default limit in APL: %s", query.APL)
		}
		if query.Format != "ndjson" {
			t.Fatalf("format = %q, want ndjson", query.Format)
		}
	})

	t.Run("search with URL-encoded special chars", func(t *testing.T) {
		query, err := CompileSegments("logs", []string{
			"search", "hello%20world%26more",
			"result.ndjson",
		}, Options{})
		if err != nil {
			t.Fatalf("compile failed: %v", err)
		}
		if !strings.Contains(query.APL, "hello world&more") {
			t.Fatalf("expected decoded search term in APL: %s", query.APL)
		}
	})

	t.Run("all valid formats", func(t *testing.T) {
		for _, format := range []string{"ndjson", "json", "csv"} {
			query, err := CompileSegments("logs", []string{"result." + format}, Options{})
			if err != nil {
				t.Fatalf("compile failed for format %s: %v", format, err)
			}
			if query.Format != format {
				t.Fatalf("format = %q, want %s", query.Format, format)
			}
		}
	})

	t.Run("format command works", func(t *testing.T) {
		query, err := CompileSegments("logs", []string{"format", "csv", "result.ndjson"}, Options{})
		if err != nil {
			t.Fatalf("compile failed: %v", err)
		}
		if query.Format != "ndjson" {
			t.Fatalf("format = %q, want ndjson (result. overrides format)", query.Format)
		}
	})

	t.Run("custom default range and limit", func(t *testing.T) {
		query, err := CompileSegments("logs", []string{"result.ndjson"}, Options{
			DefaultRange: "30m",
			DefaultLimit: 500,
		})
		if err != nil {
			t.Fatalf("compile failed: %v", err)
		}
		if !strings.Contains(query.APL, "ago(30m)") {
			t.Fatalf("expected custom default range in APL: %s", query.APL)
		}
		if !strings.Contains(query.APL, "take 500") {
			t.Fatalf("expected custom default limit in APL: %s", query.APL)
		}
	})
}

func TestCompileQueryPath_Variations(t *testing.T) {
	tests := []struct {
		name        string
		path        string
		wantDataset string
		wantErr     string
	}{
		{
			name:        "standard path",
			path:        "/mnt/axiom/logs/q/limit/1/result.ndjson",
			wantDataset: "logs",
		},
		{
			name:        "path with leading/trailing slashes",
			path:        "///mnt/axiom/logs/q/limit/1/result.ndjson///",
			wantDataset: "logs",
		},
		{
			name:        "path with different mount point",
			path:        "/axiom/events/q/result.json",
			wantDataset: "events",
		},
		{
			name:        "deeply nested path",
			path:        "/a/b/c/d/my-dataset/q/where/status%3E%3D500/result.csv",
			wantDataset: "my-dataset",
		},
		{
			name:    "path too short",
			path:    "/logs",
			wantErr: "path too short",
		},
		{
			name:    "missing q segment",
			path:    "/mnt/axiom/logs/limit/1/result.ndjson",
			wantErr: "missing dataset/q",
		},
		{
			name:    "q at start without dataset",
			path:    "/q/limit/1/result.ndjson",
			wantErr: "missing dataset/q",
		},
		{
			name:        "path with whitespace trimmed",
			path:        "  /axiom/logs/q/result.ndjson  ",
			wantDataset: "logs",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			query, err := CompileQueryPath(tc.path, Options{})
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error = %q, want containing %q", err.Error(), tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if query.Dataset != tc.wantDataset {
				t.Fatalf("dataset = %q, want %q", query.Dataset, tc.wantDataset)
			}
		})
	}
}
