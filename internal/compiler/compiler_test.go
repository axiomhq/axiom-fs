package compiler

import (
	"encoding/base64"
	"strings"
	"testing"
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
