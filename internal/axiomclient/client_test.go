package axiomclient_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/axiomhq/axiom-fs/internal/axiomclient"
)

func TestListDatasets(t *testing.T) {
	datasets := []axiomclient.Dataset{
		{ID: "ds1", Name: "logs", Description: "Application logs"},
		{ID: "ds2", Name: "metrics", Description: "System metrics"},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		if r.URL.Path != "/v2/datasets" {
			t.Errorf("expected /v2/datasets, got %s", r.URL.Path)
		}
		if auth := r.Header.Get("Authorization"); auth != "Bearer test-token" {
			t.Errorf("expected Bearer test-token, got %s", auth)
		}
		if org := r.Header.Get("X-Axiom-Org-ID"); org != "test-org" {
			t.Errorf("expected X-Axiom-Org-ID test-org, got %s", org)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(datasets)
	}))
	defer srv.Close()

	client, err := axiomclient.New(srv.URL, "test-token", "test-org")
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := context.Background()
	got, err := client.ListDatasets(ctx)
	if err != nil {
		t.Fatalf("ListDatasets: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 datasets, got %d", len(got))
	}
	if got[0].Name != "logs" {
		t.Errorf("expected logs, got %s", got[0].Name)
	}
	if got[1].Name != "metrics" {
		t.Errorf("expected metrics, got %s", got[1].Name)
	}
}

func TestListFields(t *testing.T) {
	fields := []axiomclient.Field{
		{Name: "_time", Type: "datetime"},
		{Name: "message", Type: "string", Description: "Log message"},
		{Name: "level", Type: "string", Hidden: true},
		{Name: "duration_ms", Type: "integer", Unit: "ms"},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		if r.URL.Path != "/v2/datasets/logs/fields" {
			t.Errorf("expected /v2/datasets/logs/fields, got %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(fields)
	}))
	defer srv.Close()

	client, err := axiomclient.New(srv.URL, "test-token", "test-org")
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := context.Background()
	got, err := client.ListFields(ctx, "logs")
	if err != nil {
		t.Fatalf("ListFields: %v", err)
	}

	if len(got) != 4 {
		t.Fatalf("expected 4 fields, got %d", len(got))
	}
	if got[0].Name != "_time" {
		t.Errorf("expected _time, got %s", got[0].Name)
	}
	if got[1].Description != "Log message" {
		t.Errorf("expected 'Log message', got %s", got[1].Description)
	}
	if !got[2].Hidden {
		t.Error("expected level to be hidden")
	}
	if got[3].Unit != "ms" {
		t.Errorf("expected unit 'ms', got %s", got[3].Unit)
	}
}

func TestQueryAPL(t *testing.T) {
	result := axiomclient.QueryResult{
		Tables: []axiomclient.QueryTable{
			{
				Name: "result",
				Fields: []axiomclient.QueryField{
					{Name: "_time", Type: "datetime"},
					{Name: "count_", Type: "integer"},
				},
				Columns: [][]any{
					{"2024-01-15T10:00:00Z", "2024-01-15T11:00:00Z"},
					{float64(100), float64(200)},
				},
			},
		},
		Status: axiomclient.QueryStatus{
			ElapsedTime:    150,
			BlocksExamined: 10,
			RowsExamined:   1000,
			RowsMatched:    2,
		},
	}

	var capturedAPL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/v2/datasets/_apl" {
			t.Errorf("expected /v2/datasets/_apl, got %s", r.URL.Path)
		}
		if !strings.Contains(r.URL.RawQuery, "format=tabular") {
			t.Errorf("expected format=tabular in query, got %s", r.URL.RawQuery)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("expected Content-Type application/json, got %s", ct)
		}

		var req struct {
			APL string `json:"apl"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("failed to decode request: %v", err)
		}
		capturedAPL = req.APL

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	}))
	defer srv.Close()

	client, err := axiomclient.New(srv.URL, "test-token", "test-org")
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := context.Background()
	apl := "['logs'] | summarize count() by bin(_time, 1h)"
	got, err := client.QueryAPL(ctx, apl)
	if err != nil {
		t.Fatalf("QueryAPL: %v", err)
	}

	if capturedAPL != apl {
		t.Errorf("expected APL %q, got %q", apl, capturedAPL)
	}
	if len(got.Tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(got.Tables))
	}
	if len(got.Tables[0].Fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(got.Tables[0].Fields))
	}
	if got.Status.RowsMatched != 2 {
		t.Errorf("expected 2 rows matched, got %d", got.Status.RowsMatched)
	}
}

func TestAPIErrorHandling(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       string
		wantErr    string
	}{
		{
			name:       "structured error",
			statusCode: 403,
			body:       `{"code":403,"message":"forbidden"}`,
			wantErr:    "axiom API error 403: forbidden",
		},
		{
			name:       "unstructured error",
			statusCode: 500,
			body:       "internal server error",
			wantErr:    "axiom API error: status 500",
		},
		{
			name:       "bad request",
			statusCode: 400,
			body:       `{"code":400,"message":"invalid APL query"}`,
			wantErr:    "axiom API error 400: invalid APL query",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)
				w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			client, err := axiomclient.New(srv.URL, "test-token", "test-org")
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			ctx := context.Background()
			_, err = client.ListDatasets(ctx)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("expected error containing %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

func TestNewClientValidation(t *testing.T) {
	_, err := axiomclient.New("https://api.axiom.co", "", "org")
	if err == nil {
		t.Error("expected error for empty token")
	}
}

func TestNewClientDefaults(t *testing.T) {
	client, err := axiomclient.New("", "token", "org")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if client == nil {
		t.Fatal("expected non-nil client")
	}
}

func TestContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		json.NewEncoder(w).Encode([]axiomclient.Dataset{})
	}))
	defer srv.Close()

	client, err := axiomclient.New(srv.URL, "test-token", "test-org")
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = client.ListDatasets(ctx)
	if err == nil {
		t.Error("expected error for cancelled context")
	}
}

func TestContextTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		json.NewEncoder(w).Encode([]axiomclient.Dataset{})
	}))
	defer srv.Close()

	client, err := axiomclient.New(srv.URL, "test-token", "test-org")
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err = client.ListDatasets(ctx)
	if err == nil {
		t.Error("expected error for timeout")
	}
}

func TestOrgIDOptional(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if org := r.Header.Get("X-Axiom-Org-ID"); org != "" {
			t.Errorf("expected no X-Axiom-Org-ID header, got %s", org)
		}
		json.NewEncoder(w).Encode([]axiomclient.Dataset{})
	}))
	defer srv.Close()

	client, err := axiomclient.New(srv.URL, "test-token", "")
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := context.Background()
	_, err = client.ListDatasets(ctx)
	if err != nil {
		t.Fatalf("ListDatasets: %v", err)
	}
}

func TestEmptyResponses(t *testing.T) {
	t.Run("empty datasets", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode([]axiomclient.Dataset{})
		}))
		defer srv.Close()

		client, _ := axiomclient.New(srv.URL, "token", "org")
		got, err := client.ListDatasets(context.Background())
		if err != nil {
			t.Fatalf("ListDatasets: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("expected empty slice, got %d", len(got))
		}
	})

	t.Run("empty fields", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode([]axiomclient.Field{})
		}))
		defer srv.Close()

		client, _ := axiomclient.New(srv.URL, "token", "org")
		got, err := client.ListFields(context.Background(), "logs")
		if err != nil {
			t.Fatalf("ListFields: %v", err)
		}
		if len(got) != 0 {
			t.Errorf("expected empty slice, got %d", len(got))
		}
	})

	t.Run("empty query result", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode(axiomclient.QueryResult{Tables: []axiomclient.QueryTable{}})
		}))
		defer srv.Close()

		client, _ := axiomclient.New(srv.URL, "token", "org")
		got, err := client.QueryAPL(context.Background(), "['logs']")
		if err != nil {
			t.Fatalf("QueryAPL: %v", err)
		}
		if len(got.Tables) != 0 {
			t.Errorf("expected no tables, got %d", len(got.Tables))
		}
	})
}

func TestSpecialCharactersInDatasetName(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/datasets/my-logs-2024/fields" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		json.NewEncoder(w).Encode([]axiomclient.Field{})
	}))
	defer srv.Close()

	client, _ := axiomclient.New(srv.URL, "token", "org")
	_, err := client.ListFields(context.Background(), "my-logs-2024")
	if err != nil {
		t.Fatalf("ListFields: %v", err)
	}
}

func TestQueryWithAggregation(t *testing.T) {
	result := axiomclient.QueryResult{
		Tables: []axiomclient.QueryTable{
			{
				Fields: []axiomclient.QueryField{
					{
						Name: "count_",
						Type: "integer",
						Aggregation: &axiomclient.Aggregation{
							Op:     "count",
							Fields: []string{},
						},
					},
				},
				Columns: [][]any{{float64(42)}},
			},
		},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(result)
	}))
	defer srv.Close()

	client, _ := axiomclient.New(srv.URL, "token", "org")
	got, err := client.QueryAPL(context.Background(), "['logs'] | summarize count()")
	if err != nil {
		t.Fatalf("QueryAPL: %v", err)
	}

	if got.Tables[0].Fields[0].Aggregation == nil {
		t.Error("expected aggregation to be set")
	}
	if got.Tables[0].Fields[0].Aggregation.Op != "count" {
		t.Errorf("expected count aggregation, got %s", got.Tables[0].Fields[0].Aggregation.Op)
	}
}
