package axiomclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/BurntSushi/toml"
)

// Field represents a field in a dataset.
type Field struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description,omitempty"`
	Hidden      bool   `json:"hidden,omitempty"`
	Unit        string `json:"unit,omitempty"`
}

// Dataset represents an Axiom dataset.
type Dataset struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Kind        string `json:"kind,omitempty"`
	Description string `json:"description,omitempty"`
}

// QueryResult represents the result of an APL query.
type QueryResult struct {
	Tables []QueryTable `json:"tables"`
	Status QueryStatus  `json:"status"`
}

// QueryTable represents a table in query results.
type QueryTable struct {
	Name    string       `json:"name"`
	Fields  []QueryField `json:"fields"`
	Columns [][]any      `json:"columns"`
}

// QueryField represents a field in query results.
type QueryField struct {
	Name        string       `json:"name"`
	Type        string       `json:"type"`
	Aggregation *Aggregation `json:"aggregation,omitempty"`
}

// Aggregation represents an aggregation in query results.
type Aggregation struct {
	Op     string   `json:"op"`
	Fields []string `json:"fields,omitempty"`
	Args   []any    `json:"args,omitempty"`
}

// QueryStatus represents the status of a query.
type QueryStatus struct {
	ElapsedTime    int64 `json:"elapsedTime"`
	BlocksExamined int64 `json:"blocksExamined"`
	RowsExamined   int64 `json:"rowsExamined"`
	RowsMatched    int64 `json:"rowsMatched"`
}

// User represents the current authenticated user.
type User struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

// API defines the interface for Axiom API operations.
type API interface {
	CurrentUser(ctx context.Context) (*User, error)
	ListDatasets(ctx context.Context) ([]Dataset, error)
	ListFields(ctx context.Context, datasetID string) ([]Field, error)
	QueryAPL(ctx context.Context, apl string) (*QueryResult, error)
}

// Client is an HTTP client for the Axiom API.
type Client struct {
	httpClient *http.Client
	baseURL    string
	token      string
	orgID      string
}

type axiomConfig struct {
	ActiveDeployment string                        `toml:"active_deployment"`
	Deployments      map[string]deploymentSettings `toml:"deployments"`
}

type deploymentSettings struct {
	URL   string `toml:"url"`
	Token string `toml:"token"`
	OrgID string `toml:"org_id"`
}

func loadAxiomTOML() (url, token, orgID string) {
	home, err := os.UserHomeDir()
	if err != nil {
		return
	}
	path := filepath.Join(home, ".axiom.toml")
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var cfg axiomConfig
	if err := toml.Unmarshal(data, &cfg); err != nil {
		return
	}
	if cfg.ActiveDeployment == "" {
		return
	}
	deployment, ok := cfg.Deployments[cfg.ActiveDeployment]
	if !ok {
		return
	}
	return deployment.URL, deployment.Token, deployment.OrgID
}

// New creates a new Axiom API client.
func New(baseURL, token, orgID string) (*Client, error) {
	if baseURL == "" {
		baseURL = "https://api.axiom.co"
	}
	if token == "" {
		return nil, fmt.Errorf("axiom token is required")
	}
	return &Client{
		httpClient: &http.Client{Timeout: 60 * time.Second},
		baseURL:    baseURL,
		token:      token,
		orgID:      orgID,
	}, nil
}

// NewWithEnvOverrides creates a client with configuration from flags, env, and ~/.axiom.toml.
func NewWithEnvOverrides(url, token, orgID string) (*Client, error) {
	var (
		envURL   = os.Getenv("AXIOM_URL")
		envToken = os.Getenv("AXIOM_TOKEN")
		envOrg   = os.Getenv("AXIOM_ORG_ID")
	)

	tomlURL, tomlToken, tomlOrg := loadAxiomTOML()

	if url == "" {
		url = envURL
	}
	if url == "" {
		url = tomlURL
	}

	if token == "" {
		token = envToken
	}
	if token == "" {
		token = tomlToken
	}

	if orgID == "" {
		orgID = envOrg
	}
	if orgID == "" {
		orgID = tomlOrg
	}

	return New(url, token, orgID)
}

func (c *Client) doRequest(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	if c.orgID != "" {
		req.Header.Set("X-Axiom-Org-ID", c.orgID)
	}
	return c.httpClient.Do(req)
}

type apiError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (c *Client) checkResponse(resp *http.Response) error {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var apiErr apiError
	if json.Unmarshal(body, &apiErr) == nil && apiErr.Message != "" {
		return fmt.Errorf("axiom API error %d: %s", apiErr.Code, apiErr.Message)
	}
	return fmt.Errorf("axiom API error: status %d", resp.StatusCode)
}

// CurrentUser returns the authenticated user.
func (c *Client) CurrentUser(ctx context.Context) (*User, error) {
	resp, err := c.doRequest(ctx, http.MethodGet, "/v2/user", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := c.checkResponse(resp); err != nil {
		return nil, err
	}
	var user User
	if err := json.NewDecoder(resp.Body).Decode(&user); err != nil {
		return nil, err
	}
	return &user, nil
}

// ListDatasets returns all datasets.
func (c *Client) ListDatasets(ctx context.Context) ([]Dataset, error) {
	resp, err := c.doRequest(ctx, http.MethodGet, "/v2/datasets", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := c.checkResponse(resp); err != nil {
		return nil, err
	}
	var datasets []Dataset
	if err := json.NewDecoder(resp.Body).Decode(&datasets); err != nil {
		return nil, err
	}
	return datasets, nil
}

// ListFields returns all fields for a dataset.
func (c *Client) ListFields(ctx context.Context, datasetID string) ([]Field, error) {
	resp, err := c.doRequest(ctx, http.MethodGet, "/v2/datasets/"+datasetID+"/fields", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := c.checkResponse(resp); err != nil {
		return nil, err
	}
	var fields []Field
	if err := json.NewDecoder(resp.Body).Decode(&fields); err != nil {
		return nil, err
	}
	return fields, nil
}

type queryRequest struct {
	APL string `json:"apl"`
}

// QueryAPL executes an APL query and returns the result.
func (c *Client) QueryAPL(ctx context.Context, apl string) (*QueryResult, error) {
	reqBody, err := json.Marshal(queryRequest{APL: apl})
	if err != nil {
		return nil, err
	}
	resp, err := c.doRequest(ctx, http.MethodPost, "/v1/datasets/_apl?format=tabular", bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if err := c.checkResponse(resp); err != nil {
		return nil, err
	}
	var result QueryResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}
