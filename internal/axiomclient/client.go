package axiomclient

import (
	"context"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
	"github.com/axiomhq/axiom-go/axiom"
	"github.com/axiomhq/axiom-go/axiom/query"
)

type Client struct {
	raw *axiom.Client
}

type API interface {
	ListDatasets(ctx context.Context) ([]*axiom.Dataset, error)
	QueryAPL(ctx context.Context, apl string) (*query.Result, error)
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

func New(options ...axiom.Option) (*Client, error) {
	raw, err := axiom.NewClient(options...)
	if err != nil {
		return nil, err
	}
	return &Client{raw: raw}, nil
}

func NewWithEnvOverrides(url, token, orgID string) (*Client, error) {
	// Priority: flags > env vars > ~/.axiom.toml
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

	options := []axiom.Option{axiom.SetNoEnv()}
	if url != "" {
		options = append(options, axiom.SetURL(url))
	}
	if token != "" {
		options = append(options, axiom.SetToken(token))
	}
	if orgID != "" {
		options = append(options, axiom.SetOrganizationID(orgID))
	}

	return New(options...)
}

func (c *Client) ListDatasets(ctx context.Context) ([]*axiom.Dataset, error) {
	return c.raw.Datasets.List(ctx)
}

func (c *Client) QueryAPL(ctx context.Context, apl string) (*query.Result, error) {
	return c.raw.Datasets.Query(ctx, apl)
}
