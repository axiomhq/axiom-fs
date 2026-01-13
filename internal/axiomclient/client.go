package axiomclient

import (
	"context"
	"os"

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

func New(options ...axiom.Option) (*Client, error) {
	raw, err := axiom.NewClient(options...)
	if err != nil {
		return nil, err
	}
	return &Client{raw: raw}, nil
}

func NewWithEnvOverrides(url, token, orgID string) (*Client, error) {
	var (
		envURL   = os.Getenv("AXIOM_URL")
		envToken = os.Getenv("AXIOM_TOKEN")
		envOrg   = os.Getenv("AXIOM_ORG_ID")
	)

	if url == "" {
		url = envURL
	}
	if token == "" {
		token = envToken
	}
	if orgID == "" {
		orgID = envOrg
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
	// Datasets.Query always requests tabular results; keep for clarity.
	return c.raw.Datasets.Query(ctx, apl)
}
