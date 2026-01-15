package presets

import (
	"encoding/json"
	"strings"

	"github.com/axiomhq/axiom-fs/internal/axiomclient"
)

type Preset struct {
	Name         string
	Description  string
	Format       string
	Template     string
	DefaultRange string
}

type Catalog struct {
	Core    []Preset
	OTel    []Preset
	Stripe  []Preset
	Segment []Preset
}

func DefaultCatalog() Catalog {
	return Catalog{
		Core: []Preset{
			{
				Name:        "errors",
				Description: "HTTP 500+ counts by service",
				Format:      "csv",
				Template:    "['${DATASET}']\n| where _time between (${RANGE})\n| where status >= 500\n| summarize count() by service",
			},
			{
				Name:        "latency",
				Description: "Latency p50/p95/p99 by service and endpoint",
				Format:      "csv",
				Template:    "['${DATASET}']\n| where _time between (${RANGE})\n| summarize p50=percentile(duration, 50), p95=percentile(duration, 95), p99=percentile(duration, 99) by service, endpoint",
			},
			{
				Name:        "traffic",
				Description: "Request rate over time",
				Format:      "csv",
				Template:    "['${DATASET}']\n| where _time between (${RANGE})\n| summarize count() by bin_auto(_time)",
			},
			{
				Name:        "slow-requests",
				Description: "Slow requests over threshold",
				Format:      "csv",
				Template:    "['${DATASET}']\n| where _time between (${RANGE})\n| where duration > 1s\n| project _time, service, endpoint, duration\n| order by duration desc",
			},
			{
				Name:        "top-endpoints",
				Description: "Top endpoints by request volume",
				Format:      "csv",
				Template:    "['${DATASET}']\n| where _time between (${RANGE})\n| summarize count() by endpoint\n| order by count_ desc\n| take 50",
			},
		},
		OTel: []Preset{
			{
				Name:        "dependencies",
				Description: "Service-to-service call volume and latency",
				Format:      "csv",
				Template:    "['${DATASET}']\n| where _time between (${RANGE})\n| summarize count(), p95=percentile(duration, 95) by service, peer_service",
			},
			{
				Name:        "top-spans",
				Description: "Slowest spans with attributes",
				Format:      "csv",
				Template:    "['${DATASET}']\n| where _time between (${RANGE})\n| project service, span_name, duration\n| order by duration desc\n| take 50",
			},
			{
				Name:        "slo-burn",
				Description: "Error budget burn over time",
				Format:      "csv",
				Template:    "['${DATASET}']\n| where _time between (${RANGE})\n| summarize error_rate=100.0 * countif(status>=500)/count() by bin_auto(_time)",
			},
		},
		Stripe: []Preset{
			{
				Name:        "payments",
				Description: "Counts by payment status and method",
				Format:      "csv",
				Template:    "['${DATASET}']\n| where _time between (${RANGE})\n| summarize count() by status, method",
			},
			{
				Name:        "refunds",
				Description: "Refund rate over time",
				Format:      "csv",
				Template:    "['${DATASET}']\n| where _time between (${RANGE})\n| summarize refund_rate=100.0 * countif(type==\"refund\")/count() by bin_auto(_time)",
			},
			{
				Name:        "disputes",
				Description: "Dispute volume by reason",
				Format:      "csv",
				Template:    "['${DATASET}']\n| where _time between (${RANGE})\n| summarize count() by dispute_reason",
			},
			{
				Name:        "latency",
				Description: "Processing latency percentiles",
				Format:      "csv",
				Template:    "['${DATASET}']\n| where _time between (${RANGE})\n| summarize p50=percentile(duration, 50), p95=percentile(duration, 95), p99=percentile(duration, 99)",
			},
			{
				Name:        "top-customers",
				Description: "Top customers by volume",
				Format:      "csv",
				Template:    "['${DATASET}']\n| where _time between (${RANGE})\n| summarize count() by customer_id\n| order by count_ desc\n| take 50",
			},
		},
		Segment: []Preset{
			{
				Name:        "events",
				Description: "Top event names over time",
				Format:      "csv",
				Template:    "['${DATASET}']\n| where _time between (${RANGE})\n| summarize count() by bin_auto(_time), event",
			},
			{
				Name:        "sources",
				Description: "Volume by source and integration",
				Format:      "csv",
				Template:    "['${DATASET}']\n| where _time between (${RANGE})\n| summarize count() by source, integration",
			},
			{
				Name:        "schemas",
				Description: "Top fields by event type",
				Format:      "csv",
				Template:    "['${DATASET}']\n| where _time between (${RANGE})\n| summarize count() by event, field",
			},
			{
				Name:        "errors",
				Description: "Delivery failures by destination",
				Format:      "csv",
				Template:    "['${DATASET}']\n| where _time between (${RANGE})\n| where status >= 400\n| summarize count() by destination",
			},
			{
				Name:        "latency",
				Description: "Ingestion latency percentiles",
				Format:      "csv",
				Template:    "['${DATASET}']\n| where _time between (${RANGE})\n| summarize p50=percentile(duration, 50), p95=percentile(duration, 95) by source",
			},
		},
	}
}

func PresetsForDataset(dataset *axiomclient.Dataset) []Preset {
	catalog := DefaultCatalog()
	presets := append([]Preset{}, catalog.Core...)

	kind := strings.ToLower(dataset.Kind)
	name := strings.ToLower(dataset.Name)

	if strings.Contains(kind, "otel") || strings.Contains(name, "otel") || strings.Contains(name, "trace") || strings.Contains(name, "metric") || strings.Contains(name, "log") {
		presets = append(presets, catalog.OTel...)
	}
	if strings.Contains(name, "stripe") {
		presets = append(presets, catalog.Stripe...)
	}
	if strings.Contains(name, "segment") {
		presets = append(presets, catalog.Segment...)
	}

	return presets
}

func Render(preset Preset, dataset string, defaultRange string) string {
	rangeExpr := fmtRange(defaultRange)
	if preset.DefaultRange != "" {
		rangeExpr = preset.DefaultRange
	}
	replacer := strings.NewReplacer(
		"${DATASET}", dataset,
		"${RANGE}", rangeExpr,
	)
	return replacer.Replace(preset.Template)
}

func fmtRange(defaultRange string) string {
	return "ago(" + defaultRange + ") .. now()"
}

func MetadataJSON(preset Preset) []byte {
	payload := map[string]any{
		"name":        preset.Name,
		"description": preset.Description,
		"format":      preset.Format,
		"template":    preset.Template,
	}
	data, _ := json.MarshalIndent(payload, "", "  ")
	return append(data, '\n')
}
