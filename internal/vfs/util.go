package vfs

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strings"

	axiomquery "github.com/axiomhq/axiom-go/axiom/query"

	"github.com/axiomhq/axiom-fs/internal/compiler"
	"github.com/axiomhq/axiom-fs/internal/config"
	"github.com/axiomhq/axiom-fs/internal/presets"
	"github.com/axiomhq/axiom-fs/internal/query"
)

func compilePath(dataset string, segments []string, cfg config.Config) (compiler.Query, error) {
	if len(segments) > 0 && segments[len(segments)-1] == "result.error" {
		segments = append([]string{}, segments[:len(segments)-1]...)
		segments = append(segments, "result.ndjson")
	}
	opts := compiler.Options{
		DefaultRange: cfg.DefaultRange,
		DefaultLimit: cfg.DefaultLimit,
		MaxRange:     cfg.MaxRange,
		MaxLimit:     cfg.MaxLimit,
	}
	return compiler.CompileSegments(dataset, segments, opts)
}

var readmeText = []byte(`Axiom NFS FS

Most useful:
  /<dataset>/presets/*.csv

Advanced:
  /<dataset>/q/<...>/result.ndjson

Raw APL:
  /_queries/<name>/apl
`)

var exampleText = []byte(`Example query:
/mnt/axiom/logs/q/range/ago/1h/where/status>=500/summarize/count()/by/service/order/count_:desc/limit/50/result.csv
`)

func fetchFields(ctx context.Context, root *Root, dataset string) ([]string, error) {
	apl := fmt.Sprintf("['%s']\n| where _time between (ago(%s) .. now())\n| getschema",
		dataset,
		root.Config().DefaultRange,
	)
	result, err := root.Executor().QueryAPL(ctx, apl, query.ExecOptions{})
	if err != nil {
		return nil, err
	}
	return extractFieldNames(result), nil
}

func extractFieldNames(result *axiomquery.Result) []string {
	if result == nil || len(result.Tables) == 0 {
		return nil
	}
	table := result.Tables[0]
	if len(table.Columns) == 0 {
		return nil
	}
	index := -1
	for i, field := range table.Fields {
		switch strings.ToLower(field.Name) {
		case "name", "field", "column", "key":
			index = i
		}
	}
	if index == -1 {
		index = 0
	}
	if index >= len(table.Columns) {
		return nil
	}
	column := table.Columns[index]
	unique := make(map[string]struct{})
	for _, value := range column {
		name := strings.TrimSpace(fmt.Sprint(value))
		if name == "" {
			continue
		}
		unique[name] = struct{}{}
	}
	names := make([]string, 0, len(unique))
	for name := range unique {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func schemaCSV(result *axiomquery.Result) ([]byte, error) {
	if len(result.Tables) == 0 {
		return []byte{}, nil
	}
	fields := result.Tables[0].Fields
	var buf strings.Builder
	writer := csv.NewWriter(&buf)
	if err := writer.Write([]string{"name", "type", "aggregation"}); err != nil {
		return nil, err
	}
	for _, field := range fields {
		agg := ""
		if field.Aggregation != nil {
			agg = aggregationString(field.Aggregation)
		}
		if err := writer.Write([]string{field.Name, field.Type, agg}); err != nil {
			return nil, err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, err
	}
	return []byte(buf.String()), nil
}

func aggregationString(agg *axiomquery.Aggregation) string {
	if agg == nil {
		return ""
	}
	op := agg.Op.String()
	if len(agg.Fields) == 0 && len(agg.Args) == 0 {
		return op
	}
	args := append([]string{}, agg.Fields...)
	for _, arg := range agg.Args {
		args = append(args, fmt.Sprint(arg))
	}
	return fmt.Sprintf("%s(%s)", op, strings.Join(args, ", "))
}

func allPresets() []presets.Preset {
	catalog := presets.DefaultCatalog()
	list := append([]presets.Preset{}, catalog.Core...)
	list = append(list, catalog.OTel...)
	list = append(list, catalog.Stripe...)
	list = append(list, catalog.Segment...)
	return list
}

func isValidQueryName(name string) bool {
	if name == "" {
		return false
	}
	if strings.Contains(name, "/") || strings.Contains(name, string(os.PathSeparator)) {
		return false
	}
	if strings.Contains(name, "..") {
		return false
	}
	return true
}
