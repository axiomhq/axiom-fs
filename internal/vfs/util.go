package vfs

import (
	"encoding/csv"
	"os"
	"strings"

	"github.com/axiomhq/axiom-fs/internal/axiomclient"
	"github.com/axiomhq/axiom-fs/internal/compiler"
	"github.com/axiomhq/axiom-fs/internal/config"
	"github.com/axiomhq/axiom-fs/internal/presets"
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

func schemaCSV(result *axiomclient.QueryResult) ([]byte, error) {
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

func aggregationString(agg *axiomclient.Aggregation) string {
	if agg == nil {
		return ""
	}
	op := agg.Op
	if len(agg.Fields) == 0 && len(agg.Args) == 0 {
		return op
	}
	args := append([]string{}, agg.Fields...)
	for _, arg := range agg.Args {
		switch v := arg.(type) {
		case string:
			args = append(args, v)
		default:
			args = append(args, stringify(v))
		}
	}
	return op + "(" + strings.Join(args, ", ") + ")"
}

func stringify(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		if x == float64(int64(x)) {
			return itoa(int(x))
		}
		return ftoa(x)
	case int:
		return itoa(x)
	case int64:
		return itoa(int(x))
	case bool:
		if x {
			return "true"
		}
		return "false"
	default:
		return ""
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

func ftoa(f float64) string {
	if f == 0 {
		return "0"
	}
	neg := f < 0
	if neg {
		f = -f
	}
	intPart := int64(f)
	fracPart := f - float64(intPart)
	result := itoa(int(intPart))
	if fracPart > 0 {
		result += "."
		for i := 0; i < 6 && fracPart > 0.0000001; i++ {
			fracPart *= 10
			digit := int(fracPart)
			result += string(byte('0' + digit))
			fracPart -= float64(digit)
		}
	}
	if neg {
		return "-" + result
	}
	return result
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
