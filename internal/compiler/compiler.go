package compiler

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	defaultFormat = "ndjson"
)

type Options struct {
	// DefaultRange is the duration passed to ago(), e.g. "1h".
	DefaultRange string
	// DefaultLimit is the row limit appended when no limit is present.
	DefaultLimit int
	// MaxRange rejects range/ago or range/from/to longer than this duration.
	MaxRange time.Duration
	// MaxLimit rejects limit/top values larger than this.
	MaxLimit int
}

type Query struct {
	Dataset string
	APL     string
	Format  string
}

// CompileQueryPath compiles a full filesystem path to an APL query.
// The path must include a "<dataset>/q/" segment.
func CompileQueryPath(path string, opts Options) (Query, error) {
	clean := filepath.ToSlash(strings.TrimSpace(path))
	clean = strings.Trim(clean, "/")
	parts := strings.Split(clean, "/")
	if len(parts) < 2 {
		return Query{}, fmt.Errorf("path too short: %q", path)
	}

	qIndex := -1
	for i, part := range parts {
		if part == "q" {
			qIndex = i
			break
		}
	}
	if qIndex == -1 || qIndex == 0 {
		return Query{}, fmt.Errorf("missing dataset/q in path: %q", path)
	}

	dataset := parts[qIndex-1]
	segments := parts[qIndex+1:]
	return CompileSegments(dataset, segments, opts)
}

// CompileSegments compiles a list of path segments (after q/) into APL.
func CompileSegments(dataset string, segments []string, opts Options) (Query, error) {
	if dataset == "" {
		return Query{}, errors.New("dataset is required")
	}

	state := compileState{
		format: defaultFormat,
	}
	if opts.DefaultRange != "" {
		state.defaultRange = opts.DefaultRange
	} else {
		state.defaultRange = "1h"
	}
	if opts.DefaultLimit > 0 {
		state.defaultLimit = opts.DefaultLimit
	} else {
		state.defaultLimit = 10000
	}
	state.maxRange = opts.MaxRange
	state.maxLimit = opts.MaxLimit

	i := 0
	for i < len(segments) {
		seg := segments[i]
		switch seg {
		case "range":
			if i+2 >= len(segments) {
				return Query{}, fmt.Errorf("range missing arguments")
			}
			if segments[i+1] == "ago" {
				dur := segments[i+2]
				if err := checkRangeAgo(dur, state.maxRange); err != nil {
					return Query{}, err
				}
				state.addRange(rangeAgo(dur))
				i += 3
				continue
			}
			if segments[i+1] == "from" {
				if i+4 >= len(segments) || segments[i+3] != "to" {
					return Query{}, fmt.Errorf("range/from missing to")
				}
				from := segments[i+2]
				to := segments[i+4]
				if err := checkRangeFromTo(from, to, state.maxRange); err != nil {
					return Query{}, err
				}
				state.addRange(rangeFromTo(from, to))
				i += 5
				continue
			}
			return Query{}, fmt.Errorf("range mode unsupported: %q", segments[i+1])
		case "where":
			if i+1 >= len(segments) {
				return Query{}, fmt.Errorf("where missing expression")
			}
			expr, err := decodeExpr(segments[i+1])
			if err != nil {
				return Query{}, fmt.Errorf("where decode: %w", err)
			}
			state.append(fmt.Sprintf("where %s", expr))
			i += 2
			continue
		case "search":
			if i+1 >= len(segments) {
				return Query{}, fmt.Errorf("search missing term")
			}
			term, err := decodeExpr(segments[i+1])
			if err != nil {
				return Query{}, fmt.Errorf("search decode: %w", err)
			}
			state.append(fmt.Sprintf("search %q", escapeAPLString(term)))
			i += 2
			continue
		case "summarize":
			if i+1 >= len(segments) {
				return Query{}, fmt.Errorf("summarize missing agg")
			}
			agg, err := decodeExpr(segments[i+1])
			if err != nil {
				return Query{}, fmt.Errorf("summarize decode: %w", err)
			}
			if i+2 < len(segments) && segments[i+2] == "by" {
				if i+3 >= len(segments) {
					return Query{}, fmt.Errorf("summarize/by missing fields")
				}
				fields, err := decodeExpr(segments[i+3])
				if err != nil {
					return Query{}, fmt.Errorf("summarize/by decode: %w", err)
				}
				state.append(fmt.Sprintf("summarize %s by %s", agg, fields))
				i += 4
				continue
			}
			state.append(fmt.Sprintf("summarize %s", agg))
			i += 2
			continue
		case "project":
			if i+1 >= len(segments) {
				return Query{}, fmt.Errorf("project missing fields")
			}
			fields, err := decodeExpr(segments[i+1])
			if err != nil {
				return Query{}, fmt.Errorf("project decode: %w", err)
			}
			state.append(fmt.Sprintf("project %s", fields))
			i += 2
			continue
		case "project-away":
			if i+1 >= len(segments) {
				return Query{}, fmt.Errorf("project-away missing fields")
			}
			fields, err := decodeExpr(segments[i+1])
			if err != nil {
				return Query{}, fmt.Errorf("project-away decode: %w", err)
			}
			state.append(fmt.Sprintf("project-away %s", fields))
			i += 2
			continue
		case "order":
			if i+1 >= len(segments) {
				return Query{}, fmt.Errorf("order missing field:dir")
			}
			field, dir, err := splitFieldDir(segments[i+1])
			if err != nil {
				return Query{}, fmt.Errorf("order invalid: %w", err)
			}
			state.append(fmt.Sprintf("order by %s %s", field, dir))
			i += 2
			continue
		case "limit":
			if i+1 >= len(segments) {
				return Query{}, fmt.Errorf("limit missing value")
			}
			n, err := strconv.Atoi(segments[i+1])
			if err != nil || n < 0 {
				return Query{}, fmt.Errorf("limit invalid: %q", segments[i+1])
			}
			if err := checkLimit(n, state.maxLimit); err != nil {
				return Query{}, err
			}
			state.append(fmt.Sprintf("take %d", n))
			state.hasLimit = true
			i += 2
			continue
		case "top":
			if i+3 >= len(segments) || segments[i+2] != "by" {
				return Query{}, fmt.Errorf("top requires n/by/field:dir")
			}
			n, err := strconv.Atoi(segments[i+1])
			if err != nil || n < 0 {
				return Query{}, fmt.Errorf("top invalid: %q", segments[i+1])
			}
			if err := checkLimit(n, state.maxLimit); err != nil {
				return Query{}, err
			}
			field, dir, err := splitFieldDir(segments[i+3])
			if err != nil {
				return Query{}, fmt.Errorf("top invalid: %w", err)
			}
			state.append(fmt.Sprintf("top %d by %s %s", n, field, dir))
			state.hasLimit = true
			i += 4
			continue
		case "format":
			if i+1 >= len(segments) {
				return Query{}, fmt.Errorf("format missing value")
			}
			format := segments[i+1]
			if !isFormat(format) {
				return Query{}, fmt.Errorf("format invalid: %q", format)
			}
			state.format = format
			i += 2
			continue
		default:
			if strings.HasPrefix(seg, "result.") {
				ext := strings.TrimPrefix(seg, "result.")
				if !isFormat(ext) {
					return Query{}, fmt.Errorf("result extension invalid: %q", seg)
				}
				state.format = ext
				i++
				continue
			}
			return Query{}, fmt.Errorf("unknown segment: %q", seg)
		}
	}

	steps := state.steps
	if !state.hasRange {
		steps = append([]string{rangeAgo(state.defaultRange)}, steps...)
	}
	if !state.hasLimit && state.defaultLimit > 0 {
		steps = append(steps, fmt.Sprintf("take %d", state.defaultLimit))
	}

	apl := fmt.Sprintf("['%s']", dataset)
	if len(steps) > 0 {
		apl += "\n| " + strings.Join(steps, "\n| ")
	}

	return Query{
		Dataset: dataset,
		APL:     apl,
		Format:  state.format,
	}, nil
}

type compileState struct {
	steps        []string
	hasRange     bool
	hasLimit     bool
	format       string
	defaultRange string
	defaultLimit int
	maxRange     time.Duration
	maxLimit     int
}

func (s *compileState) append(step string) {
	s.steps = append(s.steps, step)
}

func (s *compileState) addRange(step string) {
	s.hasRange = true
	s.steps = append(s.steps, step)
}

func rangeAgo(dur string) string {
	return fmt.Sprintf("where _time between (ago(%s) .. now())", dur)
}

func rangeFromTo(from, to string) string {
	return fmt.Sprintf("where _time between (%s .. %s)", datetimeArg(from), datetimeArg(to))
}

func datetimeArg(value string) string {
	if strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"") {
		return fmt.Sprintf("datetime(%s)", value)
	}
	return fmt.Sprintf("datetime(%q)", value)
}

func splitFieldDir(input string) (string, string, error) {
	parts := strings.Split(input, ":")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("expected field:dir")
	}
	field := parts[0]
	dir := parts[1]
	if field == "" || dir == "" {
		return "", "", fmt.Errorf("field and dir required")
	}
	if dir != "asc" && dir != "desc" {
		return "", "", fmt.Errorf("dir must be asc or desc")
	}
	return field, dir, nil
}

func isFormat(format string) bool {
	switch format {
	case "ndjson", "csv", "json":
		return true
	default:
		return false
	}
}

func decodeExpr(input string) (string, error) {
	if input == "" {
		return "", errors.New("empty input")
	}

	decoded, err := url.PathUnescape(input)
	if err != nil {
		return "", err
	}

	if strings.Contains(input, "%") {
		return decoded, nil
	}

	if b, err := base64.RawURLEncoding.DecodeString(input); err == nil {
		if utf8.Valid(b) && base64.RawURLEncoding.EncodeToString(b) == input {
			return string(b), nil
		}
	}

	return decoded, nil
}

func escapeAPLString(input string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `"`, `\"`)
	return replacer.Replace(input)
}

func checkRangeAgo(dur string, maxRange time.Duration) error {
	if maxRange == 0 {
		return nil
	}
	parsed, err := time.ParseDuration(dur)
	if err != nil {
		return fmt.Errorf("range/ago invalid duration: %q", dur)
	}
	if parsed > maxRange {
		return fmt.Errorf("range exceeds max: %s > %s", parsed, maxRange)
	}
	return nil
}

func checkRangeFromTo(from, to string, maxRange time.Duration) error {
	if maxRange == 0 {
		return nil
	}
	start, err := time.Parse(time.RFC3339Nano, from)
	if err != nil {
		return fmt.Errorf("range/from invalid time: %q", from)
	}
	end, err := time.Parse(time.RFC3339Nano, to)
	if err != nil {
		return fmt.Errorf("range/to invalid time: %q", to)
	}
	if end.Before(start) {
		return fmt.Errorf("range invalid: end before start")
	}
	if end.Sub(start) > maxRange {
		return fmt.Errorf("range exceeds max: %s > %s", end.Sub(start), maxRange)
	}
	return nil
}

func checkLimit(n int, maxLimit int) error {
	if maxLimit > 0 && n > maxLimit {
		return fmt.Errorf("limit exceeds max: %d > %d", n, maxLimit)
	}
	return nil
}
