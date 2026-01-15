package vfs

import (
	"context"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/go-git/go-billy/v5"

	"github.com/axiomhq/axiom-fs/internal/axiomclient"
	"github.com/axiomhq/axiom-fs/internal/presets"
	"github.com/axiomhq/axiom-fs/internal/query"
)

type PresetsDir struct{}

func (p *PresetsDir) Stat(ctx context.Context) (os.FileInfo, error) {
	return DirInfo("_presets"), nil
}

func (p *PresetsDir) ReadDir(ctx context.Context) ([]os.FileInfo, error) {
	entries := make([]os.FileInfo, 0)
	for _, preset := range allPresets() {
		entries = append(entries, FileInfo(preset.Name+".json", 0))
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	return entries, nil
}

func (p *PresetsDir) Lookup(ctx context.Context, name string) (Node, error) {
	base := strings.TrimSuffix(name, ".json")
	for _, preset := range allPresets() {
		if preset.Name == base {
			data := presets.MetadataJSON(preset)
			return &StaticFile{data: data}, nil
		}
	}
	return nil, os.ErrNotExist
}

type DatasetPresetsDir struct {
	root    *Root
	dataset *axiomclient.Dataset
}

func (p *DatasetPresetsDir) Stat(ctx context.Context) (os.FileInfo, error) {
	return DirInfo("presets"), nil
}

func (p *DatasetPresetsDir) ReadDir(ctx context.Context) ([]os.FileInfo, error) {
	entries := []os.FileInfo{}
	for _, preset := range presets.PresetsForDataset(p.dataset) {
		filename := preset.Name + "." + preset.Format
		entries = append(entries, FileInfo(filename, 0))
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	return entries, nil
}

func (p *DatasetPresetsDir) Lookup(ctx context.Context, name string) (Node, error) {
	base := strings.TrimSuffix(name, path.Ext(name))
	ext := strings.TrimPrefix(path.Ext(name), ".")
	for _, preset := range presets.PresetsForDataset(p.dataset) {
		if preset.Name == base && preset.Format == ext {
			return &PresetResultFile{root: p.root, dataset: p.dataset, preset: preset}, nil
		}
	}
	return nil, os.ErrNotExist
}

type PresetResultFile struct {
	root    *Root
	dataset *axiomclient.Dataset
	preset  presets.Preset
}

func (p *PresetResultFile) Stat(ctx context.Context) (os.FileInfo, error) {
	return FileInfo(p.preset.Name+"."+p.preset.Format, 0), nil
}

func (p *PresetResultFile) Open(ctx context.Context, flags int) (billy.File, error) {
	apl := presets.Render(p.preset, p.dataset.Name, p.root.Config().DefaultRange)
	result, err := p.root.Executor().ExecuteAPLResult(ctx, apl, p.preset.Format, query.ExecOptions{
		UseCache:        true,
		EnsureTimeRange: true,
		EnsureLimit:     true,
	})
	if err != nil {
		return nil, err
	}
	return openResult(result)
}
