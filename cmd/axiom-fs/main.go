package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/peterbourgon/ff/v3"
	"github.com/peterbourgon/ff/v3/ffcli"
	nfs "github.com/willscott/go-nfs"
	nfshelper "github.com/willscott/go-nfs/helpers"

	"github.com/axiomhq/axiom-fs/internal/axiomclient"
	"github.com/axiomhq/axiom-fs/internal/cache"
	"github.com/axiomhq/axiom-fs/internal/config"
	"github.com/axiomhq/axiom-fs/internal/nfsfs"
	"github.com/axiomhq/axiom-fs/internal/query"
	"github.com/axiomhq/axiom-fs/internal/vfs"
)

func main() {
	cfg := config.Default()
	fsFlagSet := flag.NewFlagSet("axiom-fs", flag.ExitOnError)

	fsFlagSet.StringVar(&cfg.ListenAddr, "listen", cfg.ListenAddr, "NFS server listen address")
	fsFlagSet.StringVar(&cfg.DefaultRange, "default-range", cfg.DefaultRange, "default range for queries (ago duration)")
	fsFlagSet.IntVar(&cfg.DefaultLimit, "default-limit", cfg.DefaultLimit, "default row limit when not specified")
	fsFlagSet.IntVar(&cfg.MaxLimit, "max-limit", cfg.MaxLimit, "maximum row limit allowed")
	fsFlagSet.DurationVar(&cfg.MaxRange, "max-range", cfg.MaxRange, "maximum allowed range duration")
	fsFlagSet.DurationVar(&cfg.CacheTTL, "cache-ttl", cfg.CacheTTL, "query cache TTL")
	fsFlagSet.IntVar(&cfg.MaxCacheEntries, "cache-max-entries", cfg.MaxCacheEntries, "max cache entries")
	fsFlagSet.IntVar(&cfg.MaxCacheBytes, "cache-max-bytes", cfg.MaxCacheBytes, "max cache size in bytes")
	fsFlagSet.IntVar(&cfg.MaxInMemoryBytes, "max-in-memory-bytes", cfg.MaxInMemoryBytes, "max in-memory result size before spilling to disk")
	fsFlagSet.StringVar(&cfg.CacheDir, "cache-dir", cfg.CacheDir, "directory for persistent query cache")
	fsFlagSet.StringVar(&cfg.QueryDir, "query-dir", cfg.QueryDir, "directory for persisted raw queries")
	fsFlagSet.StringVar(&cfg.TempDir, "temp-dir", cfg.TempDir, "temporary directory for large result files")
	fsFlagSet.IntVar(&cfg.SampleLimit, "sample-limit", cfg.SampleLimit, "sample size for sample.ndjson")
	fsFlagSet.DurationVar(&cfg.MetadataTTL, "metadata-ttl", cfg.MetadataTTL, "dataset and field cache TTL")
	fsFlagSet.StringVar(&cfg.AxiomURL, "axiom-url", "", "Axiom API base URL (overrides env)")
	fsFlagSet.StringVar(&cfg.AxiomToken, "axiom-token", "", "Axiom token (overrides env)")
	fsFlagSet.StringVar(&cfg.AxiomOrgID, "axiom-org", "", "Axiom org ID (overrides env)")

	rootCmd := &ffcli.Command{
		Name:       "axiom-fs",
		ShortUsage: "axiom-fs [flags]",
		FlagSet:    fsFlagSet,
		Options: []ff.Option{
			ff.WithEnvVarPrefix("AXIOM_FS"),
		},
		Exec: func(ctx context.Context, args []string) error {
			return run(ctx, cfg)
		},
	}

	if err := rootCmd.ParseAndRun(context.Background(), os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config.Config) error {
	client, err := axiomclient.NewWithEnvOverrides(cfg.AxiomURL, cfg.AxiomToken, cfg.AxiomOrgID)
	if err != nil {
		return err
	}

	c := cache.New(cfg.CacheTTL, cfg.MaxCacheEntries, cfg.MaxCacheBytes, cfg.CacheDir)
	exec := query.NewExecutor(client, c, cfg.DefaultRange, cfg.DefaultLimit, cfg.MaxCacheBytes, cfg.MaxInMemoryBytes, cfg.TempDir)

	root := vfs.NewRoot(cfg, client, exec)
	billyFS := nfsfs.New(root)

	// Prefetch datasets to warm cache before Finder opens
	go func() {
		if _, err := root.ReadDir(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "prefetch warning: %v\n", err)
		}
	}()

	handler := nfshelper.NewNullAuthHandler(billyFS)
	cacheHandler := nfshelper.NewCachingHandler(handler, 1024)

	listener, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return err
	}
	defer listener.Close()

	fmt.Printf("Axiom NFS server listening on %s\n", cfg.ListenAddr)
	fmt.Println()
	fmt.Println("Mount on macOS:")
	fmt.Printf("  sudo mount -t nfs -o vers=3,tcp,port=2049,mountport=2049 127.0.0.1:/ /mnt/axiom\n")
	fmt.Println()
	fmt.Println("Mount on Linux:")
	fmt.Printf("  sudo mount -t nfs -o vers=3,tcp,port=2049,mountport=2049 127.0.0.1:/ /mnt/axiom\n")
	fmt.Println()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-ctx.Done():
		case <-sigs:
		}
		fmt.Println("\nShutting down...")
		_ = listener.Close()
	}()

	return nfs.Serve(listener, cacheHandler)
}
