// Copyright 2026 Daniel Valdivia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Command lasd runs the local agent-sandbox control plane and data plane,
// backing sandboxes with Docker containers.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/dvaldivia/local-agent-sandbox/internal/app"
	"github.com/dvaldivia/local-agent-sandbox/internal/config"
	"github.com/dvaldivia/local-agent-sandbox/internal/kubeconfig"
)

// version is overridden via -ldflags at build time.
var version = "dev"

func main() {
	if err := rootCmd().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "lasd",
		Short:         "Local agent-sandbox controller backed by Docker",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(
		serveCmd(), kubeconfigCmd(), versionCmd(),
		statusCmd(), lsCmd(), gcCmd(), purgeCmd(), doctorCmd(),
	)
	return root
}

func newLogger(verbose bool) *slog.Logger {
	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
}

func serveCmd() *cobra.Command {
	cfg := config.Defaults()
	var verbose bool
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the local control plane and data plane",
		RunE: func(cmd *cobra.Command, _ []string) error {
			log := newLogger(verbose)
			a, err := app.New(cfg, log)
			if err != nil {
				return err
			}
			ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer stop()
			return a.Run(ctx)
		},
	}
	f := cmd.Flags()
	f.StringVar(&cfg.Bind, "bind", cfg.Bind, "interface to bind (127.0.0.1 recommended)")
	f.IntVar(&cfg.APIPort, "api-port", cfg.APIPort, "kube-facade port")
	f.IntVar(&cfg.RouterPort, "router-port", cfg.RouterPort, "data-plane router port")
	f.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "directory for persistent state")
	f.BoolVar(&cfg.EphemeralOnShutdown, "ephemeral", false, "tear down all managed containers/volumes on exit")
	f.StringVar(&cfg.RuntimeImage, "runtime-image", cfg.RuntimeImage, "bundled sandbox-runtime image tag")
	f.StringVar(&cfg.RuntimeBuildContext, "runtime-build-context", cfg.RuntimeBuildContext, "directory to build the runtime image from when missing (e.g. ./runtime)")
	f.IntVar(&cfg.ServerPort, "server-port", cfg.ServerPort, "default runtime port inside sandbox containers")
	f.StringVar(&cfg.ClusterDomain, "cluster-domain", cfg.ClusterDomain, "cluster domain used for status.serviceFQDN")
	f.BoolVar(&cfg.Bootstrap, "bootstrap", cfg.Bootstrap, "create default SandboxTemplate + SandboxWarmPool on startup")
	f.BoolVarP(&verbose, "verbose", "v", false, "verbose (debug) logging")
	// Auto-detect a runtime build context in a source checkout.
	if cfg.RuntimeBuildContext == "" {
		if _, err := os.Stat("runtime/Dockerfile"); err == nil {
			cfg.RuntimeBuildContext = "runtime"
		}
	}
	return cmd
}

func kubeconfigCmd() *cobra.Command {
	cfg := config.Defaults()
	var write bool
	var printExport bool
	cmd := &cobra.Command{
		Use:   "kubeconfig",
		Short: "Print or write a kubeconfig pointing at lasd",
		RunE: func(cmd *cobra.Command, _ []string) error {
			server := cfg.APIServerURL()
			if write {
				if err := kubeconfig.Write(server, cfg.KubeconfigPath()); err != nil {
					return err
				}
				fmt.Fprintln(os.Stderr, "wrote", cfg.KubeconfigPath())
			}
			if printExport {
				fmt.Printf("export KUBECONFIG=%s\n", cfg.KubeconfigPath())
				return nil
			}
			data, err := kubeconfig.Bytes(server)
			if err != nil {
				return err
			}
			fmt.Print(string(data))
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&cfg.Bind, "bind", cfg.Bind, "interface lasd binds")
	f.IntVar(&cfg.APIPort, "api-port", cfg.APIPort, "kube-facade port")
	f.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "data directory")
	f.BoolVar(&write, "write", false, "write the kubeconfig to the data dir")
	f.BoolVar(&printExport, "print-export", false, "print an export KUBECONFIG=... line")
	return cmd
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the lasd version",
		Run: func(*cobra.Command, []string) {
			fmt.Printf("lasd %s\n", version)
		},
	}
}
