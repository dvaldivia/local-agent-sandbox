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

// Package config holds runtime configuration for the lasd server.
package config

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
)

// Config is the resolved server configuration.
type Config struct {
	// Bind is the interface to listen on. Defaults to 127.0.0.1.
	Bind string
	// APIPort is the kube-facade port.
	APIPort int
	// RouterPort is the data-plane router port.
	RouterPort int
	// DataDir holds persistent state (bbolt db, generated kubeconfig).
	DataDir string
	// EphemeralOnShutdown tears down all managed containers/volumes on exit.
	EphemeralOnShutdown bool

	// RuntimeImage is the bundled sandbox-runtime image tag.
	RuntimeImage string
	// RuntimeBuildContext, if set, is a directory to build RuntimeImage from
	// when it is missing (dev/CI). Empty means pull-or-fail.
	RuntimeBuildContext string
	// ServerPort is the default runtime port inside sandbox containers.
	ServerPort int
	// ClusterDomain is used to synthesize status.serviceFQDN.
	ClusterDomain string
	// Bootstrap, when true, creates a default SandboxTemplate + SandboxWarmPool
	// on startup so CreateSandbox(ctx, "default") works with no YAML.
	Bootstrap bool
}

// Defaults returns the default configuration.
func Defaults() Config {
	return Config{
		Bind:          "127.0.0.1",
		APIPort:       6644,
		RouterPort:    8880,
		DataDir:       DefaultDataDir(),
		RuntimeImage:  "lasd/sandbox-runtime:dev",
		ServerPort:    8888,
		ClusterDomain: "cluster.local",
		Bootstrap:     true,
	}
}

// DefaultDataDir returns the platform data directory
// (~/.local/share/local-agent-sandbox, honoring XDG_DATA_HOME).
func DefaultDataDir() string {
	if x := os.Getenv("XDG_DATA_HOME"); x != "" {
		return filepath.Join(x, "local-agent-sandbox")
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return filepath.Join(os.TempDir(), "local-agent-sandbox")
	}
	return filepath.Join(home, ".local", "share", "local-agent-sandbox")
}

// DBPath returns the bbolt database path.
func (c Config) DBPath() string { return filepath.Join(c.DataDir, "state.db") }

// KubeconfigPath returns the generated kubeconfig path.
func (c Config) KubeconfigPath() string { return filepath.Join(c.DataDir, "kubeconfig") }

// APIServerURL returns the http URL clients should use for the kube API.
func (c Config) APIServerURL() string { return "http://" + hostPort(c.Bind, c.APIPort) }

// RouterURL returns the http URL for the data-plane router.
func (c Config) RouterURL() string { return "http://" + hostPort(c.Bind, c.RouterPort) }

func hostPort(bind string, port int) string {
	return net.JoinHostPort(bind, strconv.Itoa(port))
}
