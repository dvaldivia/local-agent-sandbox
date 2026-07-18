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

// Package kubeconfig generates a kubeconfig pointing at the local lasd facade.
// The facade serves plain HTTP with no auth, which client-go, kubectl, and the
// python kubernetes client all accept — this is what lets the unmodified SDKs
// talk to lasd by only changing KUBECONFIG.
package kubeconfig

import (
	"os"
	"path/filepath"

	"k8s.io/apimachinery/pkg/runtime"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	clientcmdlatest "k8s.io/client-go/tools/clientcmd/api/latest"
)

const (
	clusterName = "lasd"
	contextName = "lasd"
	userName    = "lasd"
	namespace   = "default"
)

// Build constructs the kubeconfig API object pointing at server (an http URL).
func Build(server string) *clientcmdapi.Config {
	cfg := clientcmdapi.NewConfig()
	cfg.Clusters[clusterName] = &clientcmdapi.Cluster{Server: server}
	cfg.AuthInfos[userName] = &clientcmdapi.AuthInfo{}
	cfg.Contexts[contextName] = &clientcmdapi.Context{
		Cluster:   clusterName,
		AuthInfo:  userName,
		Namespace: namespace,
	}
	cfg.CurrentContext = contextName
	return cfg
}

// Bytes returns the serialized YAML kubeconfig for server.
func Bytes(server string) ([]byte, error) {
	cfg := Build(server)
	return runtime.Encode(clientcmdlatest.Codec, cfg)
}

// Write writes the kubeconfig for server to path (creating parent dirs).
func Write(server, path string) error {
	data, err := Bytes(server)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}
