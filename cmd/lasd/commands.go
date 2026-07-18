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

package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/dvaldivia/local-agent-sandbox/internal/apis"
	"github.com/dvaldivia/local-agent-sandbox/internal/config"
	"github.com/dvaldivia/local-agent-sandbox/internal/driver"
)

// dynClient builds a dynamic client from the lasd kubeconfig (or $KUBECONFIG).
func dynClient(cfg config.Config) (dynamic.Interface, error) {
	loading := clientcmd.NewDefaultClientConfigLoadingRules()
	if os.Getenv("KUBECONFIG") == "" {
		loading.ExplicitPath = cfg.KubeconfigPath()
	}
	restCfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loading, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, err
	}
	return dynamic.NewForConfig(restCfg)
}

func doctorCmd() *cobra.Command {
	cfg := config.Defaults()
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Diagnose the local environment (Docker, kubectl, ports)",
		RunE: func(*cobra.Command, []string) error {
			ok := true
			// Docker.
			if d, err := driver.NewDockerDriver(); err != nil {
				fmt.Printf("✗ docker client: %v\n", err)
				ok = false
			} else {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if perr := d.Ping(ctx); perr != nil {
					fmt.Printf("✗ docker daemon unreachable: %v\n", perr)
					ok = false
				} else {
					fmt.Println("✓ docker daemon reachable")
				}
				_ = d.Close()
			}
			// kubectl.
			if _, err := exec.LookPath("kubectl"); err != nil {
				fmt.Println("• kubectl not found (optional; needed for kubectl/python tunnel mode)")
			} else {
				fmt.Println("✓ kubectl found")
			}
			// Ports.
			for _, p := range []int{cfg.APIPort, cfg.RouterPort} {
				addr := net.JoinHostPort(cfg.Bind, itoac(p))
				if ln, err := net.Listen("tcp", addr); err != nil {
					fmt.Printf("✗ port %s in use: %v\n", addr, err)
					ok = false
				} else {
					ln.Close()
					fmt.Printf("✓ port %s available\n", addr)
				}
			}
			// Data dir.
			if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
				fmt.Printf("✗ data dir %s: %v\n", cfg.DataDir, err)
				ok = false
			} else {
				fmt.Printf("✓ data dir %s writable\n", cfg.DataDir)
			}
			if !ok {
				return fmt.Errorf("doctor found problems")
			}
			return nil
		},
	}
	bindServerFlags(cmd, &cfg)
	return cmd
}

func statusCmd() *cobra.Command {
	cfg := config.Defaults()
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show server health and resource counts",
		RunE: func(*cobra.Command, []string) error {
			dc, err := dynClient(cfg)
			if err != nil {
				return fmt.Errorf("connect to lasd (is it running?): %w", err)
			}
			ctx := context.Background()
			counts := map[string]int{}
			for name, gvr := range map[string]apis.GVR{
				"templates": apis.TemplateGVR, "warmpools": apis.WarmPoolGVR,
				"claims": apis.SandboxClaimGVR, "sandboxes": apis.SandboxGVR,
			} {
				list, lerr := dc.Resource(toSchemaGVR(gvr)).List(ctx, metav1.ListOptions{})
				if lerr == nil {
					counts[name] = len(list.Items)
				}
			}
			fmt.Printf("API:    %s\n", cfg.APIServerURL())
			fmt.Printf("Router: %s\n", cfg.RouterURL())
			fmt.Printf("templates=%d warmpools=%d claims=%d sandboxes=%d\n",
				counts["templates"], counts["warmpools"], counts["claims"], counts["sandboxes"])
			if d, derr := driver.NewDockerDriver(); derr == nil {
				cs, _ := d.ListManaged(ctx)
				fmt.Printf("managed containers: %d\n", len(cs))
				_ = d.Close()
			}
			return nil
		},
	}
	bindServerFlags(cmd, &cfg)
	return cmd
}

func lsCmd() *cobra.Command {
	cfg := config.Defaults()
	cmd := &cobra.Command{
		Use:   "ls",
		Short: "List sandboxes and their containers",
		RunE: func(*cobra.Command, []string) error {
			dc, err := dynClient(cfg)
			if err != nil {
				return fmt.Errorf("connect to lasd (is it running?): %w", err)
			}
			ctx := context.Background()
			list, err := dc.Resource(toSchemaGVR(apis.SandboxGVR)).Namespace("").List(ctx, metav1.ListOptions{})
			if err != nil {
				return err
			}
			ports := map[string]int{}
			state := map[string]string{}
			if d, derr := driver.NewDockerDriver(); derr == nil {
				if cs, cerr := d.ListManaged(ctx); cerr == nil {
					for _, c := range cs {
						key := c.Namespace() + "/" + c.Sandbox()
						state[key] = c.State
						for cp, hp := range c.PortMap {
							if cp == cfg.ServerPort {
								ports[key] = hp
							}
						}
					}
				}
				_ = d.Close()
			}
			w := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
			fmt.Fprintln(w, "NAMESPACE\tNAME\tREADY\tCONTAINER\tHOSTPORT")
			for _, sb := range list.Items {
				ns, name := sb.GetNamespace(), sb.GetName()
				key := ns + "/" + name
				ready := conditionStatus(sb.Object, "Ready")
				hp := ""
				if p, ok := ports[key]; ok {
					hp = itoac(p)
				}
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n", ns, name, ready, state[key], hp)
			}
			return w.Flush()
		},
	}
	bindServerFlags(cmd, &cfg)
	return cmd
}

func gcCmd() *cobra.Command {
	cfg := config.Defaults()
	var dryRun bool
	cmd := &cobra.Command{
		Use:   "gc",
		Short: "Remove containers/volumes with no backing Sandbox CR",
		RunE: func(*cobra.Command, []string) error {
			d, err := driver.NewDockerDriver()
			if err != nil {
				return err
			}
			defer d.Close()
			ctx := context.Background()
			dc, err := dynClient(cfg)
			if err != nil {
				return fmt.Errorf("connect to lasd (is it running?): %w", err)
			}
			exists := func(ns, name string) bool {
				_, gerr := dc.Resource(toSchemaGVR(apis.SandboxGVR)).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
				return gerr == nil
			}
			cs, err := d.ListManaged(ctx)
			if err != nil {
				return err
			}
			removed := 0
			for _, c := range cs {
				if c.Sandbox() == "" || exists(c.Namespace(), c.Sandbox()) {
					continue
				}
				fmt.Printf("orphan container %s (%s/%s)\n", c.Name, c.Namespace(), c.Sandbox())
				if !dryRun {
					_ = d.RemoveContainer(ctx, c.ID)
				}
				removed++
			}
			fmt.Printf("%d orphaned containers%s\n", removed, dryRunLabel(dryRun))
			return nil
		},
	}
	bindServerFlags(cmd, &cfg)
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "report without removing")
	return cmd
}

func purgeCmd() *cobra.Command {
	cfg := config.Defaults()
	var withState bool
	cmd := &cobra.Command{
		Use:   "purge",
		Short: "Remove ALL lasd-managed containers and volumes",
		RunE: func(*cobra.Command, []string) error {
			d, err := driver.NewDockerDriver()
			if err != nil {
				return err
			}
			defer d.Close()
			ctx := context.Background()
			cs, _ := d.ListManaged(ctx)
			for _, c := range cs {
				_ = d.RemoveContainer(ctx, c.ID)
			}
			vs, _ := d.ListManagedVolumes(ctx)
			for _, v := range vs {
				_ = d.RemoveVolume(ctx, v.Name)
			}
			fmt.Printf("removed %d containers, %d volumes\n", len(cs), len(vs))
			if withState {
				if err := os.Remove(cfg.DBPath()); err != nil && !os.IsNotExist(err) {
					return err
				}
				fmt.Println("removed state db")
			}
			return nil
		},
	}
	bindServerFlags(cmd, &cfg)
	cmd.Flags().BoolVar(&withState, "state", false, "also delete the persisted state db")
	return cmd
}

func bindServerFlags(cmd *cobra.Command, cfg *config.Config) {
	f := cmd.Flags()
	f.StringVar(&cfg.Bind, "bind", cfg.Bind, "interface lasd binds")
	f.IntVar(&cfg.APIPort, "api-port", cfg.APIPort, "kube-facade port")
	f.IntVar(&cfg.RouterPort, "router-port", cfg.RouterPort, "router port")
	f.StringVar(&cfg.DataDir, "data-dir", cfg.DataDir, "data directory")
	f.IntVar(&cfg.ServerPort, "server-port", cfg.ServerPort, "runtime port inside containers")
}

func dryRunLabel(dry bool) string {
	if dry {
		return " (dry-run)"
	}
	return " removed"
}

func itoac(i int) string { return fmt.Sprintf("%d", i) }

func toSchemaGVR(g apis.GVR) schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: g.Group, Version: g.Version, Resource: g.Resource}
}

// conditionStatus returns the status string of the named condition, or "-".
func conditionStatus(obj map[string]any, condType string) string {
	conds, found, _ := unstructured.NestedSlice(obj, "status", "conditions")
	if !found {
		return "-"
	}
	for _, c := range conds {
		m, ok := c.(map[string]any)
		if ok && m["type"] == condType {
			if s, ok := m["status"].(string); ok {
				return s
			}
		}
	}
	return "-"
}
