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

// Package reconciler is a lightweight controller runtime over the object
// store. Each controller watches a primary GVR (plus optional secondary
// sources with key-mapping functions), enqueues namespace/name keys into a
// rate-limited workqueue, and runs a level-triggered, idempotent reconcile
// function. It deliberately avoids a controller-runtime dependency.
package reconciler

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/util/workqueue"

	"github.com/dvaldivia/local-agent-sandbox/internal/apis"
	"github.com/dvaldivia/local-agent-sandbox/internal/store"
)

// Result controls requeue behavior after a reconcile.
type Result struct {
	// RequeueAfter, if > 0, re-enqueues the key after the delay (used for
	// time-based lifecycle, e.g. shutdownTime).
	RequeueAfter time.Duration
}

// ReconcileFunc reconciles a single object identified by namespace/name.
type ReconcileFunc func(ctx context.Context, namespace, name string) (Result, error)

// MapFunc maps an object from a secondary source to keys to enqueue on the
// owning controller's queue.
type MapFunc func(obj *unstructured.Unstructured) []string

type source struct {
	gvr   apis.GVR
	mapFn MapFunc
}

type controller struct {
	name      string
	primary   apis.GVR
	reconcile ReconcileFunc
	sources   []source
	queue     workqueue.TypedRateLimitingInterface[string]
	workers   int
}

// Manager wires controllers to the store and runs their watch/worker loops.
type Manager struct {
	store *store.Store
	log   *slog.Logger
	ctrls []*controller
}

// NewManager creates a Manager.
func NewManager(st *store.Store, log *slog.Logger) *Manager {
	if log == nil {
		log = slog.Default()
	}
	return &Manager{store: st, log: log}
}

// Register adds a controller for a primary GVR. Secondary sources (with their
// mapping functions) trigger reconciles of related primary objects.
func (m *Manager) Register(name string, primary apis.GVR, fn ReconcileFunc, workers int, sources ...source) {
	if workers <= 0 {
		workers = 2
	}
	m.ctrls = append(m.ctrls, &controller{
		name:      name,
		primary:   primary,
		reconcile: fn,
		sources:   sources,
		workers:   workers,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: name},
		),
	})
}

// Source builds a secondary watch source.
func Source(gvr apis.GVR, mapFn MapFunc) source { return source{gvr: gvr, mapFn: mapFn} }

// Enqueue adds namespace/name to the named controller's queue. Used to bridge
// external event sources (e.g. Docker lifecycle events) into a reconcile.
func (m *Manager) Enqueue(controllerName, namespace, name string) {
	for _, c := range m.ctrls {
		if c.name == controllerName {
			c.queue.Add(namespace + "/" + name)
			return
		}
	}
}

// Start launches all controllers' watches and workers, blocking until ctx is
// cancelled, after which queues are shut down and workers drain.
func (m *Manager) Start(ctx context.Context) error {
	var wg sync.WaitGroup
	for _, c := range m.ctrls {
		// Primary watch: enqueue the object's own key.
		m.startWatch(ctx, &wg, c, c.primary, func(obj *unstructured.Unstructured) []string {
			return []string{keyOf(obj)}
		})
		// Secondary watches.
		for _, s := range c.sources {
			m.startWatch(ctx, &wg, c, s.gvr, s.mapFn)
		}
		// Workers.
		for i := 0; i < c.workers; i++ {
			wg.Add(1)
			go func(c *controller) {
				defer wg.Done()
				m.runWorker(ctx, c)
			}(c)
		}
	}

	<-ctx.Done()
	for _, c := range m.ctrls {
		c.queue.ShutDown()
	}
	wg.Wait()
	return nil
}

// startWatch runs a resilient watch on gvr, enqueuing mapped keys onto c.queue.
func (m *Manager) startWatch(ctx context.Context, wg *sync.WaitGroup, c *controller, gvr apis.GVR, mapFn MapFunc) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			ch, err := m.store.Watch(ctx, gvr, store.WatchOptions{})
			if err != nil {
				m.log.Error("watch failed", "controller", c.name, "gvr", gvr.String(), "err", err)
				sleepCtx(ctx, time.Second)
				continue
			}
			for ev := range ch {
				if ev.Object == nil {
					continue
				}
				for _, k := range mapFn(ev.Object) {
					if k != "" {
						c.queue.Add(k)
					}
				}
			}
			// Channel closed; re-establish unless shutting down.
			sleepCtx(ctx, 100*time.Millisecond)
		}
	}()
}

func (m *Manager) runWorker(ctx context.Context, c *controller) {
	for {
		key, shutdown := c.queue.Get()
		if shutdown {
			return
		}
		m.process(ctx, c, key)
	}
}

func (m *Manager) process(ctx context.Context, c *controller, key string) {
	defer c.queue.Done(key)
	ns, name := splitKey(key)
	res, err := c.reconcile(ctx, ns, name)
	if err != nil {
		m.log.Error("reconcile error", "controller", c.name, "key", key, "err", err)
		c.queue.AddRateLimited(key)
		return
	}
	c.queue.Forget(key)
	if res.RequeueAfter > 0 {
		c.queue.AddAfter(key, res.RequeueAfter)
	}
}

func keyOf(obj *unstructured.Unstructured) string {
	ns := obj.GetNamespace()
	return ns + "/" + obj.GetName()
}

func splitKey(key string) (namespace, name string) {
	parts := strings.SplitN(key, "/", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", key
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
