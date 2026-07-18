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

package store

import (
	"context"

	"github.com/dvaldivia/local-agent-sandbox/internal/apis"
)

// subscription is a live watch registration. Events with RV strictly greater
// than sinceRV are delivered to ch, filtered by gvr/namespace/selectors in the
// consuming goroutine.
type subscription struct {
	id  int
	gvr apis.GVR
	ch  chan Event
}

// emitLocked appends an event to history and fans it out to subscribers. If a
// subscriber's buffer is full it is dropped (its channel closed) so a slow
// watcher can never stall writers; the client observes the closed stream and
// re-lists. Caller must hold s.mu.
func (s *Store) emitLocked(ev Event) {
	s.history = append(s.history, ev)
	if len(s.history) > s.histCap {
		// Trim from the front; keep the most recent histCap events.
		drop := len(s.history) - s.histCap
		s.history = append(s.history[:0:0], s.history[drop:]...)
	}
	for id, sub := range s.subs {
		if sub.gvr != ev.GVR {
			continue
		}
		select {
		case sub.ch <- ev:
		default:
			close(sub.ch)
			delete(s.subs, id)
		}
	}
}

// oldestHistoryRV returns the RV of the earliest retained history event, or 0
// if history is empty. Caller must hold s.mu.
func (s *Store) oldestHistoryRV() uint64 {
	if len(s.history) == 0 {
		return 0
	}
	return s.history[0].RV
}

// WatchOptions configures a Watch.
type WatchOptions struct {
	Namespace string
	Selectors Selectors
	// StartRV is the resourceVersion to watch from. Zero (or "") means
	// "current state": the watcher first receives ADDED for all matching
	// objects, then live events.
	StartRV uint64
	// HasStartRV distinguishes an explicit StartRV of 0 (rare) from unset.
	HasStartRV bool
}

// Watch returns a channel of events. The channel is closed when ctx is done or
// the subscription is dropped due to slow consumption. The bootstrap (initial
// ADDED snapshot or history replay) is computed atomically with subscriber
// registration so no event is missed or duplicated across the handoff.
func (s *Store) Watch(ctx context.Context, gvr apis.GVR, opts WatchOptions) (<-chan Event, error) {
	s.mu.Lock()
	rm, err := s.meta(gvr)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}

	var bootstrap []Event
	currentRV := s.rv

	replayFromHistory := opts.HasStartRV && opts.StartRV > 0 &&
		(opts.StartRV >= s.oldestHistoryRV() || len(s.history) == 0)

	// Bootstrap events reference the stored/history object pointers directly
	// (no DeepCopy under the lock): stored objects are immutable once placed
	// (writes always replace the map entry with a fresh copy), so pump can copy
	// each event lazily outside the critical section. This keeps the O(N)
	// snapshot copy from blocking all readers/writers while a watch is set up.
	if replayFromHistory {
		for _, ev := range s.history {
			if ev.RV > opts.StartRV && ev.GVR == gvr {
				bootstrap = append(bootstrap, ev)
			}
		}
	} else {
		for _, obj := range s.data[gvr] {
			if rm.namespaced && opts.Namespace != "" && obj.GetNamespace() != opts.Namespace {
				continue
			}
			objRV := parseRV(obj.GetResourceVersion())
			if opts.HasStartRV && opts.StartRV > 0 && objRV <= opts.StartRV {
				continue
			}
			bootstrap = append(bootstrap, Event{Type: Added, Object: obj, RV: objRV, GVR: gvr})
		}
	}

	sub := &subscription{id: s.nextSub, gvr: gvr, ch: make(chan Event, 512)}
	s.nextSub++
	s.subs[sub.id] = sub
	s.mu.Unlock()

	out := make(chan Event)
	go s.pump(ctx, sub, bootstrap, currentRV, opts, out)
	return out, nil
}

// pump delivers bootstrap events then live events, applying per-watch filters.
func (s *Store) pump(ctx context.Context, sub *subscription, bootstrap []Event, currentRV uint64, opts WatchOptions, out chan<- Event) {
	defer close(out)
	defer s.removeSub(sub.id)

	rm, _ := s.metaSafe(sub.gvr)

	match := func(ev Event) bool {
		if rm.namespaced && opts.Namespace != "" && ev.Object.GetNamespace() != opts.Namespace {
			return false
		}
		// DELETED events must pass even if the object no longer matches label
		// changes; match on the object as carried in the event.
		return opts.Selectors.Matches(ev.Object)
	}

	for _, ev := range bootstrap {
		if !match(ev) {
			continue
		}
		// Copy lazily, outside the store lock, right before delivery.
		ev.Object = ev.Object.DeepCopy()
		select {
		case out <- ev:
		case <-ctx.Done():
			return
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-sub.ch:
			if !ok {
				return // dropped (slow consumer)
			}
			// Skip live events already covered by the bootstrap replay.
			if ev.RV <= currentRV {
				continue
			}
			if !match(ev) {
				continue
			}
			select {
			case out <- ev:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (s *Store) removeSub(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sub, ok := s.subs[id]; ok {
		delete(s.subs, id)
		// Do not close sub.ch here: emitLocked may close it on drop, and a
		// double close panics. Leaving it for GC is safe since no one sends
		// after removal except emitLocked (guarded by the map).
		_ = sub
	}
}

func (s *Store) metaSafe(gvr apis.GVR) (resourceMeta, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rm, ok := s.resources[gvr]
	return rm, ok
}
