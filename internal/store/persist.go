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
	"encoding/binary"
	"strconv"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/dvaldivia/local-agent-sandbox/internal/apis"
)

// Persister durably records objects and the resourceVersion high-water mark.
// A nil Persister on the Store means in-memory only (used by tests). Each
// mutation is a single transaction that writes the object (or tombstone) and
// the new RV together, so a store operation performs at most one write.
type Persister interface {
	SaveObject(gvr apis.GVR, key string, data []byte, rv uint64) error
	RemoveObject(gvr apis.GVR, key string, rv uint64) error
	// Load returns all persisted objects and the stored RV high-water mark.
	Load() (map[apis.GVR]map[string][]byte, uint64, error)
	Close() error
}

var metaBucket = []byte("__meta__")
var rvKey = []byte("rv")

// BoltPersister persists objects to a bbolt database, one bucket per GVR.
// It runs with fsync disabled on the write path (NoSync) and flushes on a
// timer, so mutating operations never block on disk I/O while holding the
// store lock; on a crash at most the last flush interval is lost, which is
// acceptable for a local dev tool.
type BoltPersister struct {
	db        *bolt.DB
	done      chan struct{}
	closeOnce sync.Once
}

// OpenBolt opens (or creates) a bbolt-backed persister at path and starts the
// background flusher.
func OpenBolt(path string) (*BoltPersister, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, err
	}
	db.NoSync = true // fsync happens off the hot path via the flusher below
	p := &BoltPersister{db: db, done: make(chan struct{})}
	go p.flushLoop()
	return p, nil
}

func (p *BoltPersister) flushLoop() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-t.C:
			_ = p.db.Sync()
		}
	}
}

func bucketName(gvr apis.GVR) []byte { return []byte(gvr.String()) }

// SaveObject writes an object and the RV high-water mark in one transaction.
func (p *BoltPersister) SaveObject(gvr apis.GVR, key string, data []byte, rv uint64) error {
	return p.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucketName(gvr))
		if err != nil {
			return err
		}
		if err := b.Put([]byte(key), data); err != nil {
			return err
		}
		return putRV(tx, rv)
	})
}

// RemoveObject deletes an object and records the RV in one transaction.
func (p *BoltPersister) RemoveObject(gvr apis.GVR, key string, rv uint64) error {
	return p.db.Update(func(tx *bolt.Tx) error {
		if b := tx.Bucket(bucketName(gvr)); b != nil {
			if err := b.Delete([]byte(key)); err != nil {
				return err
			}
		}
		return putRV(tx, rv)
	})
}

func putRV(tx *bolt.Tx, rv uint64) error {
	b, err := tx.CreateBucketIfNotExists(metaBucket)
	if err != nil {
		return err
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], rv)
	return b.Put(rvKey, buf[:])
}

func (p *BoltPersister) Load() (map[apis.GVR]map[string][]byte, uint64, error) {
	out := make(map[apis.GVR]map[string][]byte)
	var rv uint64
	err := p.db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			if string(name) == string(metaBucket) {
				if v := b.Get(rvKey); len(v) == 8 {
					rv = binary.BigEndian.Uint64(v)
				}
				return nil
			}
			gvr, ok := parseGVRBucket(string(name))
			if !ok {
				return nil
			}
			m := make(map[string][]byte)
			if err := b.ForEach(func(k, v []byte) error {
				cp := make([]byte, len(v))
				copy(cp, v)
				m[string(k)] = cp
				return nil
			}); err != nil {
				return err
			}
			out[gvr] = m
			return nil
		})
	})
	return out, rv, err
}

func (p *BoltPersister) Close() error {
	p.closeOnce.Do(func() { close(p.done) })
	_ = p.db.Sync()
	return p.db.Close()
}

// parseGVRBucket parses a "group/version/resource" bucket name. The core group
// has an empty group segment, producing "/v1/pods".
func parseGVRBucket(name string) (apis.GVR, bool) {
	first, second := -1, -1
	for i := 0; i < len(name); i++ {
		if name[i] == '/' {
			if first == -1 {
				first = i
			} else {
				second = i
				break
			}
		}
	}
	if first == -1 || second == -1 {
		return apis.GVR{}, false
	}
	version := name[first+1 : second]
	resource := name[second+1:]
	if version == "" || resource == "" {
		return apis.GVR{}, false
	}
	return apis.GVR{Group: name[:first], Version: version, Resource: resource}, true
}

// parseRV parses a decimal resourceVersion string; unparseable/empty yields 0.
func parseRV(s string) uint64 {
	if s == "" {
		return 0
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0
	}
	return n
}
