package catalogservice

import (
	"sync"

	"github.com/milvus-io/milvus/internal/rootcoord"
)

// Registry resolves a namespace (cluster-id) to its MetaTable. A pooled catalog service
// hosts many clusters in one process; each namespace gets its own MetaTable over its own
// TiKV prefix, which is how cluster metadata stays isolated on a shared backend.
//
// Entries are keyed by namespace AND tagged with the ownership term they were built under.
// When ownership of a namespace's shard changes (a re-claim bumps the term), Get rebuilds —
// reloading from the backend before serving — so a node that lost and re-acquired a shard
// never serves a stale in-memory view.
type Registry struct {
	build func(namespace string) (rootcoord.IMetaTable, error)

	mu    sync.Mutex
	metas map[string]*entry
}

type entry struct {
	meta rootcoord.IMetaTable
	term int64
}

// NewRegistry creates a namespace registry with the given per-namespace MetaTable builder.
func NewRegistry(build func(namespace string) (rootcoord.IMetaTable, error)) *Registry {
	return &Registry{
		build: build,
		metas: make(map[string]*entry),
	}
}

// Get returns the MetaTable for a namespace at the given ownership term, building (reloading)
// it on first use or whenever the term has changed since it was last built. term 0 means
// "ownership-agnostic" (single-tenant / no routing): the entry is cached and never reloaded.
func (r *Registry) Get(namespace string, term int64) (rootcoord.IMetaTable, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.metas[namespace]; ok && e.term == term {
		return e.meta, nil
	}
	m, err := r.build(namespace)
	if err != nil {
		return nil, err
	}
	r.metas[namespace] = &entry{meta: m, term: term}
	return m, nil
}

// Evict drops a namespace's cached MetaTable (e.g. on namespace deletion or handoff), so the
// next Get rebuilds from the backend.
func (r *Registry) Evict(namespace string) {
	r.mu.Lock()
	delete(r.metas, namespace)
	r.mu.Unlock()
}
