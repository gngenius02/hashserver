package shardmap

import (
	"runtime"
	"sync"

	"github.com/cespare/xxhash"
	"github.com/tidwall/rhh"
)

type Map struct {
	init   sync.Once
	cap    int
	shards int
	mu     []sync.RWMutex
	smap   []*rhh.Map
}

func New(cap int) *Map {
	return &Map{cap: cap}
}

func (m *Map) FLUSHDB() {
	m.initMap()
	for i := 0; i < m.shards; i++ {
		m.mu[i].Lock()
		m.smap[i] = rhh.New(m.cap / m.shards)
		m.mu[i].Unlock()
	}
}

func (m *Map) SET(key string, value interface{}) (replaced bool) {
	m.initMap()
	shard := m.hashKey(key)
	m.mu[shard].Lock()
	_, replaced = m.smap[shard].Set(key, value)
	m.mu[shard].Unlock()
	return replaced
}

func (m *Map) MSET(keys ...[]string) {
	for _, pair := range keys {
		key, value := pair[0], pair[1]
		m.SET(key, value)
	}
}

func (m *Map) PMSET(keys ...[]string) (value interface{}, ok bool) {
	var wg sync.WaitGroup
	blocks, blockSize := runtime.NumCPU()*16, len(keys)/(runtime.NumCPU()*16)

	if len(keys)%blocks != 0 {
		blocks = blocks + 1
	}
	for i := 0; i < blocks; i++ {
		wg.Add(1)
		start, end := blockSize*i, blockSize*(i+1)
		if end > len(keys) {
			end = len(keys)
		}
		go func(block ...[]string) {
			defer wg.Done()
			for _, pair := range block {
				key, value := pair[0], pair[1]
				m.SET(key, value)
			}
		}(keys[start:end]...)
	}
	wg.Wait()
	return value, ok
}

func (m *Map) GET(key string) (value interface{}, ok bool) {
	m.initMap()
	shard := m.hashKey(key)
	m.mu[shard].RLock()
	value, ok = m.smap[shard].Get(key)
	m.mu[shard].RUnlock()
	return value, ok
}

func (m *Map) MGET(keys ...string) (value interface{}, ok bool) {
	for _, key := range keys {
		if v, o := m.GET(key); v != nil || o {
			return v, o
		}
	}
	return value, ok
}

func (m *Map) PMGET(keys ...string) (value interface{}, ok bool) {
	var wg sync.WaitGroup
	blocks, blockSize := runtime.NumCPU()*16, len(keys)/(runtime.NumCPU()*16)
	if len(keys)%blocks != 0 {
		blocks = blocks + 1
	}
	for i := 0; i < blocks; i++ {
		wg.Add(1)
		start, end := blockSize*i, blockSize*(i+1)
		if end > len(keys) {
			end = len(keys)
		}
		go func(block ...string) {
			defer wg.Done()
			for _, key := range block {
				if v, o := m.GET(key); v != nil || o {
					value, ok = v, true
					break
				}
			}
		}(keys[start:end]...)
	}
	wg.Wait()
	return value, ok
}

func (m *Map) DEL(key string) (prev interface{}, deleted bool) {
	m.initMap()
	shard := m.hashKey(key)
	m.mu[shard].Lock()
	prev, deleted = m.smap[shard].Delete(key)
	m.mu[shard].Unlock()
	return prev, deleted
}

func (m *Map) DBSIZE() int {
	m.initMap()
	var len int
	for i := 0; i < m.shards; i++ {
		m.mu[i].Lock()
		len += m.smap[i].Len()
		m.mu[i].Unlock()
	}
	return len
}

func (m *Map) hashKey(key string) int {
	return int(xxhash.Sum64String(key) & uint64(m.shards-1))
}

func (m *Map) initMap() {
	m.init.Do(func() {
		m.shards = 1
		for m.shards < runtime.NumCPU() {
			m.shards *= 2
		}
		scap := m.cap / m.shards
		m.mu = make([]sync.RWMutex, m.shards)
		m.smap = make([]*rhh.Map, m.shards)
		for i := 0; i < len(m.smap); i++ {
			m.smap[i] = rhh.New(scap)
		}
	})
}
