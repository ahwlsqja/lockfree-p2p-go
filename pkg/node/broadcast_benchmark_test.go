package node

import (
	"crypto/sha256"
	"encoding/hex"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/go-p2p-network/go-p2p/pkg/protocol"
)

// =============================================================================
// 해시 함수 벤치마크: SHA256 vs xxhash
// =============================================================================

// BenchmarkHash_SHA256은 SHA256 + hex.EncodeToString 성능을 측정합니다.
func BenchmarkHash_SHA256(b *testing.B) {
	msg := &protocol.Message{
		Type:    protocol.MsgTx,
		Payload: make([]byte, 256), // 일반적인 트랜잭션 크기
	}
	rand.Read(msg.Payload)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hasher := sha256.New()
		hasher.Write([]byte{byte(msg.Type)})
		hasher.Write(msg.Payload)
		_ = hex.EncodeToString(hasher.Sum(nil))
	}
}

// BenchmarkHash_XXHash는 xxhash64 성능을 측정합니다.
func BenchmarkHash_XXHash(b *testing.B) {
	msg := &protocol.Message{
		Type:    protocol.MsgTx,
		Payload: make([]byte, 256),
	}
	rand.Read(msg.Payload)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := xxhash.New()
		h.Write([]byte{byte(msg.Type)})
		h.Write(msg.Payload)
		_ = h.Sum64()
	}
}

// BenchmarkHash_XXHashDirect는 xxhash.Sum64 직접 호출 성능을 측정합니다.
func BenchmarkHash_XXHashDirect(b *testing.B) {
	msg := &protocol.Message{
		Type:    protocol.MsgTx,
		Payload: make([]byte, 256),
	}
	rand.Read(msg.Payload)

	// 미리 버퍼 준비
	buf := make([]byte, 1+len(msg.Payload))
	buf[0] = byte(msg.Type)
	copy(buf[1:], msg.Payload)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = xxhash.Sum64(buf)
	}
}

// =============================================================================
// sync.Map vs RWMutex 벤치마크
// =============================================================================

// seenCacheSyncMap은 sync.Map 기반 구현입니다.
type seenCacheSyncMap struct {
	m     sync.Map
	count int64
}

func (c *seenCacheSyncMap) Has(key uint64) bool {
	_, ok := c.m.Load(key)
	return ok
}

func (c *seenCacheSyncMap) MarkSeen(key uint64) bool {
	_, loaded := c.m.LoadOrStore(key, time.Now())
	return !loaded
}

// seenCacheRWMutex는 RWMutex + map 기반 구현입니다.
type seenCacheRWMutex struct {
	m  map[uint64]time.Time
	mu sync.RWMutex
}

func newSeenCacheRWMutex() *seenCacheRWMutex {
	return &seenCacheRWMutex{
		m: make(map[uint64]time.Time),
	}
}

func (c *seenCacheRWMutex) Has(key uint64) bool {
	c.mu.RLock()
	_, ok := c.m[key]
	c.mu.RUnlock()
	return ok
}

func (c *seenCacheRWMutex) MarkSeen(key uint64) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.m[key]; ok {
		return false
	}
	c.m[key] = time.Now()
	return true
}

// -----------------------------------------------------------------------------
// 시나리오 1: Read-Heavy (90% 읽기, 10% 쓰기)
// 대부분의 메시지가 이미 본 것인 상황 (일반적인 P2P 네트워크)
// -----------------------------------------------------------------------------

func BenchmarkSeenCache_SyncMap_ReadHeavy(b *testing.B) {
	cache := &seenCacheSyncMap{}

	// 미리 10000개 키 삽입
	for i := uint64(0); i < 10000; i++ {
		cache.MarkSeen(i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := uint64(0)
		for pb.Next() {
			key := i % 10000
			if i%10 == 0 {
				// 10% 쓰기 (새 키)
				cache.MarkSeen(10000 + i)
			} else {
				// 90% 읽기 (기존 키)
				cache.Has(key)
			}
			i++
		}
	})
}

func BenchmarkSeenCache_RWMutex_ReadHeavy(b *testing.B) {
	cache := newSeenCacheRWMutex()

	// 미리 10000개 키 삽입
	for i := uint64(0); i < 10000; i++ {
		cache.MarkSeen(i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := uint64(0)
		for pb.Next() {
			key := i % 10000
			if i%10 == 0 {
				cache.MarkSeen(10000 + i)
			} else {
				cache.Has(key)
			}
			i++
		}
	})
}

// -----------------------------------------------------------------------------
// 시나리오 2: Write-Heavy (50% 읽기, 50% 쓰기)
// 새 메시지가 많이 들어오는 상황
// -----------------------------------------------------------------------------

func BenchmarkSeenCache_SyncMap_WriteHeavy(b *testing.B) {
	cache := &seenCacheSyncMap{}

	for i := uint64(0); i < 10000; i++ {
		cache.MarkSeen(i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := uint64(0)
		for pb.Next() {
			key := i % 10000
			if i%2 == 0 {
				cache.MarkSeen(10000 + i)
			} else {
				cache.Has(key)
			}
			i++
		}
	})
}

func BenchmarkSeenCache_RWMutex_WriteHeavy(b *testing.B) {
	cache := newSeenCacheRWMutex()

	for i := uint64(0); i < 10000; i++ {
		cache.MarkSeen(i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := uint64(0)
		for pb.Next() {
			key := i % 10000
			if i%2 == 0 {
				cache.MarkSeen(10000 + i)
			} else {
				cache.Has(key)
			}
			i++
		}
	})
}

// -----------------------------------------------------------------------------
// 시나리오 3: Read-Only (100% 읽기)
// 모든 메시지가 이미 본 것인 상황 (안정화된 네트워크)
// -----------------------------------------------------------------------------

func BenchmarkSeenCache_SyncMap_ReadOnly(b *testing.B) {
	cache := &seenCacheSyncMap{}

	for i := uint64(0); i < 10000; i++ {
		cache.MarkSeen(i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := uint64(0)
		for pb.Next() {
			cache.Has(i % 10000)
			i++
		}
	})
}

func BenchmarkSeenCache_RWMutex_ReadOnly(b *testing.B) {
	cache := newSeenCacheRWMutex()

	for i := uint64(0); i < 10000; i++ {
		cache.MarkSeen(i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := uint64(0)
		for pb.Next() {
			cache.Has(i % 10000)
			i++
		}
	})
}

// =============================================================================
// RNG 벤치마크: 전역 rand vs goroutine-local rand
// =============================================================================

func BenchmarkRNG_Global(b *testing.B) {
	slice := make([]int, 50)
	for i := range slice {
		slice[i] = i
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		localSlice := make([]int, len(slice))
		for pb.Next() {
			copy(localSlice, slice)
			rand.Shuffle(len(localSlice), func(i, j int) {
				localSlice[i], localSlice[j] = localSlice[j], localSlice[i]
			})
		}
	})
}

func BenchmarkRNG_Local(b *testing.B) {
	slice := make([]int, 50)
	for i := range slice {
		slice[i] = i
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// 각 goroutine마다 별도 RNG
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		localSlice := make([]int, len(slice))
		for pb.Next() {
			copy(localSlice, slice)
			rng.Shuffle(len(localSlice), func(i, j int) {
				localSlice[i], localSlice[j] = localSlice[j], localSlice[i]
			})
		}
	})
}

// =============================================================================
// 통합 벤치마크: MessageHash + MarkSeen 전체 플로우
// =============================================================================

func BenchmarkFullFlow_Old(b *testing.B) {
	// 이전 방식: SHA256 + string key + RWMutex
	type oldCache struct {
		m  map[string]time.Time
		mu sync.RWMutex
	}
	cache := &oldCache{m: make(map[string]time.Time)}

	msg := &protocol.Message{
		Type:    protocol.MsgTx,
		Payload: make([]byte, 256),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// SHA256 해시
		hasher := sha256.New()
		hasher.Write([]byte{byte(msg.Type)})
		hasher.Write(msg.Payload)
		hash := hex.EncodeToString(hasher.Sum(nil))

		// MarkSeen (RWMutex)
		cache.mu.Lock()
		if _, ok := cache.m[hash]; !ok {
			cache.m[hash] = time.Now()
		}
		cache.mu.Unlock()

		// 페이로드 변경 (다른 메시지 시뮬레이션)
		msg.Payload[0] = byte(i)
	}
}

func BenchmarkFullFlow_New(b *testing.B) {
	// 새 방식: xxhash + uint64 key + sync.Map
	cache := &seenCacheSyncMap{}

	msg := &protocol.Message{
		Type:    protocol.MsgTx,
		Payload: make([]byte, 256),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// xxhash
		hash := MessageHash(msg)

		// MarkSeen (sync.Map)
		cache.MarkSeen(hash)

		// 페이로드 변경
		msg.Payload[0] = byte(i)
	}
}

func BenchmarkFullFlow_New_Parallel(b *testing.B) {
	cache := &seenCacheSyncMap{}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		msg := &protocol.Message{
			Type:    protocol.MsgTx,
			Payload: make([]byte, 256),
		}
		i := 0
		for pb.Next() {
			msg.Payload[0] = byte(i)
			hash := MessageHash(msg)
			cache.MarkSeen(hash)
			i++
		}
	})
}
