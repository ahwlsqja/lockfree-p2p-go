package sync

import (
	"sync"
)

// =============================================================================
// Mutex 기반 HashMap
// =============================================================================

// HashMap은 Mutex로 보호되는 해시맵입니다.
//
// [Go의 sync.Map vs 일반 map + RWMutex]
//
// sync.Map:
// - 읽기가 매우 많고 쓰기가 적을 때 최적화
// - 캐시 효율을 위해 read-only 맵과 dirty 맵 2개 유지
// - Range 성능이 안 좋을 수 있음
//
// map + RWMutex:
// - 범용적인 성능
// - 읽기/쓰기 비율이 비슷할 때 적합
// - 구현이 단순하고 예측 가능
//
// 여기서는 비교 목적으로 RWMutex 버전 구현
type HashMap struct {
	// data는 실제 데이터를 저장하는 맵입니다.
	data map[string]interface{}

	// mu는 data를 보호하는 RWMutex입니다.
	//
	// [RWMutex 동작]
	// - RLock(): 읽기 락 (여러 고루틴 동시 가능)
	// - Lock(): 쓰기 락 (배타적, 읽기도 차단)
	// - RUnlock() / Unlock(): 락 해제
	//
	// [주의]
	// - 쓰기가 대기 중이면 새 읽기도 대기 (starvation 방지)
	// - 읽기 중에 RLock() 중첩 호출 OK
	// - Lock() 중에 RLock() 호출하면 데드락!
	mu sync.RWMutex
}

// NewHashMap은 새로운 Mutex 기반 해시맵을 생성합니다.
func NewHashMap() *HashMap {
	return &HashMap{
		data: make(map[string]interface{}),
	}
}

// Get은 키로 값을 조회합니다.
//
// [코드 플로우]
// 1. RLock() - 읽기 락 획득
// 2. map에서 조회
// 3. RUnlock() - 읽기 락 해제
//
// [동시성]
// 여러 고루틴이 동시에 Get 가능 (RLock은 공유됨)
func (m *HashMap) Get(key string) (interface{}, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	value, ok := m.data[key]
	return value, ok
}

// Put은 키-값 쌍을 저장합니다.
//
// [코드 플로우]
// 1. Lock() - 쓰기 락 획득 (모든 읽기도 대기)
// 2. map에 저장
// 3. Unlock() - 쓰기 락 해제
func (m *HashMap) Put(key string, value interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = value
}

// Delete는 키를 삭제합니다.
func (m *HashMap) Delete(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.data[key]; !ok {
		return false
	}

	delete(m.data, key)
	return true
}

// Has는 키 존재 여부를 확인합니다.
func (m *HashMap) Has(key string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.data[key]
	return ok
}

// Len은 맵의 크기를 반환합니다.
func (m *HashMap) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.data)
}

// Range는 모든 키-값 쌍에 대해 함수를 호출합니다.
//
// [주의]
// - Range 중에는 쓰기 락이 걸려있지 않음 (읽기 락만)
// - 순회 중 다른 고루틴이 Put/Delete 호출하면?
//   → Go map은 concurrent read/write 시 panic!
//   → 그래서 Range도 RLock 필요
//
// [콜백 반환값]
// - true: 계속 순회
// - false: 순회 중단
func (m *HashMap) Range(fn func(key string, value interface{}) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for k, v := range m.data {
		if !fn(k, v) {
			break
		}
	}
}

// Clear는 모든 데이터를 삭제합니다.
func (m *HashMap) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 새 맵으로 교체 (기존 맵은 GC가 수거)
	m.data = make(map[string]interface{})
}

// Keys는 모든 키를 반환합니다.
func (m *HashMap) Keys() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	return keys
}

// Values는 모든 값을 반환합니다.
func (m *HashMap) Values() []interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	values := make([]interface{}, 0, len(m.data))
	for _, v := range m.data {
		values = append(values, v)
	}
	return values
}
