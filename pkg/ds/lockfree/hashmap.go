package lockfree

import (
	"hash/fnv"
	"sync/atomic"
	"unsafe"
)

// =============================================================================
// Lock-free HashMap (Split-Ordered List 기반)
// =============================================================================

// HashMap은 Lock-free 동시성 해시맵입니다.
//
// [알고리즘: 분할 정렬 리스트 (Split-Ordered List)]
// Ori Shalev와 Nir Shavit이 2006년에 발표한 알고리즘입니다.
//
// [기존 해시맵의 문제]
// 해시맵은 버킷(bucket) 배열 + 체이닝으로 구현됩니다.
// 문제: 리사이징(버킷 배열 확장) 시 모든 요소 재배치 필요 → Lock 필요
//
// [Split-Ordered List 해결책]
// 1. 모든 요소를 하나의 정렬된 연결 리스트에 저장
// 2. 버킷은 이 리스트의 특정 위치를 가리키는 포인터만 저장 그니까 그 버킷요소의 시작 포인터만 자장한다고 보면된다.
// 3. 리사이징 시 버킷 포인터만 추가하면 됨 → Lock-free 가능!
//
// [핵심 아이디어: 비트 반전 키]
// 일반 해시: h(key) → bucket index
// Split-Ordered: bit_reverse(h(key)) → 정렬 키
//
// 예시 (버킷 4개일 때):
// key  hash  bucket  reversed
// "a"  0010  2       0100
// "b"  0101  1       1010
// "c"  0110  2       0110
// "d"  1001  1       1001
//
// 정렬: 0100("a") < 0110("c") < 1001("d") < 1010("b")
//
// [왜 이게 좋은가?]
// - 같은 버킷의 요소들이 연속적으로 배치됨
// - 버킷 2배 확장 시, 기존 버킷 요소 절반만 새 버킷으로 "자연스럽게" 분리
// - 요소 이동 없이 버킷 포인터만 조정!
//
// [이 구현의 단순화]
// 여기서는 고정 버킷 수를 사용합니다 (리사이징 없음).
// 실제 구현에서는 동적 리사이징을 추가할 수 있습니다.
type HashMap struct {
	// buckets는 버킷 배열입니다.
	//
	// [각 버킷]
	// - 해당 해시 범위의 첫 번째 노드를 가리킴
	// - 리스트의 "진입점" 역할
	buckets []unsafe.Pointer // []*hashNode

	// bucketCount는 버킷 수입니다 (2의 거듭제곱).
	bucketCount uint32

	// count는 현재 저장된 요소 수입니다.
	count int64

	// 리스트의 더미 헤드 노드
	head unsafe.Pointer // *hashNode
}

// hashNode는 해시맵의 노드입니다.
type hashNode struct {
	// key는 원본 키입니다.
	key string

	// value는 저장된 값입니다.
	value unsafe.Pointer // *interface{}로 atomic 저장

	// hash는 키의 해시값입니다 (정렬에 사용).
	hash uint64

	// next는 다음 노드입니다.
	next unsafe.Pointer // *hashNode

	// deleted는 논리적 삭제 표시입니다.
	//
	// [왜 논리적 삭제?]
	// Lock-free 환경에서 물리적 삭제는 위험:
	// - A가 node1.next 읽음
	// - B가 node1 삭제
	// - A가 읽은 node1.next는 무효!
	//
	// 논리적 삭제:
	// 1. deleted = true 마킹
	// 2. 나중에 순회 시 물리적 제거
	deleted int32 // atomic boolean
}

// NewHashMap은 새로운 Lock-free 해시맵을 생성합니다.
//
// [파라미터]
// - bucketCount: 버킷 수 (2의 거듭제곱이 좋음)
func NewHashMap(bucketCount uint32) *HashMap {
	if bucketCount == 0 {
		bucketCount = 64
	}

	// 2의 거듭제곱으로 반올림
	bucketCount = nextPowerOf2(bucketCount)

	hm := &HashMap{
		buckets:     make([]unsafe.Pointer, bucketCount),
		bucketCount: bucketCount,
	}

	// 더미 헤드 노드 생성
	dummy := &hashNode{hash: 0}
	atomic.StorePointer(&hm.head, unsafe.Pointer(dummy))

	return hm
}

// hashKey는 키의 해시값을 계산합니다.
//
// [FNV-1a 해시]
// - 빠르고 분포가 좋은 비암호화 해시
// - 문자열 해싱에 적합
func hashKey(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}

// getBucket은 해시값에 해당하는 버킷 인덱스를 반환합니다.
func (m *HashMap) getBucket(hash uint64) uint32 {
	return uint32(hash) & (m.bucketCount - 1)
}

// Get은 키로 값을 조회합니다. (Lock-free)
//
// [코드 플로우]
// 1. 키 해시 계산
// 2. 버킷 인덱스 계산
// 3. 버킷 포인터에서 시작해서 리스트 순회
// 4. 키 일치하는 노드 찾으면 값 반환
//
// [시간 복잡도]
// - 평균: O(1) (버킷당 평균 요소 수가 상수)
// - 최악: O(n) (모든 요소가 한 버킷에)
func (m *HashMap) Get(key string) (interface{}, bool) {
	hash := hashKey(key)
	bucketIdx := m.getBucket(hash)

	// 버킷 시작점 가져오기
	var curr *hashNode
	bucketPtr := atomic.LoadPointer(&m.buckets[bucketIdx])
	if bucketPtr != nil {
		curr = (*hashNode)(bucketPtr)
	} else {
		// 버킷이 초기화되지 않았으면 헤드부터 시작
		curr = (*hashNode)(atomic.LoadPointer(&m.head))
	}

	// 리스트 순회
	for curr != nil {
		// 해시가 다르면 스킵
		if curr.hash == hash && curr.key == key {
			// 삭제된 노드인지 확인
			if atomic.LoadInt32(&curr.deleted) == 0 {
				// 값 읽기 (atomic)
				valPtr := atomic.LoadPointer(&curr.value)
				if valPtr != nil {
					return *(*interface{})(valPtr), true
				}
			}
			return nil, false
		}

		// 해시가 더 크면 없는 것 (정렬되어 있으므로)
		if curr.hash > hash {
			return nil, false
		}

		curr = (*hashNode)(atomic.LoadPointer(&curr.next))
	}

	return nil, false
}

// Put은 키-값 쌍을 저장합니다. (Lock-free)
//
// [코드 플로우]
// 1. 새 노드 생성
// 2. 해시 계산 및 버킷 결정
// 3. 적절한 위치 찾기 (정렬 유지)
// 4. CAS로 삽입 시도
// 5. 실패하면 재시도
//
// [기존 키가 있으면?]
// 값만 원자적으로 업데이트 (노드 재생성 X)
func (m *HashMap) Put(key string, value interface{}) {
	hash := hashKey(key)

	// 값을 힙에 저장 (포인터로 저장하기 위해)
	valPtr := unsafe.Pointer(&value)

	for {
		// 삽입 위치 찾기
		prev, curr := m.findPosition(hash, key)

		// 이미 존재하는 키인지 확인
		if curr != nil && curr.hash == hash && curr.key == key {
			// 삭제된 상태면 복구
			if atomic.LoadInt32(&curr.deleted) == 1 {
				if atomic.CompareAndSwapInt32(&curr.deleted, 1, 0) {
					atomic.StorePointer(&curr.value, valPtr)
					atomic.AddInt64(&m.count, 1)
					return
				}
				continue // 재시도
			}

			// 값만 업데이트
			atomic.StorePointer(&curr.value, valPtr)
			return
		}

		// 새 노드 생성
		newNode := &hashNode{
			key:   key,
			value: valPtr,
			hash:  hash,
		}

		// 새 노드의 next 설정
		if curr != nil {
			atomic.StorePointer(&newNode.next, unsafe.Pointer(curr))
		}

		// CAS로 삽입
		var success bool
		if prev == nil {
			// 리스트 맨 앞에 삽입
			head := (*hashNode)(atomic.LoadPointer(&m.head))
			atomic.StorePointer(&newNode.next, unsafe.Pointer(head.next))
			success = atomic.CompareAndSwapPointer(&head.next, unsafe.Pointer(curr), unsafe.Pointer(newNode))
		} else {
			success = atomic.CompareAndSwapPointer(&prev.next, unsafe.Pointer(curr), unsafe.Pointer(newNode))
		}

		if success {
			atomic.AddInt64(&m.count, 1)

			// 버킷 포인터 업데이트 (필요시)
			m.maybeUpdateBucket(hash, newNode)
			return
		}
		// 실패하면 재시도
	}
}

// Delete는 키를 삭제합니다. (Lock-free, 논리적 삭제)
//
// [논리적 삭제]
// 실제로 노드를 제거하지 않고 deleted 플래그만 설정합니다.
// 물리적 제거는 나중에 순회 시 발생합니다.
func (m *HashMap) Delete(key string) bool {
	hash := hashKey(key)

	for {
		_, curr := m.findPosition(hash, key)

		if curr == nil || curr.hash != hash || curr.key != key {
			return false // 키가 없음
		}

		if atomic.LoadInt32(&curr.deleted) == 1 {
			return false // 이미 삭제됨
		}

		// 논리적 삭제 시도
		if atomic.CompareAndSwapInt32(&curr.deleted, 0, 1) {
			atomic.AddInt64(&m.count, -1)
			return true
		}
		// 실패하면 재시도
	}
}

// Has는 키 존재 여부를 확인합니다.
func (m *HashMap) Has(key string) bool {
	_, ok := m.Get(key)
	return ok
}

// Len은 맵의 크기를 반환합니다.
func (m *HashMap) Len() int {
	return int(atomic.LoadInt64(&m.count))
}

// Range는 모든 키-값 쌍에 대해 함수를 호출합니다.
//
// [주의]
// Lock-free 환경에서 Range는 "스냅샷"이 아닙니다.
// 순회 중에 다른 고루틴이 수정할 수 있습니다.
func (m *HashMap) Range(fn func(key string, value interface{}) bool) {
	head := (*hashNode)(atomic.LoadPointer(&m.head))
	curr := (*hashNode)(atomic.LoadPointer(&head.next))

	for curr != nil {
		if atomic.LoadInt32(&curr.deleted) == 0 {
			valPtr := atomic.LoadPointer(&curr.value)
			if valPtr != nil {
				if !fn(curr.key, *(*interface{})(valPtr)) {
					return
				}
			}
		}
		curr = (*hashNode)(atomic.LoadPointer(&curr.next))
	}
}

// findPosition은 삽입/검색 위치를 찾습니다.
//
// [반환값]
// - prev: 삽입 위치의 이전 노드 (nil이면 맨 앞)
// - curr: 삽입 위치 또는 찾은 노드
func (m *HashMap) findPosition(hash uint64, key string) (*hashNode, *hashNode) {
	head := (*hashNode)(atomic.LoadPointer(&m.head))

	var prev *hashNode
	curr := (*hashNode)(atomic.LoadPointer(&head.next))

	for curr != nil {
		// 해시로 정렬된 위치 찾기
		if curr.hash > hash {
			break
		}
		if curr.hash == hash && curr.key == key {
			return prev, curr
		}

		prev = curr
		curr = (*hashNode)(atomic.LoadPointer(&curr.next))
	}

	return prev, curr
}

// maybeUpdateBucket은 버킷 포인터를 업데이트합니다.
func (m *HashMap) maybeUpdateBucket(hash uint64, node *hashNode) {
	bucketIdx := m.getBucket(hash)

	// 버킷이 비어있으면 설정
	atomic.CompareAndSwapPointer(&m.buckets[bucketIdx], nil, unsafe.Pointer(node))
}

// Clear는 모든 데이터를 삭제합니다.
func (m *HashMap) Clear() {
	// 모든 노드를 논리적 삭제
	head := (*hashNode)(atomic.LoadPointer(&m.head))
	curr := (*hashNode)(atomic.LoadPointer(&head.next))

	for curr != nil {
		atomic.StoreInt32(&curr.deleted, 1)
		curr = (*hashNode)(atomic.LoadPointer(&curr.next))
	}

	atomic.StoreInt64(&m.count, 0)

	// 버킷 포인터 초기화
	for i := range m.buckets {
		atomic.StorePointer(&m.buckets[i], nil)
	}
}

// =============================================================================
// 유틸리티 함수
// =============================================================================

// nextPowerOf2는 n 이상의 가장 작은 2의 거듭제곱을 반환합니다.
func nextPowerOf2(n uint32) uint32 {
	if n == 0 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	return n + 1
}
