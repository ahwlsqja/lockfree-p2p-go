package sync

import (
	"container/heap"
	"sync"
)

// =============================================================================
// Mutex 기반 Priority Queue
// =============================================================================

// PriorityQueue는 Mutex로 보호되는 우선순위 큐입니다.
//
// [우선순위 큐란?]
// 일반 큐: FIFO (먼저 들어간 게 먼저 나옴)
// 우선순위 큐: 우선순위 높은 게 먼저 나옴
//
// [블록체인에서의 사용]
// Mempool에서 트랜잭션 선택 시:
// - GasPrice가 높은 트랜잭션 먼저 선택
// - 채굴자/검증자 수익 최대화
//
// [구현 방식: 힙(Heap)]
// - 완전 이진 트리 기반
// - 부모 노드가 자식보다 우선순위 높음 (max-heap)
// - Push/Pop: O(log n)
// - Peek: O(1)
type PriorityQueue struct {
	// items는 힙으로 관리되는 항목들입니다.
	items *itemHeap

	// mu는 items를 보호하는 뮤텍스입니다.
	mu sync.RWMutex
}

// Item은 우선순위 큐의 항목입니다.
type Item struct {
	Value    interface{}
	Priority int64 // 높을수록 먼저 나옴
}

// NewPriorityQueue는 새로운 Mutex 기반 우선순위 큐를 생성합니다.
func NewPriorityQueue() *PriorityQueue {
	pq := &PriorityQueue{
		items: &itemHeap{},
	}
	heap.Init(pq.items)
	return pq
}

// Push는 항목을 큐에 추가합니다.
//
// [코드 플로우]
// 1. 쓰기 락 획득
// 2. 힙에 Push (내부적으로 heapify-up)
// 3. 락 해제
//
// [heapify-up]
// 새 항목을 맨 끝에 추가하고, 부모와 비교하며 위로 이동
// 시간 복잡도: O(log n)
func (pq *PriorityQueue) Push(value interface{}, priority int64) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	heap.Push(pq.items, &Item{
		Value:    value,
		Priority: priority,
	})
}

// Pop은 가장 높은 우선순위 항목을 꺼냅니다.
//
// [코드 플로우]
// 1. 쓰기 락 획득
// 2. 힙에서 Pop (내부적으로 heapify-down)
// 3. 락 해제
//
// [heapify-down]
// 루트를 제거하고, 마지막 항목을 루트로 이동, 자식과 비교하며 아래로 이동
// 시간 복잡도: O(log n)
func (pq *PriorityQueue) Pop() (*Item, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.items.Len() == 0 {
		return nil, false
	}

	item := heap.Pop(pq.items).(*Item)
	return item, true
}

// Peek은 가장 높은 우선순위 항목을 제거하지 않고 반환합니다.
//
// [시간 복잡도]
// O(1) - 힙에서 최대값은 항상 루트(인덱스 0)에 있음
func (pq *PriorityQueue) Peek() (*Item, bool) {
	pq.mu.RLock()
	defer pq.mu.RUnlock()

	if pq.items.Len() == 0 {
		return nil, false
	}

	return (*pq.items)[0], true
}

// Len은 큐의 크기를 반환합니다.
func (pq *PriorityQueue) Len() int {
	pq.mu.RLock()
	defer pq.mu.RUnlock()
	return pq.items.Len()
}

// IsEmpty는 큐가 비어있는지 확인합니다.
func (pq *PriorityQueue) IsEmpty() bool {
	return pq.Len() == 0
}

// Clear는 큐를 비웁니다.
func (pq *PriorityQueue) Clear() {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	*pq.items = (*pq.items)[:0]
}

// =============================================================================
// 힙 구현 (container/heap 인터페이스)
// =============================================================================

// itemHeap은 Item 슬라이스로 구현된 힙입니다.
//
// [container/heap 인터페이스]
// - Len() int
// - Less(i, j int) bool  ← 비교 함수 (우선순위 정의)
// - Swap(i, j int)
// - Push(x interface{})
// - Pop() interface{}
//
// [힙 구조]
// 배열로 표현된 완전 이진 트리:
// 인덱스 i의 부모: (i-1)/2
// 인덱스 i의 왼쪽 자식: 2*i+1
// 인덱스 i의 오른쪽 자식: 2*i+2
//
// 예시 (max-heap):
//        [100]         인덱스 0
//       /     \
//    [50]     [80]     인덱스 1, 2
//   /    \
// [30]   [40]          인덱스 3, 4
//
// 배열: [100, 50, 80, 30, 40]
type itemHeap []*Item

func (h itemHeap) Len() int { return len(h) }

// Less는 우선순위 비교 함수입니다.
// container/heap은 기본적으로 min-heap이므로,
// 우리는 max-heap을 원하기 때문에 > 사용
func (h itemHeap) Less(i, j int) bool {
	return h[i].Priority > h[j].Priority
}

func (h itemHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *itemHeap) Push(x interface{}) {
	*h = append(*h, x.(*Item))
}

func (h *itemHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // GC를 위해 nil 설정
	*h = old[0 : n-1]
	return item
}
