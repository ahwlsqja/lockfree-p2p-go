// Package sync는 Mutex 기반 동기화 자료구조를 제공합니다.
// Lock-free 버전과 성능 비교를 위한 기준선(baseline)으로 사용됩니다.
package sync

import (
	"sync"
)

// =============================================================================
// Mutex 기반 Queue
// =============================================================================

// Queue는 Mutex로 보호되는 FIFO 큐입니다.
//
// [구조]
// ┌─────────────────────────────────────┐
// │  Mutex (잠금)                        │
// │  ┌─────┬─────┬─────┬─────┬─────┐   │
// │  │  A  │  B  │  C  │  D  │ ... │   │
// │  └─────┴─────┴─────┴─────┴─────┘   │
// │    ↑                         ↑      │
// │   head                      tail    │
// └─────────────────────────────────────┘
//
// [동작 방식]
// 1. Enqueue/Dequeue 전에 Mutex.Lock() 호출
// 2. 작업 수행
// 3. Mutex.Unlock() 호출
//
// [문제점]
// - Lock Contention: 여러 고루틴이 동시에 접근하면 대기 발생
// - Context Switch: Lock 획득 실패 시 고루틴이 sleep → wake up
// - Priority Inversion: 낮은 우선순위가 Lock 잡으면 높은 우선순위도 대기
//
// [언제 괜찮은가?]
// - 경합이 적을 때 (단일 생산자/소비자)
// - 임계 구역이 짧을 때
// - 코드 단순성이 중요할 때
type Queue struct {
	// items는 큐에 저장된 항목들입니다.
	//
	// [슬라이스 기반 구현]
	// - 장점: 메모리 지역성 좋음, 구현 간단
	// - 단점: 앞에서 제거 시 O(n) 복사 발생 가능
	//
	// [링 버퍼로 최적화 가능]
	// head, tail 인덱스 사용하면 O(1) 가능
	// 여기서는 단순함을 위해 슬라이스 사용
	items []interface{}

	// mu는 items를 보호하는 뮤텍스입니다.
	//
	// [sync.Mutex vs sync.RWMutex]
	// - Mutex: 읽기/쓰기 모두 배타적
	// - RWMutex: 읽기는 동시 가능, 쓰기는 배타적
	//
	// Queue는 Peek 빼고 대부분 쓰기이므로 일반 Mutex 사용
	mu sync.Mutex
}

// NewQueue는 새로운 Mutex 기반 큐를 생성합니다.
func NewQueue() *Queue {
	return &Queue{
		items: make([]interface{}, 0),
	}
}

// Enqueue는 큐에 값을 추가합니다.
//
// [코드 플로우]
// 1. mu.Lock() - 다른 고루틴 접근 차단
//    └─ 이미 잠겨있으면 여기서 블로킹 (고루틴 파킹)
// 2. items에 append
// 3. mu.Unlock() - 다른 고루틴 접근 허용
//
// [시간 복잡도]
// - 평균: O(1) amortized (슬라이스 확장 때문)
// - 최악: O(n) (슬라이스 재할당 시)
//
// [락 오버헤드]
// - Lock/Unlock 자체: ~20-50ns
// - 경합 시: 수 μs ~ ms (컨텍스트 스위칭)
func (q *Queue) Enqueue(value interface{}) {
	// 1. 락 획득
	// [OS 레벨]
	// - Linux: futex 시스템 콜 (Fast Userspace muTEX)
	// - 경합 없으면 커널 진입 없이 유저 공간에서 처리
	// - 경합 있으면 커널이 고루틴을 sleep 시킴
	q.mu.Lock()

	// 2. 슬라이스에 추가
	// [메모리 레벨]
	// - 용량 충분하면: 그냥 인덱스만 증가
	// - 용량 부족하면: 새 배열 할당, 복사, GC 대상 등록
	q.items = append(q.items, value)

	// 3. 락 해제
	// [OS 레벨]
	// - 대기 중인 고루틴 있으면 깨움
	// - 없으면 그냥 플래그만 변경
	q.mu.Unlock()
}

// Dequeue는 큐에서 값을 꺼냅니다.
//
// [코드 플로우]
// 1. mu.Lock()
// 2. 비어있으면 nil 반환
// 3. 첫 번째 항목 꺼내기
// 4. 슬라이스 앞부분 제거
// 5. mu.Unlock()
//
// [반환값]
// - interface{}: 꺼낸 값 (비어있으면 nil)
// - bool: 성공 여부 (비어있으면 false)
//
// [성능 주의점]
// items = items[1:] 는 메모리 누수 가능!
// 원본 배열의 첫 번째 요소가 GC 안 됨
// 그래서 items[0] = nil 로 명시적 해제
func (q *Queue) Dequeue() (interface{}, bool) {
	q.mu.Lock()
	defer q.mu.Unlock() // 함수 끝에 자동 해제 (panic 안전)

	// 비어있는지 확인
	if len(q.items) == 0 {
		return nil, false
	}

	// 첫 번째 항목 가져오기
	value := q.items[0]

	// 메모리 누수 방지
	// [왜 필요한가?]
	// 슬라이스는 내부적으로 배열을 참조
	// items[1:]로 잘라도 원본 배열의 [0]은 여전히 메모리에 있음
	// nil로 설정해야 GC가 수거 가능
	q.items[0] = nil

	// 슬라이스 앞부분 제거
	// [내부 동작]
	// - 새 슬라이스 헤더 생성 (ptr+1, len-1, cap-1)
	// - 실제 데이터 복사는 없음
	// - 하지만 앞부분 메모리는 계속 점유 (cap이 유지되므로)
	q.items = q.items[1:]

	return value, true
}

// Peek은 첫 번째 항목을 제거하지 않고 반환합니다.
func (q *Queue) Peek() (interface{}, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.items) == 0 {
		return nil, false
	}

	return q.items[0], true
}

// Len은 큐의 길이를 반환합니다.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// IsEmpty는 큐가 비어있는지 확인합니다.
func (q *Queue) IsEmpty() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items) == 0
}

// Clear는 큐를 비웁니다.
func (q *Queue) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()

	// GC를 위해 모든 참조 해제
	for i := range q.items {
		q.items[i] = nil
	}
	q.items = q.items[:0] // 길이만 0으로, 용량 유지
}
