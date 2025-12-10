package mempool

import (
	"container/heap"
	"errors"
	"log"
	"sync"
	"time"
)

// =============================================================================
// Mempool 에러
// =============================================================================

var (
	// ErrTxAlreadyExists는 트랜잭션이 이미 존재할 때 반환됩니다.
	ErrTxAlreadyExists = errors.New("트랜잭션이 이미 존재합니다")

	// ErrMempoolFull은 Mempool이 가득 찼을 때 반환됩니다.
	ErrMempoolFull = errors.New("Mempool이 가득 찼습니다")

	// ErrTxNotFound는 트랜잭션을 찾을 수 없을 때 반환됩니다.
	ErrTxNotFound = errors.New("트랜잭션을 찾을 수 없습니다")

	// ErrLowGasPrice는 가스 가격이 너무 낮을 때 반환됩니다.
	ErrLowGasPrice = errors.New("가스 가격이 너무 낮습니다")
)

// =============================================================================
// Mempool 설정
// =============================================================================

// Config는 Mempool 설정입니다.
type Config struct {
	// MaxSize는 최대 트랜잭션 수입니다.
	// 기본값: 10000
	MaxSize int

	// MaxTxSize는 단일 트랜잭션 최대 크기(바이트)입니다.
	// 기본값: 32KB
	MaxTxSize int

	// MinGasPrice는 최소 가스 가격입니다.
	// 이보다 낮은 트랜잭션은 거부됩니다.
	// 기본값: 1
	MinGasPrice uint64

	// TxTTL은 트랜잭션 유효 시간입니다.
	// 이 시간이 지나면 Mempool에서 제거됩니다.
	// 기본값: 3시간
	TxTTL time.Duration

	// CleanupInterval은 만료 트랜잭션 정리 주기입니다.
	// 기본값: 5분
	CleanupInterval time.Duration
}

// DefaultConfig는 기본 Mempool 설정입니다.
var DefaultConfig = Config{
	MaxSize:         10000,
	MaxTxSize:       32 * 1024, // 32KB
	MinGasPrice:     1,
	TxTTL:           3 * time.Hour,
	CleanupInterval: 5 * time.Minute,
}

// =============================================================================
// Mempool (Mutex 기반)
// =============================================================================

// Mempool은 대기 중인 트랜잭션 풀입니다.
//
// [자료구조]
// 1. txs map: 해시로 빠른 조회 (O(1))
// 2. pending heap: 우선순위 정렬된 트랜잭션 목록
//
// [동시성]
// sync.RWMutex로 보호됨
// - 읽기: 여러 고루틴 동시 가능
// - 쓰기: 하나만 가능
//
// [Phase 3에서]
// Lock-free 우선순위 큐로 교체 예정
type Mempool struct {
	// config는 Mempool 설정입니다.
	config Config

	// txs는 해시를 키로 하는 트랜잭션 맵입니다.
	//
	// [왜 맵이 필요한가?]
	// - O(1) 존재 여부 확인 (중복 방지)
	// - O(1) 해시로 조회
	txs map[TxHash]*Tx

	// pending은 우선순위로 정렬된 트랜잭션 목록입니다.
	//
	// [Go의 container/heap]
	// - 최소 힙(min-heap) 인터페이스
	// - Push/Pop: O(log n)
	// - Peek: O(1)
	//
	// [우리 구현]
	// - 최대 힙(max-heap)으로 변경 (높은 우선순위 먼저)
	// - Less() 함수에서 > 대신 < 사용
	pending *txHeap

	// mu는 동시 접근을 보호합니다.
	mu sync.RWMutex

	// 정리 루프 관련
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// New는 새로운 Mempool을 생성합니다.
func New(config *Config) *Mempool {
	cfg := DefaultConfig
	if config != nil {
		cfg = *config
	}

	mp := &Mempool{
		config:  cfg,
		txs:     make(map[TxHash]*Tx),
		pending: &txHeap{},
		stopCh:  make(chan struct{}),
	}

	heap.Init(mp.pending)

	return mp
}

// Start는 Mempool 백그라운드 작업을 시작합니다.
func (mp *Mempool) Start() {
	mp.wg.Add(1)
	go mp.cleanupLoop()
	log.Printf("[Mempool] 시작됨 (최대 크기: %d)", mp.config.MaxSize)
}

// Stop은 Mempool을 중지합니다.
func (mp *Mempool) Stop() {
	close(mp.stopCh)
	mp.wg.Wait()
	log.Printf("[Mempool] 중지됨")
}

// cleanupLoop는 주기적으로 만료된 트랜잭션을 정리합니다.
func (mp *Mempool) cleanupLoop() {
	defer mp.wg.Done()

	ticker := time.NewTicker(mp.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-mp.stopCh:
			return
		case <-ticker.C:
			mp.removeExpired()
		}
	}
}

// removeExpired는 만료된 트랜잭션을 제거합니다.
func (mp *Mempool) removeExpired() {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	now := time.Now()
	expiredHashes := make([]TxHash, 0)

	for hash, tx := range mp.txs {
		if now.Sub(tx.ReceivedAt) > mp.config.TxTTL {
			expiredHashes = append(expiredHashes, hash)
		}
	}

	for _, hash := range expiredHashes {
		delete(mp.txs, hash)
		// pending 힙에서는 별도 제거 (lazy deletion)
	}

	if len(expiredHashes) > 0 {
		// 힙 재구성 (lazy deletion 적용)
		mp.rebuildHeapLocked()
		log.Printf("[Mempool] 만료된 트랜잭션 %d개 제거됨", len(expiredHashes))
	}
}

// rebuildHeapLocked는 힙을 재구성합니다.
// mu.Lock()이 잡힌 상태에서 호출해야 합니다.
func (mp *Mempool) rebuildHeapLocked() {
	newHeap := &txHeap{}

	for _, tx := range mp.txs {
		*newHeap = append(*newHeap, tx)
	}

	heap.Init(newHeap)
	mp.pending = newHeap
}

// =============================================================================
// 트랜잭션 추가/제거
// =============================================================================

// Add는 트랜잭션을 Mempool에 추가합니다.
//
// [검사 항목]
// 1. 기본 유효성 (Validate())
// 2. 중복 여부
// 3. 용량 여유
// 4. 최소 가스 가격
// 5. 트랜잭션 크기
//
// [반환값]
// - nil: 성공
// - ErrTxAlreadyExists: 이미 존재
// - ErrMempoolFull: 용량 초과
// - ErrLowGasPrice: 가스 가격 부족
func (mp *Mempool) Add(tx *Tx) error {
	// 기본 유효성 검사
	if err := tx.Validate(); err != nil {
		return err
	}

	// 트랜잭션 크기 확인
	if tx.Size() > mp.config.MaxTxSize {
		return ValidationError{Field: "Size", Message: "트랜잭션이 너무 큽니다"}
	}

	// 최소 가스 가격 확인
	if tx.GasPrice < mp.config.MinGasPrice {
		return ErrLowGasPrice
	}

	mp.mu.Lock()
	defer mp.mu.Unlock()

	// 중복 확인
	if _, exists := mp.txs[tx.Hash]; exists {
		return ErrTxAlreadyExists
	}

	// 용량 확인
	if len(mp.txs) >= mp.config.MaxSize {
		// 가장 낮은 우선순위 트랜잭션보다 높으면 교체
		if mp.pending.Len() > 0 {
			lowest := (*mp.pending)[mp.pending.Len()-1]
			if tx.Priority() > lowest.Priority() {
				// 가장 낮은 것 제거하고 새 것 추가
				mp.removeLowestLocked()
			} else {
				return ErrMempoolFull
			}
		} else {
			return ErrMempoolFull
		}
	}

	// 수신 시간 설정
	tx.ReceivedAt = time.Now()

	// 맵에 추가
	mp.txs[tx.Hash] = tx

	// 힙에 추가
	heap.Push(mp.pending, tx)

	return nil
}

// removeLowestLocked는 가장 낮은 우선순위 트랜잭션을 제거합니다.
// mu.Lock()이 잡힌 상태에서 호출해야 합니다.
func (mp *Mempool) removeLowestLocked() {
	if mp.pending.Len() == 0 {
		return
	}

	// 힙에서 가장 낮은 우선순위는 마지막 원소 근처에 있음
	// 정확한 제거를 위해 전체 스캔 필요 (비효율적이지만 간단)
	// TODO: 양방향 힙으로 최적화 가능

	var lowestIdx int
	var lowestPriority uint64 = ^uint64(0)

	for i, tx := range *mp.pending {
		if tx.Priority() < lowestPriority {
			lowestPriority = tx.Priority()
			lowestIdx = i
		}
	}

	// 맵에서 제거
	tx := (*mp.pending)[lowestIdx]
	delete(mp.txs, tx.Hash)

	// 힙에서 제거
	heap.Remove(mp.pending, lowestIdx)
}

// Remove는 해시로 트랜잭션을 제거합니다.
//
// [사용 시점]
// - 블록에 포함된 트랜잭션
// - 무효화된 트랜잭션
func (mp *Mempool) Remove(hash TxHash) error {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	if _, exists := mp.txs[hash]; !exists {
		return ErrTxNotFound
	}

	delete(mp.txs, hash)

	// 힙에서는 lazy deletion (성능 위해)
	// 나중에 Pop할 때 확인

	return nil
}

// RemoveBatch는 여러 트랜잭션을 한 번에 제거합니다.
//
// [사용 시점]
// 블록이 채굴/확정되어 여러 트랜잭션이 한 번에 확정될 때
func (mp *Mempool) RemoveBatch(hashes []TxHash) int {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	removed := 0
	for _, hash := range hashes {
		if _, exists := mp.txs[hash]; exists {
			delete(mp.txs, hash)
			removed++
		}
	}

	// 많이 제거했으면 힙 재구성
	if removed > 100 || removed > len(mp.txs)/10 {
		mp.rebuildHeapLocked()
	}

	return removed
}

// =============================================================================
// 트랜잭션 조회
// =============================================================================

// Get은 해시로 트랜잭션을 조회합니다.
func (mp *Mempool) Get(hash TxHash) (*Tx, bool) {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	tx, exists := mp.txs[hash]
	return tx, exists
}

// Has는 트랜잭션 존재 여부를 확인합니다.
func (mp *Mempool) Has(hash TxHash) bool {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	_, exists := mp.txs[hash]
	return exists
}

// Size는 현재 Mempool 크기를 반환합니다.
func (mp *Mempool) Size() int {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	return len(mp.txs)
}

// IsFull은 Mempool이 가득 찼는지 확인합니다.
func (mp *Mempool) IsFull() bool {
	mp.mu.RLock()
	defer mp.mu.RUnlock()
	return len(mp.txs) >= mp.config.MaxSize
}

// =============================================================================
// 블록 생성을 위한 트랜잭션 선택
// =============================================================================

// GetPending은 우선순위 순으로 트랜잭션을 반환합니다.
//
// [파라미터]
// - limit: 최대 반환 개수 (0이면 전체)
//
// [반환값]
// 우선순위 높은 순으로 정렬된 트랜잭션 슬라이스
//
// [사용 시점]
// 채굴자/검증자가 블록에 넣을 트랜잭션 선택 시
func (mp *Mempool) GetPending(limit int) []*Tx {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	if limit <= 0 || limit > len(mp.txs) {
		limit = len(mp.txs)
	}

	// 현재 힙의 복사본 생성 (원본 수정 방지)
	heapCopy := make(txHeap, mp.pending.Len())
	copy(heapCopy, *mp.pending)
	heap.Init(&heapCopy)

	result := make([]*Tx, 0, limit)
	for i := 0; i < limit && heapCopy.Len() > 0; i++ {
		tx := heap.Pop(&heapCopy).(*Tx)

		// lazy deletion 적용 (맵에 없으면 스킵)
		if _, exists := mp.txs[tx.Hash]; exists {
			result = append(result, tx)
		} else {
			i-- // 카운트 되돌리기
		}
	}

	return result
}

// PopHighestPriority는 가장 높은 우선순위 트랜잭션을 꺼냅니다.
//
// [동작]
// 1. 힙에서 최상위 트랜잭션 Pop
// 2. 맵에서 제거
// 3. 트랜잭션 반환
//
// [주의]
// 꺼낸 트랜잭션은 Mempool에서 제거됨
func (mp *Mempool) PopHighestPriority() *Tx {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	for mp.pending.Len() > 0 {
		tx := heap.Pop(mp.pending).(*Tx)

		// lazy deletion 적용
		if _, exists := mp.txs[tx.Hash]; exists {
			delete(mp.txs, tx.Hash)
			return tx
		}
		// 맵에 없으면 이미 제거된 것, 다음 것 시도
	}

	return nil
}

// PeekHighestPriority는 가장 높은 우선순위 트랜잭션을 조회합니다 (제거 안 함).
func (mp *Mempool) PeekHighestPriority() *Tx {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	// lazy deletion 때문에 맵에 있는 것 찾을 때까지
	for _, tx := range *mp.pending {
		if _, exists := mp.txs[tx.Hash]; exists {
			return tx
		}
	}

	return nil
}

// =============================================================================
// 계정별 조회
// =============================================================================

// GetByAddress는 특정 주소가 보낸 트랜잭션들을 반환합니다.
//
// [사용 케이스]
// - 사용자가 자신의 대기 중인 트랜잭션 확인
// - Nonce 충돌 확인
func (mp *Mempool) GetByAddress(addr Address) []*Tx {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	result := make([]*Tx, 0)
	for _, tx := range mp.txs {
		if tx.From == addr {
			result = append(result, tx)
		}
	}

	return result
}

// GetNonce는 특정 주소의 다음 예상 nonce를 반환합니다.
//
// [계산]
// Mempool에 있는 해당 주소 트랜잭션 중 최대 nonce + 1
//
// [주의]
// 이미 확정된 트랜잭션의 nonce는 상태 DB에서 확인해야 함
func (mp *Mempool) GetNonce(addr Address) uint64 {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	var maxNonce uint64
	found := false

	for _, tx := range mp.txs {
		if tx.From == addr {
			if !found || tx.Nonce > maxNonce {
				maxNonce = tx.Nonce
				found = true
			}
		}
	}

	if found {
		return maxNonce + 1
	}
	return 0 // 상태 DB에서 조회 필요
}

// =============================================================================
// 통계
// =============================================================================

// Stats는 Mempool 통계입니다.
type Stats struct {
	// Size는 현재 트랜잭션 수입니다.
	Size int

	// MaxSize는 최대 용량입니다.
	MaxSize int

	// TotalFees는 모든 트랜잭션의 총 수수료입니다.
	TotalFees uint64

	// MinGasPrice는 현재 최소 가스 가격입니다.
	MinGasPrice uint64

	// MaxGasPrice는 현재 최대 가스 가격입니다.
	MaxGasPrice uint64
}

// GetStats는 현재 통계를 반환합니다.
func (mp *Mempool) GetStats() Stats {
	mp.mu.RLock()
	defer mp.mu.RUnlock()

	stats := Stats{
		Size:    len(mp.txs),
		MaxSize: mp.config.MaxSize,
	}

	for _, tx := range mp.txs {
		stats.TotalFees += tx.Fee()

		if stats.MinGasPrice == 0 || tx.GasPrice < stats.MinGasPrice {
			stats.MinGasPrice = tx.GasPrice
		}
		if tx.GasPrice > stats.MaxGasPrice {
			stats.MaxGasPrice = tx.GasPrice
		}
	}

	return stats
}

// =============================================================================
// 트랜잭션 우선순위 힙
// =============================================================================

// txHeap은 트랜잭션 우선순위 힙입니다.
// container/heap 인터페이스 구현
//
// [Go heap 인터페이스]
// - Len() int
// - Less(i, j int) bool
// - Swap(i, j int)
// - Push(x interface{})
// - Pop() interface{}
//
// [힙 속성]
// - 부모 노드가 자식 노드보다 작거나 같음 (min-heap)
// - 우리는 큰 게 먼저 나와야 하므로 Less에서 부등호 반전
type txHeap []*Tx

func (h txHeap) Len() int { return len(h) }

// Less는 우선순위 비교 함수입니다.
// i가 j보다 "작다"고 하면 i가 먼저 나옴
// 우리는 높은 우선순위가 먼저 나와야 하므로 > 사용
func (h txHeap) Less(i, j int) bool {
	return h[i].Priority() > h[j].Priority()
}

func (h txHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *txHeap) Push(x interface{}) {
	*h = append(*h, x.(*Tx))
}

func (h *txHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // GC를 위해 nil 설정
	*h = old[0 : n-1]
	return item
}
