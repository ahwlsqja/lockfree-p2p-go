package node

import (
	"crypto/sha256"
	"encoding/hex"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/go-p2p-network/go-p2p/pkg/peer"
	"github.com/go-p2p-network/go-p2p/pkg/protocol"
)

// =============================================================================
// 브로드캐스트 매니저
// =============================================================================

// BroadcastManager는 메시지 브로드캐스트를 관리합니다.
//
// [역할]
// 1. 중복 메시지 필터링 (같은 메시지를 여러 피어에게서 받아도 한 번만 처리)
// 2. 메시지 전파 (가십 방식으로 효율적인 전파)
// 3. 전파 기록 관리 (누구에게 보냈는지 추적)
//
// [중복 방지 원리]
// 각 메시지에 대해 해시(fingerprint)를 계산하고,
// 이미 본 메시지의 해시는 seenMessages에 저장해서
// 같은 메시지가 다시 오면 무시합니다.
//
// [가십 전파 vs 플러딩]
// - 플러딩: 모든 피어에게 전송 → 네트워크 부하 높음
// - 가십: 일부 피어에게만 전송 → 네트워크 부하 낮음, 전파 시간 약간 증가
type BroadcastManager struct {
	// node는 부모 노드입니다.
	node *Node

	// seenMessages는 이미 처리한 메시지의 해시를 저장합니다.
	//
	// [메모리 관리]
	// - 무한히 쌓이면 안 됨
	// - TTL 지나면 자동 삭제
	// - 최대 개수 제한
	seenMessages map[string]time.Time
	seenMu       sync.RWMutex

	// config는 브로드캐스트 설정입니다.
	config BroadcastConfig

	// 정리 루프 관련
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// BroadcastConfig는 브로드캐스트 설정입니다.
type BroadcastConfig struct {
	// MessageTTL은 메시지 해시 캐시 유효 시간입니다.
	// 기본값: 10분
	//
	// [왜 10분?]
	// - 대부분의 메시지는 몇 초 내에 전파됨
	// - 하지만 네트워크 지연이 있을 수 있으므로 여유 있게
	// - 너무 길면 메모리 낭비
	MessageTTL time.Duration

	// MaxSeenMessages는 저장할 최대 메시지 해시 수입니다.
	// 기본값: 100000
	MaxSeenMessages int

	// GossipFanout은 가십 전파 시 선택할 피어 수입니다.
	// 기본값: 7
	//
	// [왜 7?]
	// - sqrt(N)에 가까운 값이 적절 (N=50이면 약 7)
	// - 전파 라운드 수와 네트워크 부하의 균형
	// - Bitcoin, Ethereum도 비슷한 값 사용
	GossipFanout int

	// CleanupInterval은 만료된 메시지 해시 정리 주기입니다.
	// 기본값: 1분
	CleanupInterval time.Duration
}

// DefaultBroadcastConfig는 기본 브로드캐스트 설정입니다.
var DefaultBroadcastConfig = BroadcastConfig{
	MessageTTL:      10 * time.Minute,
	MaxSeenMessages: 100000,
	GossipFanout:    7,
	CleanupInterval: 1 * time.Minute,
}

// NewBroadcastManager는 새로운 브로드캐스트 매니저를 생성합니다.
func NewBroadcastManager(node *Node, config *BroadcastConfig) *BroadcastManager {
	cfg := DefaultBroadcastConfig
	if config != nil {
		cfg = *config
	}

	return &BroadcastManager{
		node:         node,
		seenMessages: make(map[string]time.Time),
		config:       cfg,
		stopCh:       make(chan struct{}),
	}
}

// Start는 브로드캐스트 매니저를 시작합니다.
func (bm *BroadcastManager) Start() {
	bm.wg.Add(1)
	go bm.cleanupLoop()
}

// Stop은 브로드캐스트 매니저를 중지합니다.
func (bm *BroadcastManager) Stop() {
	close(bm.stopCh)
	bm.wg.Wait()
}

// cleanupLoop는 주기적으로 만료된 메시지 해시를 정리합니다.
func (bm *BroadcastManager) cleanupLoop() {
	defer bm.wg.Done()

	ticker := time.NewTicker(bm.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-bm.stopCh:
			return
		case <-ticker.C:
			bm.cleanup()
		}
	}
}

// cleanup은 만료된 메시지 해시를 제거합니다.
func (bm *BroadcastManager) cleanup() {
	bm.seenMu.Lock()
	defer bm.seenMu.Unlock()

	now := time.Now()
	for hash, seenAt := range bm.seenMessages {
		if now.Sub(seenAt) > bm.config.MessageTTL {
			delete(bm.seenMessages, hash)
		}
	}
}

// =============================================================================
// 중복 필터링
// =============================================================================

// MessageHash는 메시지의 고유 해시를 계산합니다.
//
// [해시 계산 방법]
// SHA-256(Type || Payload)
//
// [왜 SHA-256?]
// - 충돌 가능성 극히 낮음 (2^128 birthday attack)
// - 빠름 (하드웨어 가속 지원)
// - 32바이트로 적당한 크기
func MessageHash(msg *protocol.Message) string {
	hasher := sha256.New()
	hasher.Write([]byte{byte(msg.Type)})
	hasher.Write(msg.Payload)
	return hex.EncodeToString(hasher.Sum(nil))
}

// HasSeen은 메시지를 이미 봤는지 확인합니다.
//
// [동시성]
// - 여러 피어에서 동시에 같은 메시지 수신 가능
// - RWMutex로 동시 읽기는 허용
func (bm *BroadcastManager) HasSeen(hash string) bool {
	bm.seenMu.RLock()
	_, exists := bm.seenMessages[hash]
	bm.seenMu.RUnlock()
	return exists
}

// MarkSeen은 메시지를 본 것으로 표시합니다.
//
// [반환값]
// - true: 새로운 메시지 (처음 봄)
// - false: 이미 본 메시지
//
// [사용 패턴]
//
//	if bm.MarkSeen(hash) {
//	    // 새 메시지, 처리 및 전파
//	} else {
//	    // 이미 본 메시지, 무시
//	}
func (bm *BroadcastManager) MarkSeen(hash string) bool {
	bm.seenMu.Lock()
	defer bm.seenMu.Unlock()

	if _, exists := bm.seenMessages[hash]; exists {
		return false // 이미 봄
	}

	bm.seenMessages[hash] = time.Now()

	// 최대 개수 초과 시 오래된 것 제거
	if len(bm.seenMessages) > bm.config.MaxSeenMessages {
		bm.removeOldestLocked()
	}

	return true // 새로운 메시지
}

// removeOldestLocked는 가장 오래된 메시지 해시를 제거합니다.
// seenMu.Lock()이 잡힌 상태에서 호출해야 합니다.
func (bm *BroadcastManager) removeOldestLocked() {
	var oldestHash string
	var oldestTime time.Time

	for hash, seenAt := range bm.seenMessages {
		if oldestHash == "" || seenAt.Before(oldestTime) {
			oldestHash = hash
			oldestTime = seenAt
		}
	}

	if oldestHash != "" {
		delete(bm.seenMessages, oldestHash)
	}
}

// =============================================================================
// 브로드캐스트 메서드
// =============================================================================

// Broadcast는 모든 연결된 피어에게 메시지를 전송합니다.
// (플러딩 방식)
//
// [언제 사용?]
// - 중요한 메시지 (블록, 중요 트랜잭션)
// - 피어 수가 적을 때
// - 빠른 전파가 필요할 때
//
// [파라미터]
// - msg: 전송할 메시지
// - exclude: 제외할 피어 ID (메시지를 보낸 피어 등)
func (bm *BroadcastManager) Broadcast(msg *protocol.Message, exclude map[peer.ID]bool) int {
	data := msg.Encode()
	hash := MessageHash(msg)

	// 이미 본 메시지면 전파하지 않음
	if !bm.MarkSeen(hash) {
		return 0
	}

	count := 0
	bm.node.peerManager.ForEachPeer(func(p *peer.Peer) bool {
		// 제외 목록 확인
		if exclude != nil && exclude[p.ID()] {
			return true
		}

		// 연결 상태 확인
		if !p.IsConnected() {
			return true
		}

		// 메시지 전송
		if err := p.WriteMessage(data); err != nil {
			log.Printf("브로드캐스트 실패 (%s): %v", p.ID().ShortString(), err)
		} else {
			count++
		}

		return true
	})

	return count
}

// GossipBroadcast는 일부 피어에게만 메시지를 전송합니다.
// (가십 방식)
//
// [동작]
// 1. 연결된 피어 중 GossipFanout 개를 랜덤 선택
// 2. 선택된 피어에게만 전송
// 3. 받은 피어들이 다시 자신의 피어에게 전파
//
// [장점]
// - 네트워크 부하 감소 (O(n) → O(fanout * log(n)))
// - 스케일러블
//
// [단점]
// - 전파 시간 약간 증가
// - 일부 노드가 못 받을 확률 존재 (매우 낮음)
func (bm *BroadcastManager) GossipBroadcast(msg *protocol.Message, exclude map[peer.ID]bool) int {
	data := msg.Encode()
	hash := MessageHash(msg)

	// 이미 본 메시지면 전파하지 않음
	if !bm.MarkSeen(hash) {
		return 0
	}

	// 전파 대상 피어 선택
	peers := bm.selectGossipTargets(exclude)

	count := 0
	for _, p := range peers {
		if err := p.WriteMessage(data); err != nil {
			log.Printf("가십 브로드캐스트 실패 (%s): %v", p.ID().ShortString(), err)
		} else {
			count++
		}
	}

	return count
}

// selectGossipTargets는 가십 전파 대상 피어를 선택합니다.
//
// [선택 방법]
// 1. 연결된 피어 목록 가져오기
// 2. 제외 목록 필터링
// 3. 셔플 (랜덤성 확보)
// 4. fanout 개 선택
//
// [왜 랜덤?]
// - 특정 피어에 부하 집중 방지
// - 네트워크 토폴로지에 따른 편향 방지
// - 장애 시 다른 경로로 전파 가능
func (bm *BroadcastManager) selectGossipTargets(exclude map[peer.ID]bool) []*peer.Peer {
	allPeers := bm.node.peerManager.GetConnectedPeers()

	// 제외 목록 필터링
	candidates := make([]*peer.Peer, 0, len(allPeers))
	for _, p := range allPeers {
		if exclude != nil && exclude[p.ID()] {
			continue
		}
		candidates = append(candidates, p)
	}

	// 피어 수가 fanout보다 적으면 모두 반환
	if len(candidates) <= bm.config.GossipFanout {
		return candidates
	}

	// 셔플
	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	// fanout 개 선택
	return candidates[:bm.config.GossipFanout]
}

// =============================================================================
// 릴레이 (수신 메시지 재전파)
// =============================================================================

// Relay는 수신한 메시지를 다른 피어에게 재전파합니다.
//
// [언제 사용?]
// 피어로부터 메시지를 받았을 때, 다른 피어들에게 전달
//
// [파라미터]
// - msg: 재전파할 메시지
// - from: 메시지를 보낸 피어 (재전파에서 제외)
//
// [동작]
// 1. 메시지 해시 확인 (이미 전파했으면 스킵)
// 2. from 피어 제외하고 가십 전파
func (bm *BroadcastManager) Relay(msg *protocol.Message, from *peer.Peer) int {
	exclude := map[peer.ID]bool{
		from.ID(): true,
	}
	return bm.GossipBroadcast(msg, exclude)
}

// RelayIfNew는 새 메시지인 경우에만 재전파합니다.
//
// [반환값]
// - int: 전파한 피어 수
// - bool: 새 메시지 여부 (true면 처리해야 함)
//
// [사용 패턴]
//
//	count, isNew := bm.RelayIfNew(msg, fromPeer)
//	if isNew {
//	    // 새 메시지, 로컬에서 처리
//	    handleMessage(msg)
//	}
//	// count개 피어에게 전파됨
func (bm *BroadcastManager) RelayIfNew(msg *protocol.Message, from *peer.Peer) (int, bool) {
	hash := MessageHash(msg)

	// 새 메시지인지 확인하고 마킹
	if !bm.MarkSeen(hash) {
		return 0, false // 이미 본 메시지
	}

	// 전파
	exclude := map[peer.ID]bool{
		from.ID(): true,
	}

	data := msg.Encode()
	peers := bm.selectGossipTargets(exclude)

	count := 0
	for _, p := range peers {
		if err := p.WriteMessage(data); err != nil {
			log.Printf("릴레이 실패 (%s): %v", p.ID().ShortString(), err)
		} else {
			count++
		}
	}

	return count, true
}

// =============================================================================
// 통계
// =============================================================================

// Stats는 브로드캐스트 통계를 반환합니다.
type BroadcastStats struct {
	// SeenMessagesCount는 현재 캐시된 메시지 해시 수입니다.
	SeenMessagesCount int
}

// GetStats는 현재 통계를 반환합니다.
func (bm *BroadcastManager) GetStats() BroadcastStats {
	bm.seenMu.RLock()
	count := len(bm.seenMessages)
	bm.seenMu.RUnlock()

	return BroadcastStats{
		SeenMessagesCount: count,
	}
}
