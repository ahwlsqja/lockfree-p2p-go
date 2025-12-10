package discovery

import (
	"context"
	"log"
	"sync"
	"time"
)

// =============================================================================
// 시드 노드 기반 디스커버리
// =============================================================================

// SeedDiscovery는 시드 노드를 기반으로 피어를 찾습니다.
//
// [동작 방식]
// 1. 하드코딩된 시드 노드 목록으로 시작
// 2. 시드 노드에 연결해서 GetPeers 요청
// 3. 받은 피어 목록을 저장
// 4. 주기적으로 반복
//
// [장점]
// - 구현이 가장 간단
// - 부트스트랩에 효과적
// - 네트워크 초기에 유용
//
// [단점]
// - 시드 노드가 단일 장애점(SPOF)이 될 수 있음
// - 시드 노드 운영자에게 의존
// - 시드 노드가 모두 죽으면 새 노드 참여 불가
//
// [실제 사용]
// Bitcoin, Ethereum 모두 시드 노드 + 다른 방식 혼용
// 시드는 부트스트랩용, 이후에는 가십이나 DHT로 피어 확장
type SeedDiscovery struct {
	// seeds는 시드 노드 주소 목록입니다.
	// 예: ["seed1.example.com:3000", "seed2.example.com:3000"]
	seeds []string

	// peers는 발견된 피어 목록입니다.
	// key: 피어 주소, value: 피어 정보
	//
	// [왜 map을 쓰나?]
	// - O(1) 조회/추가/삭제
	// - 중복 방지 자동
	peers map[string]*PeerInfo

	// mu는 peers 맵을 보호하는 뮤텍스입니다.
	//
	// [왜 sync.Map 대신 일반 map + mutex?]
	// - 피어 목록은 자주 순회함 (Range 연산)
	// - sync.Map은 Range 중에 다른 연산이 섞이면 성능 저하
	// - 읽기/쓰기 비율이 비슷하면 일반 map + RWMutex가 나음
	mu sync.RWMutex

	// config는 디스커버리 설정입니다.
	config Config

	// eventHandlers는 이벤트 핸들러 목록입니다.
	eventHandlers []EventHandler
	handlerMu     sync.RWMutex

	// 실행 상태 관리
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup

	// localAddr은 자신의 주소입니다 (광고용).
	localAddr string
}

// NewSeedDiscovery는 새로운 시드 기반 디스커버리를 생성합니다.
//
// [파라미터]
// - seeds: 시드 노드 주소 목록
// - localAddr: 자신의 리스닝 주소 (다른 피어에게 알려줄 주소)
// - config: 디스커버리 설정 (nil이면 기본값 사용)
func NewSeedDiscovery(seeds []string, localAddr string, config *Config) *SeedDiscovery {
	cfg := DefaultConfig
	if config != nil {
		cfg = *config
	}

	sd := &SeedDiscovery{
		seeds:     make([]string, len(seeds)),
		peers:     make(map[string]*PeerInfo),
		config:    cfg,
		stopCh:    make(chan struct{}),
		localAddr: localAddr,
	}

	// 시드 목록 복사 (외부 수정 방지)
	copy(sd.seeds, seeds)

	// 시드 노드들을 초기 피어로 등록
	for _, seed := range seeds {
		sd.peers[seed] = &PeerInfo{
			Addr:     seed,
			Source:   "seed",
			LastSeen: time.Now(),
		}
	}

	return sd
}

// Start는 디스커버리를 시작합니다.
//
// [동작]
// 1. 백그라운드 고루틴 시작
// 2. 즉시 첫 피어 탐색 수행
// 3. 이후 주기적으로 반복
func (sd *SeedDiscovery) Start() error {
	sd.mu.Lock()
	if sd.running {
		sd.mu.Unlock()
		return nil // 이미 실행 중ㅃ
	}
	// 상태를 true로 만들고
	sd.running = true
	// 신호만 보내는 채널임 빈 구조체 채널
	sd.stopCh = make(chan struct{})
	sd.mu.Unlock()

	// 백그라운드 피어 탐색 루프
	sd.wg.Add(1)
	// 이 루프가 도는데 Stop()을 불러서 이 루프를 정지 시킨다고 보면 된다890
	go sd.discoveryLoop()

	log.Printf("[Discovery] 시드 디스커버리 시작 (시드: %d개)", len(sd.seeds))
	return nil
}

// Stop은 디스커버리를 중지합니다.
func (sd *SeedDiscovery) Stop() error {
	sd.mu.Lock()
	if !sd.running {
		sd.mu.Unlock()
		return nil
	}
	sd.running = false
	// close 하면 채널 상태 자체를 close로 닫아서 모든거 꺠움
	close(sd.stopCh)
	sd.mu.Unlock()

	// 모든 고루틴 종료 대기
	sd.wg.Wait()

	log.Printf("[Discovery] 시드 디스커버리 중지")
	return nil
}

// discoveryLoop는 주기적으로 피어를 탐색하는 백그라운드 루프입니다.
func (sd *SeedDiscovery) discoveryLoop() {
	defer sd.wg.Done()

	// 시작하자마자 한 번 실행
	sd.cleanupStalePeers()

	// 주기적 실행
	ticker := time.NewTicker(sd.config.DiscoveryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-sd.stopCh:
			return
		case <-ticker.C:
			sd.cleanupStalePeers()
		}
	}
}

// cleanupStalePeers는 오래된 피어 정보를 정리합니다.
//
// [왜 필요한가?]
// - 네트워크에서 떠난 노드 정보가 계속 쌓이면 메모리 낭비
// - 오래된 정보로 연결 시도하면 시간 낭비
// - 피어 목록 품질 유지
func (sd *SeedDiscovery) cleanupStalePeers() {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	now := time.Now()
	for addr, peer := range sd.peers {
		// 시드 노드는 제거하지 않음
		if peer.Source == "seed" {
			continue
		}

		// TTL 초과한 피어 제거
		if now.Sub(peer.LastSeen) > sd.config.PeerTTL {
			delete(sd.peers, addr)
			sd.emitEventLocked(Event{
				Type: EventPeerRemoved,
				Peer: peer,
			})
			log.Printf("[Discovery] 오래된 피어 제거: %s", addr)
		}
	}

	// 최대 피어 수 초과 시 오래된 것부터 제거
	if len(sd.peers) > sd.config.MaxPeers {
		sd.trimOldestPeersLocked(sd.config.MaxPeers)
	}
}

// trimOldestPeersLocked는 오래된 피어를 제거하여 maxCount 이하로 만듭니다.
// mu.Lock()이 잡힌 상태에서 호출해야 합니다.
func (sd *SeedDiscovery) trimOldestPeersLocked(maxCount int) {
	// 간단한 구현: LastSeen이 가장 오래된 것 제거
	// 실제로는 힙이나 정렬된 리스트로 최적화 가능

	for len(sd.peers) > maxCount {
		var oldestAddr string
		var oldestTime time.Time

		for addr, peer := range sd.peers {
			// 시드는 제거하지 않음
			if peer.Source == "seed" {
				continue
			}

			if oldestAddr == "" || peer.LastSeen.Before(oldestTime) {
				oldestAddr = addr
				oldestTime = peer.LastSeen
			}
		}

		if oldestAddr == "" {
			break // 시드만 남음
		}

		peer := sd.peers[oldestAddr]
		delete(sd.peers, oldestAddr)
		sd.emitEventLocked(Event{
			Type: EventPeerRemoved,
			Peer: peer,
		})
	}
}

// FindPeers는 피어 목록을 반환합니다.
//
// [시드 디스커버리에서의 동작]
// - 능동적으로 네트워크를 탐색하지 않음
// - 현재 알고 있는 피어 목록만 반환
// - 실제 피어 수집은 노드가 연결 후 GetPeers로 함
func (sd *SeedDiscovery) FindPeers(ctx context.Context, limit int) ([]*PeerInfo, error) {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	// 자기 자신 제외하고 피어 목록 생성
	peers := make([]*PeerInfo, 0, len(sd.peers))
	for addr, peer := range sd.peers {
		// 자기 자신 제외
		if addr == sd.localAddr {
			continue
		}

		peers = append(peers, peer)

		// limit이 지정되어 있으면 제한
		if limit > 0 && len(peers) >= limit {
			break
		}
	}

	return peers, nil
}

// Advertise는 시드 디스커버리에서는 특별한 동작이 없습니다.
// (시드 노드에 자신을 등록하는 기능은 별도 구현 필요)
func (sd *SeedDiscovery) Advertise(ctx context.Context) error {
	// 시드 기반에서는 passive - 연결된 피어들이 GetPeers 요청할 때 응답
	return nil
}

// AddPeer는 피어를 목록에 추가합니다.
//
// [호출 시점]
// - 다른 피어로부터 Peers 메시지 받았을 때
// - 새 피어가 연결되었을 때
func (sd *SeedDiscovery) AddPeer(info *PeerInfo) {
	if info == nil || info.Addr == "" {
		return
	}

	// 자기 자신은 추가하지 않음
	if info.Addr == sd.localAddr {
		return
	}

	sd.mu.Lock()
	defer sd.mu.Unlock()

	existing, exists := sd.peers[info.Addr]
	if exists {
		// 이미 있으면 정보 업데이트
		existing.LastSeen = time.Now()
		if info.ID != "" {
			existing.ID = info.ID
		}
		sd.emitEventLocked(Event{
			Type: EventPeerUpdated,
			Peer: existing,
		})
	} else {
		// 새 피어 추가
		info.LastSeen = time.Now()
		if info.Source == "" {
			info.Source = "gossip"
		}
		sd.peers[info.Addr] = info
		sd.emitEventLocked(Event{
			Type: EventPeerDiscovered,
			Peer: info,
		})
		log.Printf("[Discovery] 새 피어 발견: %s (출처: %s)", info.Addr, info.Source)
	}
}

// RemovePeer는 피어를 목록에서 제거합니다.
func (sd *SeedDiscovery) RemovePeer(addr string) {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	if peer, exists := sd.peers[addr]; exists {
		delete(sd.peers, addr)
		sd.emitEventLocked(Event{
			Type: EventPeerRemoved,
			Peer: peer,
		})
		log.Printf("[Discovery] 피어 제거: %s", addr)
	}
}

// GetPeers는 알고 있는 모든 피어 목록을 반환합니다.
func (sd *SeedDiscovery) GetPeers() []*PeerInfo {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	peers := make([]*PeerInfo, 0, len(sd.peers))
	for _, peer := range sd.peers {
		peers = append(peers, peer)
	}
	return peers
}

// PeerCount는 알고 있는 피어 수를 반환합니다.
func (sd *SeedDiscovery) PeerCount() int {
	sd.mu.RLock()
	defer sd.mu.RUnlock()
	return len(sd.peers)
}

// =============================================================================
// 이벤트 시스템
// =============================================================================

// OnEvent는 이벤트 핸들러를 등록합니다.
//
// [사용 예]
//
//	discovery.OnEvent(func(e Event) {
//	    if e.Type == EventPeerDiscovered {
//	        // 새 피어 발견 시 연결 시도
//	        node.Connect(e.Peer.Addr)
//	    }
//	})
func (sd *SeedDiscovery) OnEvent(handler EventHandler) {
	sd.handlerMu.Lock()
	defer sd.handlerMu.Unlock()
	sd.eventHandlers = append(sd.eventHandlers, handler)
}

// emitEventLocked는 이벤트를 발생시킵니다.
// mu가 이미 잠겨있는 상태에서 호출해야 합니다.
func (sd *SeedDiscovery) emitEventLocked(event Event) {
	// 읽기락 걸고
	sd.handlerMu.RLock()
	// len(sd.eventHandlers)만큼 배열 만듬
	handlers := make([]EventHandler, len(sd.eventHandlers))
	copy(handlers, sd.eventHandlers)
	sd.handlerMu.RUnlock()

	// 핸들러 호출은 락 밖에서 (데드락 방지)
	for _, handler := range handlers {
		// 고루틴에서 실행하면 순서 보장 안 됨
		// 동기 실행하되 핸들러가 빨리 리턴하도록 기대
		handler(event)
	}
}

// =============================================================================
// 유틸리티 메서드
// =============================================================================

// GetPeersForGossip은 가십 전파를 위한 피어 목록을 반환합니다.
//
// [동작]
// - 자기 자신 제외
// - limit개만 반환
// - 가능하면 다양한 소스의 피어 포함
func (sd *SeedDiscovery) GetPeersForGossip(limit int) []*PeerInfo {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	peers := make([]*PeerInfo, 0, limit)
	for addr, peer := range sd.peers {
		if addr == sd.localAddr {
			continue
		}
		peers = append(peers, peer)
		if len(peers) >= limit {
			break
		}
	}
	return peers
}

// MarkAttempt는 피어 연결 시도를 기록합니다.
//
// [사용 시점]
// 연결 시도 직전에 호출하여 재시도 로직에서 참조
func (sd *SeedDiscovery) MarkAttempt(addr string) {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	if peer, exists := sd.peers[addr]; exists {
		peer.Attempts++
		peer.LastAttempt = time.Now()
	}
}

// MarkSuccess는 피어 연결 성공을 기록합니다.
//
// [사용 시점]
// 연결 성공 후 호출하여 통계 초기화
func (sd *SeedDiscovery) MarkSuccess(addr string) {
	sd.mu.Lock()
	defer sd.mu.Unlock()

	if peer, exists := sd.peers[addr]; exists {
		peer.Attempts = 0
		peer.LastAttempt = time.Time{}
		peer.LastSeen = time.Now()
	}
}

// GetRetryablePeers는 재시도 가능한 피어 목록을 반환합니다.
//
// [사용 케이스]
// 노드가 주기적으로 이 목록을 확인하고 연결 시도
func (sd *SeedDiscovery) GetRetryablePeers() []*PeerInfo {
	sd.mu.RLock()
	defer sd.mu.RUnlock()

	peers := make([]*PeerInfo, 0)
	for addr, peer := range sd.peers {
		if addr == sd.localAddr {
			continue
		}
		if peer.ShouldRetry(sd.config.RetryBaseInterval, sd.config.MaxRetryAttempts) {
			peers = append(peers, peer)
		}
	}
	return peers
}
