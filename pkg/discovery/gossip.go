package discovery

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"
)

// =============================================================================
// 가십 프로토콜 기반 디스커버리
// =============================================================================

// GossipDiscovery는 가십 프로토콜을 사용한 피어 디스커버리입니다.
//
// [가십 프로토콜이란?]
// "소문"처럼 정보가 퍼져나가는 통신 방식입니다.
//
// 예시:
// 1. A가 새 피어 X를 알게 됨
// 2. A가 연결된 피어 B, C에게 "나 X 알아"라고 말함
// 3. B가 자기 피어 D, E에게 전달
// 4. C가 자기 피어 F, G에게 전달
// 5. 결국 네트워크 전체가 X를 알게 됨
//
// [특징]
// - 확률적 전파: 모든 피어에게 보내지 않고 랜덤 선택
// - 중복 허용: 같은 정보 여러 번 받을 수 있음 (멱등성 필요)
// - 최종 일관성: 언젠가는 모두가 같은 정보를 갖게 됨
// - 장애 내성: 일부 노드가 죽어도 정보 전파됨
//
// [Bitcoin/Ethereum에서의 사용]
// - 트랜잭션 전파
// - 블록 전파
// - 피어 목록 교환
type GossipDiscovery struct {
	// 기본 시드 디스커버리 내장 (composition)
	// 시드로 부트스트랩 후 가십으로 확장
	*SeedDiscovery

	// gossipConfig는 가십 전용 설정입니다.
	gossipConfig GossipConfig

	// recentGossip은 최근에 가십한 피어 정보입니다.
	// 같은 정보를 중복 전파하지 않기 위함
	//
	// [구조]
	// key: 피어 주소
	// value: 마지막 가십 시간
	recentGossip map[string]time.Time
	gossipMu     sync.RWMutex

	// gossipFunc는 실제 가십을 수행하는 함수입니다.
	// 노드에서 주입받아서 사용 (의존성 역전)
	//
	// [왜 함수를 주입받나?]
	// - 디스커버리는 네트워크 계층을 직접 알면 안 됨
	// - 순환 의존성 방지
	// - 테스트 시 mock 가능
	gossipFunc GossipFunc

	// 실행 상태
	gossipRunning bool
	gossipStopCh  chan struct{}
	gossipWg      sync.WaitGroup
}

// GossipFunc는 가십 메시지를 전송하는 함수 타입입니다.
//
// [파라미터]
// - peers: 전파할 피어 정보 목록
//
// [구현 책임]
// - 연결된 피어들에게 peers 정보를 담은 메시지 전송
// - 몇 명에게 보낼지는 구현체가 결정
type GossipFunc func(peers []*PeerInfo) error

// GossipConfig는 가십 프로토콜 설정입니다.
type GossipConfig struct {
	// GossipInterval은 가십 주기입니다.
	// 기본값: 60초
	//
	// [트레이드오프]
	// - 짧으면: 빠른 전파, 높은 네트워크 부하
	// - 길면: 느린 전파, 낮은 네트워크 부하
	GossipInterval time.Duration

	// GossipFanout은 한 번에 가십할 피어 수입니다.
	// 기본값: 3
	//
	// [왜 전체에게 안 보내나?]
	// - O(n²) 메시지 폭발 방지
	// - 각 노드가 3명에게만 보내도 log₃(n) 라운드면 전체 전파
	// - 1000 노드: 약 7 라운드 (7분이면 전체 전파)
	GossipFanout int

	// MaxGossipPeers는 한 번에 가십할 최대 피어 정보 수입니다.
	// 기본값: 10
	//
	// [왜 제한하나?]
	// - 메시지 크기 제한
	// - 수신자 처리 부담 경감
	MaxGossipPeers int

	// GossipTTL은 가십 정보의 캐시 유효 시간입니다.
	// 기본값: 5분
	//
	// [동작]
	// 이 시간 내에 같은 피어 정보는 다시 가십하지 않음
	GossipTTL time.Duration
}

// DefaultGossipConfig는 기본 가십 설정입니다.
var DefaultGossipConfig = GossipConfig{
	GossipInterval: 60 * time.Second,
	GossipFanout:   3,
	MaxGossipPeers: 10,
	GossipTTL:      5 * time.Minute,
}

// NewGossipDiscovery는 새로운 가십 기반 디스커버리를 생성합니다.
//
// [파라미터]
// - seeds: 시드 노드 목록 (부트스트랩용)
// - localAddr: 자신의 리스닝 주소
// - config: 기본 디스커버리 설정
// - gossipConfig: 가십 전용 설정 (nil이면 기본값)
func NewGossipDiscovery(seeds []string, localAddr string, config *Config, gossipConfig *GossipConfig) *GossipDiscovery {
	gc := DefaultGossipConfig
	if gossipConfig != nil {
		gc = *gossipConfig
	}

	gd := &GossipDiscovery{
		// 시드 불러오기 
		SeedDiscovery: NewSeedDiscovery(seeds, localAddr, config),
		gossipConfig:  gc,
		recentGossip:  make(map[string]time.Time),
		gossipStopCh:  make(chan struct{}),
	}

	// 가십디스커버리 리턴
	return gd
}

// SetGossipFunc는 가십 함수를 설정합니다.
//
// [사용 시점]
// 노드 초기화 시 실제 네트워크 전송 함수를 주입
//
// [예시]
//
//	discovery.SetGossipFunc(func(peers []*PeerInfo) error {
//	    msg := protocol.NewPeersMessage(peers)
//	    return node.Broadcast(msg)
//	})
func (gd *GossipDiscovery) SetGossipFunc(fn GossipFunc) {
	gd.gossipFunc = fn
}

// Start는 가십 디스커버리를 시작합니다.
func (gd *GossipDiscovery) Start() error {
	// 기본 시드 디스커버리 시작
	if err := gd.SeedDiscovery.Start(); err != nil {
		return err
	}

	// 락걸리
	gd.mu.Lock()
	// 얘 살아있으면 걍 리턴 
	if gd.gossipRunning {
		gd.mu.Unlock()
		return nil
	}
	// 죽어있으면 true 바꿈
	gd.gossipRunning = true
	// 이거 멈추는 채널 만드는데 close 쓸라고 걍 빈 구조체임.
	gd.gossipStopCh = make(chan struct{})
	gd.mu.Unlock()

	// 가십 루프 시작
	// 근데 공부하면서 안건데 먼저 Wg에 더해야됨. 왜냐하면 Wg는 애초부터 고루틴을 신경안씀 걍 원자적으로 카운트만 올림 그래서 gossipLoop() 에서 끝날 때 defer gd.gossipWg.Done() 해주는거임!
	gd.gossipWg.Add(1)
	go gd.gossipLoop()

	log.Printf("[Discovery] 가십 디스커버리 시작")
	return nil
}

// Stop은 가십 디스커버리를 중지합니다.
func (gd *GossipDiscovery) Stop() error {
	// 일단 락걸고 아까랑 똑같이 함 스타트랑
	gd.mu.Lock()
	if gd.gossipRunning {
		gd.gossipRunning = false
		close(gd.gossipStopCh)
	}
	gd.mu.Unlock()

	gd.gossipWg.Wait()

	// 기본 디스커버리도 중지
	return gd.SeedDiscovery.Stop()
}

// gossipLoop는 주기적으로 가십을 수행하는 백그라운드 루프입니다.
func (gd *GossipDiscovery) gossipLoop() {
	defer gd.gossipWg.Done()

	// 시작 시 약간의 랜덤 딜레이 (thundering herd 방지)
	//
	// [Thundering Herd 문제]
	// 모든 노드가 같은 시간에 가십하면 네트워크 부하 폭증
	// 랜덤 딜레이로 분산시킴
	jitter := time.Duration(rand.Int63n(int64(gd.gossipConfig.GossipInterval / 4)))
	select {
		// 이 타임 함수 자체가 채널을 만들어서 한다고 한다. 
		// func After(d Duration) <-chan Time 이렇게 정의되어 있대요
		// time.After(jitter) 자체가 채널 객체임
	case <-time.After(jitter):
	case <-gd.gossipStopCh:
		return
	}

	// 이거 자체가 주기적으로 신호를 보내는 타이머 객체를 생성해서 반환하는 친구임
	ticker := time.NewTicker(gd.gossipConfig.GossipInterval)
	defer ticker.Stop()

	for {
		select {
		case <-gd.gossipStopCh:
			return
		case <-ticker.C:
			gd.doGossip()
			gd.cleanupRecentGossip()
		}
	}
}

// doGossip은 실제 가십을 수행합니다.
//
// [동작]
// 1. 최근에 가십하지 않은 피어 정보 선택
// 2. gossipFunc를 통해 전파
// 3. 전파한 피어 정보를 recentGossip에 기록
func (gd *GossipDiscovery) doGossip() {
	if gd.gossipFunc == nil {
		return // 가십 함수가 설정되지 않음
	}

	// 가십할 피어 선택
	peersToGossip := gd.selectPeersToGossip()
	if len(peersToGossip) == 0 {
		return // 가십할 게 없음
	}

	// 가십 수행
	if err := gd.gossipFunc(peersToGossip); err != nil {
		log.Printf("[Discovery] 가십 실패: %v", err)
		return
	}

	// 가십한 피어 기록
	gd.gossipMu.Lock()
	now := time.Now()
	for _, peer := range peersToGossip {
		gd.recentGossip[peer.Addr] = now
	}
	gd.gossipMu.Unlock()

	log.Printf("[Discovery] 가십 완료: %d개 피어 정보 전파", len(peersToGossip))
}

// selectPeersToGossip은 가십할 피어 정보를 선택합니다. 가십한 피어정보들 다 모아서 후보군 랜덤
//
// [선택 기준]
// 1. 최근에 가십하지 않은 피어 (그니까 최신에 가십을 보내지않은 피어)
// 2. 최대 MaxGossipPeers개
// 3. 랜덤 섞기 (다양성 확보)
func (gd *GossipDiscovery) selectPeersToGossip() []*PeerInfo {
	gd.mu.RLock()
	// 현재 알고 있는 모든 피어 정보(gd.peers 맵의 값들)를 슬라이스(allPeers)로 복사하여 잠금 없이 나머지 작업을 진행할 수 있도록 준비합니다.
	allPeers := make([]*PeerInfo, 0, len(gd.peers))
	for _, peer := range gd.peers {
		allPeers = append(allPeers, peer)
	}
	gd.mu.RUnlock()

	gd.gossipMu.RLock()
	defer gd.gossipMu.RUnlock()

	// 최근에 가십하지 않은 피어 필터링
	now := time.Now()
	candidates := make([]*PeerInfo, 0, len(allPeers))
	for _, peer := range allPeers {
		// 가십 안됬거나 TTL 보다 오랜기간 지나면 후보군에 넣음
		lastGossip, exists := gd.recentGossip[peer.Addr]
		if !exists || now.Sub(lastGossip) > gd.gossipConfig.GossipTTL {
			candidates = append(candidates, peer)
		}
	}

	// 랜덤 섞기
	// 후보군의 순서를 완전히 무작위로 시킴
	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	// 최대 개수 제한
	if len(candidates) > gd.gossipConfig.MaxGossipPeers {
		candidates = candidates[:gd.gossipConfig.MaxGossipPeers]
	}

	return candidates
}

// cleanupRecentGossip은 오래된 가십 캐시를 정리합니다.
func (gd *GossipDiscovery) cleanupRecentGossip() {
	// gd.mu.RLock()을 사용하여 SeedDiscovery에 포함된 전체 피어 목록(gd.peers)에 대한 읽기 잠금을 획득
	gd.gossipMu.Lock()
	defer gd.gossipMu.Unlock()

	now := time.Now()
	for addr, lastGossip := range gd.recentGossip {
		if now.Sub(lastGossip) > gd.gossipConfig.GossipTTL*2 {
			delete(gd.recentGossip, addr)
		}
	}
}

// =============================================================================
// 가십 수신 처리
// =============================================================================

// HandleGossip은 다른 피어로부터 받은 가십 메시지를 처리합니다.
//
// [호출 시점]
// 노드가 Peers 메시지를 받았을 때
//
// [동작]
// 1. 받은 피어 정보를 목록에 추가
// 2. 새 피어면 이벤트 발생 (연결 시도하도록)
func (gd *GossipDiscovery) HandleGossip(peers []*PeerInfo) {
	for _, peer := range peers {
		gd.AddPeer(peer)
	}
}

// =============================================================================
// 유틸리티
// =============================================================================

// Advertise는 자신의 존재를 네트워크에 알립니다.
//
// [동작]
// 자신의 정보를 가십으로 전파
func (gd *GossipDiscovery) Advertise(ctx context.Context) error {
	if gd.gossipFunc == nil {
		return nil
	}

	// 자신의 정보 생성
	selfInfo := &PeerInfo{
		Addr:     gd.localAddr,
		Source:   "self",
		LastSeen: time.Now(),
	}

	// 가십 전파
	return gd.gossipFunc([]*PeerInfo{selfInfo})
}

// FindPeers는 피어를 찾습니다.
//
// [가십 디스커버리에서의 동작]
// - 현재 알고 있는 피어 목록 반환
// - 백그라운드 가십 루프가 자동으로 피어 목록 확장
func (gd *GossipDiscovery) FindPeers(ctx context.Context, limit int) ([]*PeerInfo, error) {
	return gd.SeedDiscovery.FindPeers(ctx, limit)
}

// GetRandomPeers는 랜덤하게 선택된 피어 목록을 반환합니다.
//
// [사용 케이스]
// - 메시지 브로드캐스트 대상 선정
// - 가십 전파 대상 선정
//
// [파라미터]
// - n: 반환할 피어 수
// - exclude: 제외할 피어 주소 (자기 자신, 메시지 발신자 등)
func (gd *GossipDiscovery) GetRandomPeers(n int, exclude map[string]bool) []*PeerInfo {
	gd.mu.RLock()
	defer gd.mu.RUnlock()

	// 제외 목록 필터링
	candidates := make([]*PeerInfo, 0, len(gd.peers))
	for addr, peer := range gd.peers {
		if exclude != nil && exclude[addr] {
			continue
		}
		if addr == gd.localAddr {
			continue
		}
		candidates = append(candidates, peer)
	}

	// 랜덤 섞기
	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	// n개 반환
	if len(candidates) > n {
		candidates = candidates[:n]
	}

	return candidates
}
