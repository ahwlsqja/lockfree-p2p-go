// Package peer는 P2P 네트워크의 피어 연결을 관리합니다.
// 피어는 네트워크에서 연결된 다른 노드를 추상화합니다.
package peer

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-p2p-network/go-p2p/pkg/transport"
)

// =============================================================================
// Peer ID - 피어 식별자
// =============================================================================

// IDLength는 피어 ID의 바이트 길이입니다.
//
// [왜 16바이트(128비트)인가?]
// - UUID와 동일한 크기 (충분한 유일성 보장)
// - 2^128 가지 가능한 ID → 충돌 확률 극히 낮음
// - Hex로 인코딩하면 32글자 문자열
const IDLength = 16

// ID는 피어의 고유 식별자입니다.
//
// [ID 생성 방식]
// - 암호학적으로 안전한 난수로 생성 (crypto/rand)
// - math/rand는 예측 가능하므로 사용 금지
type ID [IDLength]byte

// GenerateID는 새로운 랜덤 피어 ID를 생성합니다.
//
// [crypto/rand vs math/rand]
// - crypto/rand: OS의 암호학적 난수 생성기 사용 (/dev/urandom 또는 CryptGenRandom)
// - math/rand: PRNG(의사 난수 생성기), 시드를 알면 예측 가능
// - P2P에서 ID가 예측 가능하면 스푸핑 공격 가능
//
// [OS 레벨에서 일어나는 일]
// - Linux: /dev/urandom에서 읽기 (getrandom 시스템 콜)
// - Windows: CryptGenRandom API 호출
// - macOS: /dev/random (FreeBSD 스타일)
func GenerateID() (ID, error) {
	var id ID
	// rand.Read는 len(id) 바이트의 난수를 채움
	// 실패할 경우는 극히 드묾 (OS 난수 풀 고갈 시)
	_, err := rand.Read(id[:])
	if err != nil {
		return ID{}, fmt.Errorf("ID 생성 실패: %w", err)
	}
	return id, nil
}

// String은 ID를 hex 문자열로 반환합니다.
//
// [형식]
// - 32글자 hex 문자열 (예: "a1b2c3d4e5f6...")
// - 로깅과 디버깅에 사용
func (id ID) String() string {
	return hex.EncodeToString(id[:])
}

// ShortString은 ID의 앞 8글자만 반환합니다.
//
// [용도]
// - 로그에서 읽기 쉽게 (32글자는 너무 김)
// - "a1b2c3d4..." 형태로 표시
func (id ID) ShortString() string {
	return id.String()[:8] + "..."
}

// ParseID는 hex 문자열을 ID로 파싱합니다.
func ParseID(s string) (ID, error) {
	bytes, err := hex.DecodeString(s)
	if err != nil {
		return ID{}, fmt.Errorf("잘못된 ID 형식: %w", err)
	}
	if len(bytes) != IDLength {
		return ID{}, fmt.Errorf("잘못된 ID 길이: %d (expected %d)", len(bytes), IDLength)
	}
	var id ID
	copy(id[:], bytes)
	return id, nil
}

// =============================================================================
// Peer - 피어 연결 추상화
// =============================================================================

// State는 피어의 연결 상태를 나타냅니다.
type State uint32

const (
	// StateDisconnected는 연결되지 않은 상태입니다.
	StateDisconnected State = iota

	// StateConnecting는 연결 중인 상태입니다.
	StateConnecting

	// StateHandshaking는 핸드셰이크 진행 중입니다.
	StateHandshaking

	// StateConnected는 정상 연결된 상태입니다.
	StateConnected

	// StateDisconnecting은 연결 종료 중입니다.
	StateDisconnecting
)

// String은 상태를 문자열로 반환합니다.
func (s State) String() string {
	switch s {
	case StateDisconnected:
		return "Disconnected"
	case StateConnecting:
		return "Connecting"
	case StateHandshaking:
		return "Handshaking"
	case StateConnected:
		return "Connected"
	case StateDisconnecting:
		return "Disconnecting"
	default:
		return "Unknown"
	}
}

// Direction은 연결 방향을 나타냅니다.
type Direction uint8

const (
	// Inbound는 상대방이 우리에게 연결한 경우입니다.
	//
	// [특징]
	// - Accept()로 받은 연결
	// - 상대방의 IP는 알지만 리스닝 포트는 핸드셰이크로 알아야 함
	Inbound Direction = iota

	// Outbound는 우리가 상대방에게 연결한 경우입니다.
	//
	// [특징]
	// - Dial()로 만든 연결
	// - 상대방의 리스닝 주소를 이미 알고 있음
	Outbound
)

// String은 방향을 문자열로 반환합니다.
func (d Direction) String() string {
	if d == Inbound {
		return "inbound"
	}
	return "outbound"
}

// Peer는 연결된 피어를 나타냅니다.
//
// [피어의 역할]
// - 네트워크 연결을 추상화
// - 상대방 노드의 정보 저장
// - 메시지 송수신 통계 관리
//
// [스레드 안전성]
// - 여러 고루틴에서 동시 접근 가능하도록 설계
// - atomic 연산과 mutex로 동기화
type Peer struct {
	// id는 피어의 고유 식별자입니다.
	// 핸드셰이크 전까지는 zero value일 수 있음
	id ID

	// conn은 underlying 네트워크 연결입니다.
	conn *transport.Connection

	// addr은 피어의 리스닝 주소입니다.
	// 형식: "host:port" (예: "192.168.1.100:3000")
	//
	// [inbound 연결의 경우]
	// - conn.RemoteAddr()은 상대방의 다이얼 포트 (임시 포트)
	// - 실제 리스닝 포트는 핸드셰이크로 받아야 함
	addr string

	// direction은 연결 방향입니다.
	direction Direction

	// state는 현재 연결 상태입니다.
	//
	// [왜 atomic인가?]
	// - 여러 고루틴에서 상태 읽기/쓰기 가능
	// - 상태 전이는 단순하므로 CAS로 충분
	// - mutex보다 가벼움
	state uint32 // atomic, State 타입

	// createdAt은 피어 객체 생성 시각입니다.
	createdAt time.Time

	// connectedAt은 연결 완료(핸드셰이크 후) 시각입니다.
	connectedAt time.Time

	// lastSeen은 마지막으로 메시지를 받은 시각입니다.
	//
	// [용도]
	// - 죽은 연결 감지 (lastSeen이 오래됨)
	// - Ping/Pong 주기 결정
	lastSeen    time.Time
	lastSeenMu  sync.RWMutex

	// 통계 정보 (atomic으로 관리)
	//
	// [왜 atomic인가?]
	// - 읽기/쓰기 고루틴이 다름
	// - 정확한 값보다 대략적인 통계가 중요
	// - mutex는 오버헤드가 큼
	bytesSent     uint64 // atomic
	bytesReceived uint64 // atomic
	messagesSent  uint64 // atomic
	messagesRecv  uint64 // atomic

	// version은 피어의 프로토콜 버전입니다 (핸드셰이크로 받음).
	version uint32

	// userAgent는 피어의 클라이언트 식별자입니다.
	// 예: "go-p2p/1.0.0"
	userAgent string

	// metadata는 피어별 추가 데이터입니다.
	// 애플리케이션에서 자유롭게 사용
	metadata   map[string]interface{}
	metadataMu sync.RWMutex
}

// NewPeer는 새로운 Peer 객체를 생성합니다.
//
// [파라미터]
// - conn: 수립된 TCP 연결
// - direction: 연결 방향 (Inbound/Outbound)
//
// [주의] 이 시점에서는 아직 핸드셰이크가 안 됐음
// - ID는 zero value
// - 상태는 StateConnecting
func NewPeer(conn *transport.Connection, direction Direction) *Peer {
	now := time.Now()
	return &Peer{
		conn:      conn,
		direction: direction,
		state:     uint32(StateConnecting),
		createdAt: now,
		lastSeen:  now,
		metadata:  make(map[string]interface{}),
	}
}

// =============================================================================
// Getter 메서드
// =============================================================================

// ID는 피어의 식별자를 반환합니다.
func (p *Peer) ID() ID {
	return p.id
}

// SetID는 피어의 식별자를 설정합니다 (핸드셰이크 시 사용).
func (p *Peer) SetID(id ID) {
	p.id = id
}

// Addr는 피어의 리스닝 주소를 반환합니다.
func (p *Peer) Addr() string {
	return p.addr
}

// SetAddr은 피어의 리스닝 주소를 설정합니다.
func (p *Peer) SetAddr(addr string) {
	p.addr = addr
}

// Direction은 연결 방향을 반환합니다.
func (p *Peer) Direction() Direction {
	return p.direction
}

// State는 현재 연결 상태를 반환합니다.
//
// [atomic.LoadUint32]
// - 다른 고루틴이 쓰는 중에도 안전하게 읽을 수 있음
// - 메모리 배리어 포함하여 최신 값 보장
func (p *Peer) State() State {
	return State(atomic.LoadUint32(&p.state))
}

// SetState는 연결 상태를 변경합니다.
//
// [atomic.StoreUint32]
// - 다른 고루틴이 읽는 중에도 안전하게 쓸 수 있음
// - 쓰기 후 다른 고루틴에서 즉시 보임 (memory ordering)
func (p *Peer) SetState(state State) {
	atomic.StoreUint32(&p.state, uint32(state))
}

// IsConnected는 피어가 연결되어 있는지 확인합니다.
func (p *Peer) IsConnected() bool {
	return p.State() == StateConnected
}

// RemoteAddr은 원격 주소를 반환합니다.
//
// [addr vs RemoteAddr 차이]
// - addr: 피어의 리스닝 주소 (P2P용)
// - RemoteAddr: TCP 연결의 실제 원격 주소 (다이얼 포트 포함)
func (p *Peer) RemoteAddr() string {
	if p.conn == nil {
		return ""
	}
	return p.conn.RemoteAddr().String()
}

// Version은 피어의 프로토콜 버전을 반환합니다.
func (p *Peer) Version() uint32 {
	return p.version
}

// SetVersion은 피어의 프로토콜 버전을 설정합니다.
func (p *Peer) SetVersion(version uint32) {
	p.version = version
}

// UserAgent는 피어의 클라이언트 식별자를 반환합니다.
func (p *Peer) UserAgent() string {
	return p.userAgent
}

// SetUserAgent는 피어의 클라이언트 식별자를 설정합니다.
func (p *Peer) SetUserAgent(ua string) {
	p.userAgent = ua
}

// CreatedAt은 피어 생성 시각을 반환합니다.
func (p *Peer) CreatedAt() time.Time {
	return p.createdAt
}

// ConnectedAt은 연결 완료 시각을 반환합니다.
func (p *Peer) ConnectedAt() time.Time {
	return p.connectedAt
}

// SetConnectedAt은 연결 완료 시각을 설정합니다.
func (p *Peer) SetConnectedAt(t time.Time) {
	p.connectedAt = t
}

// LastSeen은 마지막 활동 시각을 반환합니다.
func (p *Peer) LastSeen() time.Time {
	p.lastSeenMu.RLock()
	defer p.lastSeenMu.RUnlock()
	return p.lastSeen
}

// UpdateLastSeen은 마지막 활동 시각을 현재로 갱신합니다.
func (p *Peer) UpdateLastSeen() {
	p.lastSeenMu.Lock()
	p.lastSeen = time.Now()
	p.lastSeenMu.Unlock()
}

// =============================================================================
// 통계 메서드
// =============================================================================

// Stats는 피어의 통계 정보입니다.
type Stats struct {
	BytesSent     uint64
	BytesReceived uint64
	MessagesSent  uint64
	MessagesRecv  uint64
	ConnectedFor  time.Duration
}

// Stats는 피어의 통계 정보를 반환합니다.
func (p *Peer) Stats() Stats {
	var connectedFor time.Duration
	if !p.connectedAt.IsZero() {
		connectedFor = time.Since(p.connectedAt)
	}
	return Stats{
		BytesSent:     atomic.LoadUint64(&p.bytesSent),
		BytesReceived: atomic.LoadUint64(&p.bytesReceived),
		MessagesSent:  atomic.LoadUint64(&p.messagesSent),
		MessagesRecv:  atomic.LoadUint64(&p.messagesRecv),
		ConnectedFor:  connectedFor,
	}
}

// addBytesSent는 전송 바이트 수를 증가시킵니다.
func (p *Peer) addBytesSent(n uint64) {
	atomic.AddUint64(&p.bytesSent, n)
}

// addBytesReceived는 수신 바이트 수를 증가시킵니다.
func (p *Peer) addBytesReceived(n uint64) {
	atomic.AddUint64(&p.bytesReceived, n)
}

// addMessagesSent는 전송 메시지 수를 증가시킵니다.
func (p *Peer) addMessagesSent() {
	atomic.AddUint64(&p.messagesSent, 1)
}

// addMessagesRecv는 수신 메시지 수를 증가시킵니다.
func (p *Peer) addMessagesRecv() {
	atomic.AddUint64(&p.messagesRecv, 1)
}

// =============================================================================
// 메타데이터 메서드
// =============================================================================

// GetMetadata는 메타데이터 값을 반환합니다.
func (p *Peer) GetMetadata(key string) (interface{}, bool) {
	p.metadataMu.RLock()
	defer p.metadataMu.RUnlock()
	val, ok := p.metadata[key]
	return val, ok
}

// SetMetadata는 메타데이터 값을 설정합니다.
func (p *Peer) SetMetadata(key string, value interface{}) {
	p.metadataMu.Lock()
	p.metadata[key] = value
	p.metadataMu.Unlock()
}

// =============================================================================
// I/O 메서드
// =============================================================================

// ReadMessage는 피어로부터 메시지를 읽습니다.
//
// [반환]
// - []byte: 메시지 페이로드 (길이 prefix 제외)
// - error: 읽기 실패 시
//
// [통계 업데이트]
// - bytesReceived, messagesRecv 증가
// - lastSeen 갱신
func (p *Peer) ReadMessage() ([]byte, error) {
	data, err := p.conn.ReadMessage()
	if err != nil {
		return nil, err
	}

	// 통계 업데이트
	p.addBytesReceived(uint64(len(data) + 4)) // +4는 길이 prefix
	p.addMessagesRecv()
	p.UpdateLastSeen()

	return data, nil
}

// WriteMessage는 피어에게 메시지를 보냅니다.
//
// [스레드 안전]
// - transport.Connection이 내부적으로 mutex 사용
// - 여러 고루틴에서 동시에 호출해도 안전
func (p *Peer) WriteMessage(data []byte) error {
	err := p.conn.WriteMessage(data)
	if err != nil {
		return err
	}

	// 통계 업데이트
	p.addBytesSent(uint64(len(data) + 4)) // +4는 길이 prefix
	p.addMessagesSent()

	return nil
}

// Close는 피어 연결을 닫습니다.
//
// [상태 전이]
// Connected → Disconnecting → Disconnected
//
// [정리되는 것들]
// - TCP 연결 종료 (FIN 전송)
// - 버퍼 해제
func (p *Peer) Close() error {
	// 이미 종료 중이거나 종료됨
	currentState := p.State()
	if currentState == StateDisconnecting || currentState == StateDisconnected {
		return nil
	}

	p.SetState(StateDisconnecting)

	var err error
	if p.conn != nil {
		err = p.conn.Close()
	}

	p.SetState(StateDisconnected)
	return err
}

// String은 피어의 문자열 표현을 반환합니다.
func (p *Peer) String() string {
	return fmt.Sprintf("Peer{id=%s, addr=%s, state=%s, direction=%s}",
		p.id.ShortString(), p.addr, p.State(), p.direction)
}
