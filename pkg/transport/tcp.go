package transport

import (
	"fmt"
	"net"
	"time"
)

// =============================================================================
// TCP 서버 - 인바운드 연결 수신
// =============================================================================

// TCPServer는 TCP 연결을 수신 대기하는 서버입니다.
//
// [역할]
// P2P 네트워크에서 다른 노드들이 우리에게 연결할 수 있도록
// 지정된 포트에서 대기합니다.
type TCPServer struct {
	// listener는 OS 수준의 리스닝 소켓을 감싸는 Go 추상화입니다.
	//
	// [내부 구조]
	// - 실제로는 파일 디스크립터(fd) 하나를 가지고 있음
	// - 이 fd는 커널의 소켓 구조체와 연결됨
	// - Go 런타임이 이 fd를 epoll/kqueue에 등록하여 관리
	listener net.Listener

	// addr은 서버가 바인딩된 주소입니다 (예: ":3000", "0.0.0.0:3000")
	addr string
}

// NewTCPServer는 새로운 TCP 서버를 생성합니다.
//
// [인자]
// - addr: 바인딩할 주소 (예: ":3000"은 모든 인터페이스의 3000번 포트)
//
// [아직 실제 리스닝은 시작하지 않음]
// Start()를 호출해야 실제로 연결을 받기 시작합니다.
func NewTCPServer(addr string) *TCPServer {
	return &TCPServer{
		addr: addr,
	}
}

// Start는 서버를 시작하고 연결 수신을 시작합니다.
//
// [OS 레벨에서 일어나는 일]
// 1. socket() 시스템 콜 → 커널이 소켓 구조체 생성, fd 반환
//   - TCP 소켓이므로 SOCK_STREAM 타입
//   - 아직 어떤 주소에도 바인딩되지 않은 상태
//
// 2. bind() 시스템 콜 → 소켓에 IP:Port 바인딩
//   - 커널의 포트 테이블에 등록
//   - 1024 미만 포트는 root 권한 필요 (well-known ports)
//   - 이미 사용 중이면 "address already in use" 에러
//
// 3. listen() 시스템 콜 → 연결 대기 상태로 전환
//   - backlog 큐 생성 (완료된 연결 대기열)
//   - Go의 기본 backlog는 보통 128 또는 SOMAXCONN
//
// [에러 케이스]
// - 포트가 이미 사용 중: "bind: address already in use"
// - 권한 없음 (1024 미만): "bind: permission denied"
// - 잘못된 주소 형식: "invalid address"
func (s *TCPServer) Start() error {
	// net.Listen은 위의 socket() → bind() → listen()을 한 번에 수행
	//
	// "tcp": IPv4와 IPv6 모두 지원
	// "tcp4": IPv4만
	// "tcp6": IPv6만
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("TCP 서버 시작 실패: %w", err)
	}
	s.listener = listener
	return nil
}

// Accept는 새 클라이언트 연결을 수락합니다.
//
// [OS 레벨에서 일어나는 일]
// 1. accept() 시스템 콜 호출
//   - listen backlog에서 완료된 연결을 꺼냄
//   - 연결이 없으면 블로킹 (Go에서는 고루틴만 블록, 스레드는 계속 실행)
//
// 2. TCP 3-way handshake는 이미 커널이 완료
//   - 클라이언트: SYN →
//   - 서버(커널): ← SYN-ACK
//   - 클라이언트: ACK →
//   - 이 시점에 연결이 backlog에 들어감
//
// 3. 새 fd 할당
//   - 리스너 fd와 별개의 새로운 fd
//   - 이 fd가 클라이언트와의 실제 통신에 사용됨
//
// [반환]
// - *Connection: 새 연결을 감싸는 우리의 추상화
// - error: 연결 수락 실패 시 (예: 서버 종료됨)
//
// [사용 패턴]
//
//	for {
//	    conn, err := server.Accept()
//	    if err != nil {
//	        // 서버가 종료되었거나 에러 발생
//	        break
//	    }
//	    go handleConnection(conn)  // 각 연결을 별도 고루틴에서 처리
//	}
func (s *TCPServer) Accept() (*Connection, error) {
	// Accept()는 블로킹 호출처럼 보이지만,
	// Go 런타임이 내부적으로 epoll_wait()을 사용해서
	// 실제로는 고루틴만 파킹하고 OS 스레드는 다른 고루틴을 실행
	//
	// 이것이 Go가 수만 개의 연결을 적은 스레드로 처리할 수 있는 비결
	conn, err := s.listener.Accept()
	if err != nil {
		return nil, fmt.Errorf("연결 수락 실패: %w", err)
	}
	return NewConnection(conn), nil
}

// Addr은 서버가 리스닝 중인 주소를 반환합니다.
//
// [왜 이 메서드가 필요한가?]
// ":0"으로 리스닝하면 OS가 사용 가능한 포트를 자동 할당
// 실제로 어떤 포트가 할당되었는지 알아야 할 때 사용
func (s *TCPServer) Addr() net.Addr {
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

// Close는 서버를 종료합니다.
//
// [일어나는 일]
// 1. close() 시스템 콜로 리스닝 소켓 fd 닫음
// 2. Accept()에서 대기 중인 고루틴들이 에러와 함께 깨어남
// 3. 이미 수립된 연결들은 영향 없음 (별도 fd)
//
// [주의]
// Close() 후에는 Accept()를 호출하면 안 됨
// 기존 연결들은 별도로 닫아야 함
func (s *TCPServer) Close() error {
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

// =============================================================================
// TCP 다이얼러 - 아웃바운드 연결 생성
// =============================================================================

// DialConfig는 TCP 연결 설정입니다.
type DialConfig struct {
	// Timeout은 연결 시도 제한 시간입니다.
	//
	// [왜 타임아웃이 필요한가?]
	// - 상대방이 오프라인이면 TCP는 기본적으로 여러 번 재시도
	// - Linux 기본값: 약 2분 (SYN 재전송 6회)
	// - P2P 네트워크에서는 빠른 실패가 더 나음
	Timeout time.Duration

	// LocalAddr은 로컬 바인딩 주소입니다 (보통 nil로 OS에 맡김).
	//
	// [언제 지정하나?]
	// - 여러 네트워크 인터페이스가 있을 때 특정 인터페이스 사용
	// - NAT 환경에서 특정 로컬 포트 사용 필요 시
	LocalAddr *net.TCPAddr
}

// DefaultDialConfig는 기본 다이얼 설정입니다.
var DefaultDialConfig = DialConfig{
	Timeout:   10 * time.Second, // P2P 연결에 적절한 타임아웃
	LocalAddr: nil,              // OS가 알아서 선택
}

// Dial은 지정된 주소로 TCP 연결을 생성합니다.
//
// [OS 레벨에서 일어나는 일]
// 1. socket() → 클라이언트용 소켓 생성
//
// 2. (선택적) bind() → 로컬 주소 지정 시
//
// 3. connect() → 서버에 연결 시도
//   - 커널이 SYN 패킷 전송
//   - SYN-ACK 대기 (타임아웃 시 재전송)
//   - ACK 전송하고 ESTABLISHED 상태로
//
// [타임아웃 동작]
// - context나 Deadline으로 제어
// - 타임아웃 시 진행 중인 connect() 취소
// - "i/o timeout" 또는 "connection timed out" 에러
//
// [일반적인 실패 원인]
// - "connection refused": 상대방 포트가 열려있지 않음 (RST 받음)
// - "no route to host": 네트워크 경로 없음
// - "connection timed out": 응답 없음 (방화벽 등)
func Dial(addr string, config DialConfig) (*Connection, error) {
	// net.Dialer는 연결 생성에 대한 세부 설정을 제공
	dialer := net.Dialer{
		Timeout:   config.Timeout,
		LocalAddr: config.LocalAddr,

		// KeepAlive: TCP keepalive 프로브 간격
		// 연결이 살아있는지 주기적으로 확인
		// 0이면 OS 기본값 사용 (보통 2시간)
		KeepAlive: 30 * time.Second,
	}

	// Dial은 connect()를 수행하고 완료될 때까지 블로킹
	// (실제로는 고루틴만 블로킹, epoll로 논블로킹 처리)
	conn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("TCP 연결 실패 (%s): %w", addr, err)
	}
	return NewConnection(conn), nil
}

// DialWithDefaults는 기본 설정으로 TCP 연결을 생성합니다.
func DialWithDefaults(addr string) (*Connection, error) {
	return Dial(addr, DefaultDialConfig)
}
