package transport

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// =============================================================================
// Connection - TCP 연결 추상화
// =============================================================================

// 타임아웃 상수
const (
	// DefaultReadTimeout은 읽기 작업의 기본 타임아웃입니다.
	//
	// [왜 30초인가?]
	// - P2P 네트워크에서 피어는 Ping/Pong으로 연결 확인 (보통 15초 간격)
	// - 30초면 2번의 Ping 주기 동안 응답 없음 = 연결 문제
	// - 너무 짧으면 네트워크 지연에 민감, 너무 길면 죽은 연결 감지 늦음
	DefaultReadTimeout = 30 * time.Second

	// DefaultWriteTimeout은 쓰기 작업의 기본 타임아웃입니다.
	//
	// [왜 10초인가?]
	// - 쓰기는 커널 버퍼로 복사하는 것이므로 보통 빠름
	// - 버퍼가 가득 찬 경우(상대방이 안 읽음)에만 블로킹
	// - 10초 동안 버퍼 공간이 안 생기면 연결 문제
	DefaultWriteTimeout = 10 * time.Second
)

// Connection은 TCP 연결을 감싸는 구조체입니다.
//
// [왜 net.Conn을 직접 안 쓰고 감싸나?]
// 1. 버퍼링된 I/O 추가 (bufio.Reader/Writer)
// 2. 프레이밍(길이 prefix) 처리
// 3. 통계 수집 (bytes sent/received)
// 4. 연결별 설정 관리
//
// [메모리 레이아웃]
// Connection 구조체는 힙에 할당되며, 내부 포인터들도 각각 힙 객체를 가리킴
// GC가 Connection을 추적하고, Close() 시 관련 리소스 정리
type Connection struct {
	// conn은 underlying TCP 연결입니다.
	//
	// [net.Conn 인터페이스]
	// - Read([]byte) (int, error)
	// - Write([]byte) (int, error)
	// - Close() error
	// - LocalAddr() net.Addr
	// - RemoteAddr() net.Addr
	// - SetDeadline(time.Time) error
	// - SetReadDeadline(time.Time) error
	// - SetWriteDeadline(time.Time) error
	conn net.Conn

	// reader는 버퍼링된 읽기를 제공합니다.
	//
	// [왜 bufio.Reader를 쓰나?]
	// - 시스템 콜 횟수 감소: read()를 매번 호출하는 대신 큰 덩어리로 읽어옴
	// - 작은 읽기 요청도 효율적: 4바이트 헤더 읽기도 버퍼에서 가져옴
	// - 내부 버퍼 기본 크기: 4096바이트
	//
	// [작동 방식]
	// 1. Read(4) 요청 → 버퍼가 비어있으면 conn에서 4096바이트 읽어옴
	// 2. 버퍼에서 4바이트 반환
	// 3. 다음 Read(100) → 버퍼에서 바로 반환 (시스템 콜 없음)
	reader *bufio.Reader

	// writer는 버퍼링된 쓰기를 제공합니다.
	//
	// [왜 bufio.Writer를 쓰나?]
	// - 작은 쓰기를 모아서 한 번에 전송 → 시스템 콜 감소
	// - TCP_NODELAY와 조합하면 좋음
	//
	// [주의] Flush() 호출 필요
	// - Write()는 버퍼에만 쓰고 반환할 수 있음
	// - 실제 전송은 Flush()나 버퍼가 가득 찼을 때
	writer *bufio.Writer

	// writeMu는 쓰기 작업의 동시성을 제어합니다.
	//
	// [왜 쓰기에만 Mutex가 있나?]
	// - 읽기는 보통 단일 고루틴에서만 수행 (readLoop)
	// - 쓰기는 여러 고루틴에서 동시에 할 수 있음 (브로드캐스트 등)
	// - TCP는 바이트 스트림이므로 쓰기가 섞이면 메시지가 깨짐
	//
	// [예시 - Mutex 없이 동시 쓰기]
	// 고루틴A: Write("HELLO")  → 실제 전송: "HEL"
	// 고루틴B: Write("WORLD")  → 실제 전송: "WORLD" (중간에 끼어듦)
	// 고루틴A: (계속)          → 실제 전송: "LO"
	// 결과: "HELWORLDLO" - 메시지 깨짐!
	writeMu sync.Mutex

	// closed는 연결 종료 여부를 나타냅니다.
	closed bool

	// closeMu는 closed 필드 접근을 보호합니다.
	closeMu sync.RWMutex

	// readTimeout은 읽기 타임아웃입니다.
	readTimeout time.Duration

	// writeTimeout은 쓰기 타임아웃입니다.
	writeTimeout time.Duration
}

// NewConnection은 새로운 Connection을 생성합니다.
//
// [초기화되는 것들]
// 1. bufio.Reader/Writer 생성 (버퍼 할당)
// 2. 기본 타임아웃 설정
// 3. TCP 옵션 설정 (keepalive, nodelay)
func NewConnection(conn net.Conn) *Connection {
	// TCP 연결인 경우 추가 옵션 설정
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		// TCP_NODELAY 설정
		//
		// [Nagle 알고리즘이란?]
		// - 작은 패킷을 모아서 전송하여 네트워크 효율 향상
		// - 예: 1바이트씩 100번 → 100바이트 1번
		// - 문제: 지연(latency) 발생, 실시간 애플리케이션에 불리
		//
		// [P2P에서 왜 끄나?]
		// - 메시지 지연 최소화가 중요
		// - 이미 우리가 메시지 단위로 버퍼링함
		// - 작은 메시지라도 바로 전송해야 응답성이 좋음
		tcpConn.SetNoDelay(true)

		// TCP Keepalive 설정
		//
		// [Keepalive란?]
		// - 연결이 살아있는지 주기적으로 확인하는 TCP 기능
		// - 커널이 자동으로 프로브 패킷 전송
		// - 응답 없으면 연결 끊김으로 처리
		//
		// [왜 필요한가?]
		// - NAT 테이블 타임아웃 방지 (보통 5~15분)
		// - 갑자기 끊긴 연결 감지 (상대방 크래시, 네트워크 단절)
		// - 애플리케이션 레벨 Ping/Pong과 별개로 동작
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	return &Connection{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),

		readTimeout:  DefaultReadTimeout,
		writeTimeout: DefaultWriteTimeout,
	}
}

// Read는 지정된 바이트 수만큼 정확히 읽습니다.
//
// [io.ReadFull vs conn.Read 차이]
// - conn.Read(buf): 1바이트라도 읽으면 반환할 수 있음
// - io.ReadFull(conn, buf): buf가 가득 찰 때까지 읽음
//
// [왜 ReadFull을 쓰나?]
// TCP는 스트림이라 메시지 경계가 없음
// - Write("HELLO")를 해도
// - Read()에서 "HEL", "LO" 두 번에 나눠서 올 수 있음
// - 따라서 원하는 바이트 수를 정확히 읽어야 함
//
// [OS 레벨에서 일어나는 일]
// 1. 커널 수신 버퍼에 데이터가 있으면 복사
// 2. 없으면 고루틴 파킹 (epoll_wait에서 대기)
// 3. 데이터 도착하면 고루틴 깨움
// 4. 요청한 바이트 수가 될 때까지 반복
func (c *Connection) Read(buf []byte) (int, error) {
	c.closeMu.RLock()
	if c.closed {
		c.closeMu.RUnlock()
		return 0, fmt.Errorf("연결이 닫혔습니다")
	}
	c.closeMu.RUnlock()

	// 타임아웃 설정
	// SetReadDeadline: 이 시간까지 읽기가 완료되지 않으면 에러
	// time.Time{} (zero value)를 설정하면 타임아웃 해제
	if c.readTimeout > 0 {
		c.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	}

	// io.ReadFull은 buf가 가득 찰 때까지 읽음
	// 중간에 EOF가 오면 io.ErrUnexpectedEOF 반환
	return io.ReadFull(c.reader, buf)
}

// ReadMessage는 길이 prefix가 있는 메시지를 읽습니다.
//
// [메시지 프레이밍]
// TCP는 바이트 스트림이므로 메시지 경계를 직접 처리해야 함
// 방법 1: 구분자 (delimiter) - "\n"으로 메시지 구분
// 방법 2: 길이 prefix - 먼저 길이를 읽고, 그 길이만큼 읽음 ← 우리가 사용
//
// [프로토콜 형식]
// [4바이트 길이][페이로드...]
// - 길이: Big-endian uint32 (최대 4GB, 실제로는 제한 필요)
// - 페이로드: 실제 메시지 데이터
//
// [왜 Big-endian인가?]
// - 네트워크 바이트 순서가 Big-endian (RFC 표준)
// - x86은 Little-endian이지만 네트워크에서는 Big-endian 사용
// - binary.BigEndian으로 변환
func (c *Connection) ReadMessage() ([]byte, error) {
	// 1. 길이 읽기 (4바이트)
	lengthBuf := make([]byte, 4)
	if _, err := c.Read(lengthBuf); err != nil {
		return nil, fmt.Errorf("메시지 길이 읽기 실패: %w", err)
	}

	// Big-endian으로 해석
	// [0x00, 0x00, 0x00, 0x0A] → 10
	length := binary.BigEndian.Uint32(lengthBuf)

	// 2. 길이 검증 (DoS 방지)
	//
	// [왜 제한이 필요한가?]
	// 악의적인 피어가 length = 4GB로 보내면
	// 4GB 버퍼를 할당하려다 메모리 부족으로 크래시
	const maxMessageSize = 10 * 1024 * 1024 // 10MB
	if length > maxMessageSize {
		return nil, fmt.Errorf("메시지가 너무 큼: %d > %d", length, maxMessageSize)
	}
	if length == 0 {
		return nil, fmt.Errorf("메시지 길이가 0입니다")
	}

	// 3. 페이로드 읽기
	payload := make([]byte, length)
	if _, err := c.Read(payload); err != nil {
		return nil, fmt.Errorf("메시지 페이로드 읽기 실패: %w", err)
	}

	return payload, nil
}

// Write는 데이터를 연결에 씁니다.
//
// [Mutex가 필요한 이유]
// 여러 고루틴이 동시에 Write하면 데이터가 섞일 수 있음
// bufio.Writer도 thread-safe하지 않음
//
// [OS 레벨에서 일어나는 일]
// 1. bufio.Writer 버퍼에 데이터 복사
// 2. Flush() 시 커널 송신 버퍼로 복사 (write 시스템 콜)
// 3. 커널이 백그라운드로 TCP 세그먼트 전송
// 4. ACK 받으면 송신 버퍼에서 제거
//
// [중요] Write() 성공 ≠ 상대방 수신 완료
// Write()는 커널 버퍼에 들어가면 성공 반환
// 실제 전송과 ACK는 비동기로 처리됨
func (c *Connection) Write(data []byte) (int, error) {
	c.closeMu.RLock()
	if c.closed {
		c.closeMu.RUnlock()
		return 0, fmt.Errorf("연결이 닫혔습니다")
	}
	c.closeMu.RUnlock()

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	// 타임아웃 설정
	if c.writeTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}

	n, err := c.writer.Write(data)
	if err != nil {
		return n, fmt.Errorf("쓰기 실패: %w", err)
	}

	// Flush()를 호출해야 실제로 전송됨
	// 버퍼에만 있으면 네트워크로 안 나감
	if err := c.writer.Flush(); err != nil {
		return n, fmt.Errorf("플러시 실패: %w", err)
	}

	return n, nil
}

// WriteMessage는 길이 prefix를 붙여서 메시지를 전송합니다.
//
// [전송 형식]
// [4바이트 길이][페이로드]
//
// [왜 한 번의 Write로 보내나?]
// - 두 번 Write하면 Nagle 알고리즘 때문에 지연될 수 있음
// - 원자적으로 메시지 전체를 보내야 함 (중간에 다른 메시지 끼어들면 안 됨)
func (c *Connection) WriteMessage(data []byte) error {
	c.closeMu.RLock()
	if c.closed {
		c.closeMu.RUnlock()
		return fmt.Errorf("연결이 닫혔습니다")
	}
	c.closeMu.RUnlock()

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	// 타임아웃 설정
	if c.writeTimeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	}

	// 1. 길이 쓰기 (4바이트, Big-endian)
	lengthBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBuf, uint32(len(data)))

	if _, err := c.writer.Write(lengthBuf); err != nil {
		return fmt.Errorf("길이 쓰기 실패: %w", err)
	}

	// 2. 페이로드 쓰기
	if _, err := c.writer.Write(data); err != nil {
		return fmt.Errorf("페이로드 쓰기 실패: %w", err)
	}

	// 3. 플러시 - 실제로 네트워크로 전송
	if err := c.writer.Flush(); err != nil {
		return fmt.Errorf("플러시 실패: %w", err)
	}

	return nil
}

// Close는 연결을 닫습니다.
//
// [OS 레벨에서 일어나는 일]
// 1. shutdown() 또는 close() 시스템 콜
// 2. FIN 패킷 전송 (TCP 4-way handshake 시작)
//    - 우리: FIN →
//    - 상대: ← ACK
//    - 상대: ← FIN
//    - 우리: ACK →
// 3. fd 해제, 커널 자원 정리
//
// [TIME_WAIT 상태]
// - Close() 후에도 소켓이 바로 사라지지 않음
// - TIME_WAIT 상태로 약 2분간 유지 (MSL * 2)
// - 지연된 패킷 처리 및 연결 재사용 방지 위함
// - 서버 재시작 시 "address already in use" 원인이 되기도 함
func (c *Connection) Close() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()

	if c.closed {
		return nil // 이미 닫힘
	}
	c.closed = true

	// 남은 버퍼 플러시 시도 (무시해도 됨)
	c.writer.Flush()

	return c.conn.Close()
}

// LocalAddr은 로컬 주소를 반환합니다.
func (c *Connection) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// RemoteAddr은 원격 주소를 반환합니다.
//
// [주소 형식]
// - IPv4: "192.168.1.100:3000"
// - IPv6: "[::1]:3000"
func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// SetReadTimeout은 읽기 타임아웃을 설정합니다.
func (c *Connection) SetReadTimeout(d time.Duration) {
	c.readTimeout = d
}

// SetWriteTimeout은 쓰기 타임아웃을 설정합니다.
func (c *Connection) SetWriteTimeout(d time.Duration) {
	c.writeTimeout = d
}

// IsClosed는 연결이 닫혔는지 확인합니다.
func (c *Connection) IsClosed() bool {
	c.closeMu.RLock()
	defer c.closeMu.RUnlock()
	return c.closed
}
