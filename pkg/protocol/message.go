// Package protocol은 P2P 네트워크의 메시지 프로토콜을 정의합니다.
// 모든 노드 간 통신은 이 프로토콜을 따릅니다.
package protocol

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// =============================================================================
// 프로토콜 상수
// =============================================================================

const (
	// MagicNumber는 프로토콜 식별을 위한 매직 넘버입니다.
	//
	// [왜 매직 넘버가 필요한가?]
	// - 잘못된 포트로 연결된 경우 빠른 감지
	// - 프로토콜 버전 불일치 감지
	// - "P2P1"을 16진수로 인코딩: 0x50325031
	//
	// [검증 방식]
	// 메시지의 첫 4바이트가 이 값이 아니면 즉시 연결 종료
	MagicNumber uint32 = 0x50325031 // "P2P1"

	// ProtocolVersion은 현재 프로토콜 버전입니다.
	//
	// [버전 관리]
	// - 메이저 버전 변경: 호환 불가능한 변경
	// - 마이너 버전 변경: 하위 호환 가능한 변경
	// - 핸드셰이크 시 버전 교환하여 호환성 확인
	ProtocolVersion uint32 = 1

	// HeaderSize는 메시지 헤더의 바이트 크기입니다.
	//
	// [헤더 구조] (13바이트)
	// - Magic: 4바이트 (프로토콜 식별)
	// - Type: 1바이트 (메시지 타입)
	// - Length: 4바이트 (페이로드 길이)
	// - Checksum: 4바이트 (CRC32 체크섬)
	HeaderSize = 13

	// MaxPayloadSize는 페이로드의 최대 크기입니다.
	//
	// [왜 10MB인가?]
	// - 블록 데이터가 가장 큰 메시지 (보통 1~2MB)
	// - 여유를 두어 10MB로 설정
	// - 이 이상은 악의적 요청으로 간주
	MaxPayloadSize = 10 * 1024 * 1024 // 10MB
)

// =============================================================================
// 메시지 타입
// =============================================================================

// MessageType은 메시지의 종류를 나타냅니다.
type MessageType uint8

const (
	// 연결 관리 메시지 (0x00 ~ 0x0F)

	// MsgHandshake는 연결 초기화 메시지입니다.
	//
	// [교환 정보]
	// - 프로토콜 버전
	// - 노드 ID
	// - 리스닝 주소
	// - User-Agent (클라이언트 식별)
	MsgHandshake MessageType = 0x00

	// MsgHandshakeAck는 핸드셰이크 응답입니다.
	MsgHandshakeAck MessageType = 0x01

	// MsgPing은 연결 유지 확인 메시지입니다.
	//
	// [Ping/Pong 프로토콜]
	// - 주기적으로 Ping 전송 (보통 15~30초)
	// - 응답 없으면 연결 끊김으로 판단
	// - 네트워크 지연(latency) 측정에도 사용
	MsgPing MessageType = 0x02

	// MsgPong은 Ping에 대한 응답입니다.
	MsgPong MessageType = 0x03

	// MsgDisconnect는 연결 종료 알림입니다.
	//
	// [왜 필요한가?]
	// - 정상 종료 vs 비정상 종료 구분
	// - 종료 이유 전달 (버전 불일치, 피어 수 초과 등)
	MsgDisconnect MessageType = 0x04

	// 피어 디스커버리 메시지 (0x10 ~ 0x1F)

	// MsgGetPeers는 피어 목록 요청입니다.
	//
	// [디스커버리 프로토콜]
	// 1. 새 노드가 시드 노드에 연결
	// 2. GetPeers로 다른 피어 목록 요청
	// 3. 받은 피어들에게 연결 시도
	// 4. 반복하여 네트워크 전체로 확산
	MsgGetPeers MessageType = 0x10

	// MsgPeers는 피어 목록 응답입니다.
	MsgPeers MessageType = 0x11

	// 데이터 전파 메시지 (0x20 ~ 0x2F)

	// MsgTx는 트랜잭션 전파 메시지입니다.
	//
	// [트랜잭션 전파 방식]
	// - 새 트랜잭션 수신 시 모든 피어에게 전파
	// - 이미 본 트랜잭션은 무시 (중복 전파 방지)
	// - Mempool에 저장하여 블록 생성 시 포함
	MsgTx MessageType = 0x20

	// MsgBlock은 블록 전파 메시지입니다.
	//
	// [블록 전파 방식]
	// - 새 블록 생성/수신 시 모든 피어에게 전파
	// - 검증 후 체인에 추가
	MsgBlock MessageType = 0x21

	// MsgGetBlocks는 블록 요청 메시지입니다.
	// 동기화 시 사용
	MsgGetBlocks MessageType = 0x22

	// MsgInv는 인벤토리(데이터 존재 알림) 메시지입니다.
	//
	// [Inv 프로토콜]
	// - 데이터 자체 대신 해시만 전송
	// - 상대방이 필요하면 GetData로 요청
	// - 대역폭 절약 (이미 있는 데이터 재전송 방지)
	MsgInv MessageType = 0x23

	// MsgGetData는 데이터 요청 메시지입니다.
	MsgGetData MessageType = 0x24
)

// String은 메시지 타입을 문자열로 반환합니다.
func (t MessageType) String() string {
	switch t {
	case MsgHandshake:
		return "Handshake"
	case MsgHandshakeAck:
		return "HandshakeAck"
	case MsgPing:
		return "Ping"
	case MsgPong:
		return "Pong"
	case MsgDisconnect:
		return "Disconnect"
	case MsgGetPeers:
		return "GetPeers"
	case MsgPeers:
		return "Peers"
	case MsgTx:
		return "Tx"
	case MsgBlock:
		return "Block"
	case MsgGetBlocks:
		return "GetBlocks"
	case MsgInv:
		return "Inv"
	case MsgGetData:
		return "GetData"
	default:
		return fmt.Sprintf("Unknown(0x%02x)", uint8(t))
	}
}

// =============================================================================
// Message - 메시지 구조체
// =============================================================================

// Message는 P2P 프로토콜 메시지입니다.
//
// [와이어 포맷]
// ┌──────────────────────────────────────┐
// │  Magic (4 bytes)  │  0x50325031      │
// ├───────────────────┼──────────────────┤
// │  Type (1 byte)    │  메시지 타입      │
// ├───────────────────┼──────────────────┤
// │  Length (4 bytes) │  페이로드 길이    │
// ├───────────────────┼──────────────────┤
// │  Checksum (4 B)   │  CRC32           │
// ├───────────────────┴──────────────────┤
// │  Payload (variable)                  │
// └──────────────────────────────────────┘
//
// [왜 이 순서인가?]
// 1. Magic을 먼저 읽어 잘못된 프로토콜 빠르게 거부
// 2. Type으로 어떤 종류의 메시지인지 파악
// 3. Length로 얼마나 더 읽어야 하는지 파악
// 4. Checksum으로 무결성 검증 (페이로드 읽기 전에 할 수 없음)
type Message struct {
	// Type은 메시지 종류입니다.
	Type MessageType

	// Payload는 메시지 본문입니다.
	// 메시지 타입에 따라 다른 구조체로 파싱됨
	Payload []byte

	// Checksum은 페이로드의 CRC32 체크섬입니다.
	// 전송 중 데이터 손상 감지용
	Checksum uint32
}

// NewMessage는 새 메시지를 생성합니다.
func NewMessage(msgType MessageType, payload []byte) *Message {
	return &Message{
		Type:     msgType,
		Payload:  payload,
		Checksum: crc32.ChecksumIEEE(payload),
	}
}

// Encode는 메시지를 바이트 슬라이스로 인코딩합니다.
//
// [인코딩 과정]
// 1. 헤더 13바이트 + 페이로드 크기의 버퍼 할당
// 2. Big-endian으로 각 필드 기록
// 3. 페이로드 복사
//
// [왜 Big-endian인가?]
// - 네트워크 바이트 순서 (RFC 표준)
// - 다양한 아키텍처 간 호환성 보장
// - x86(Little-endian)과 다르지만, 명시적 변환으로 해결
func (m *Message) Encode() []byte {
	// 버퍼 할당
	buf := make([]byte, HeaderSize+len(m.Payload))

	// Magic (4바이트, Big-endian)
	binary.BigEndian.PutUint32(buf[0:4], MagicNumber)

	// Type (1바이트)
	buf[4] = byte(m.Type)

	// Length (4바이트, Big-endian)
	binary.BigEndian.PutUint32(buf[5:9], uint32(len(m.Payload)))

	// Checksum (4바이트, Big-endian)
	binary.BigEndian.PutUint32(buf[9:13], m.Checksum)

	// Payload
	copy(buf[HeaderSize:], m.Payload)

	return buf
}

// Validate는 메시지의 유효성을 검사합니다.
//
// [검사 항목]
// 1. 페이로드 크기 제한
// 2. 체크섬 일치
func (m *Message) Validate() error {
	// 페이로드 크기 검사
	if len(m.Payload) > MaxPayloadSize {
		return fmt.Errorf("페이로드가 너무 큼: %d > %d", len(m.Payload), MaxPayloadSize)
	}

	// 체크섬 검증
	//
	// [CRC32란?]
	// - Cyclic Redundancy Check의 32비트 버전
	// - 데이터 무결성 검사용 (암호학적 보안은 아님)
	// - 빠른 계산 속도가 장점
	// - 우연한 손상은 감지하지만, 악의적 조작은 방지 못함
	expectedChecksum := crc32.ChecksumIEEE(m.Payload)
	if m.Checksum != expectedChecksum {
		return fmt.Errorf("체크섬 불일치: expected %x, got %x", expectedChecksum, m.Checksum)
	}

	return nil
}

// String은 메시지의 문자열 표현을 반환합니다.
func (m *Message) String() string {
	return fmt.Sprintf("Message{type=%s, payload=%d bytes, checksum=%x}",
		m.Type, len(m.Payload), m.Checksum)
}

// =============================================================================
// 연결 종료 이유
// =============================================================================

// DisconnectReason은 연결 종료 이유입니다.
type DisconnectReason uint8

const (
	// DisconnectRequested는 정상 종료 요청입니다.
	DisconnectRequested DisconnectReason = iota

	// DisconnectProtocolError는 프로토콜 오류입니다.
	DisconnectProtocolError

	// DisconnectVersionMismatch는 버전 불일치입니다.
	DisconnectVersionMismatch

	// DisconnectTooManyPeers는 피어 수 초과입니다.
	DisconnectTooManyPeers

	// DisconnectTimeout는 응답 타임아웃입니다.
	DisconnectTimeout

	// DisconnectBadBehavior는 악의적 행동 감지입니다.
	DisconnectBadBehavior
)

// String은 종료 이유를 문자열로 반환합니다.
func (r DisconnectReason) String() string {
	switch r {
	case DisconnectRequested:
		return "Requested"
	case DisconnectProtocolError:
		return "ProtocolError"
	case DisconnectVersionMismatch:
		return "VersionMismatch"
	case DisconnectTooManyPeers:
		return "TooManyPeers"
	case DisconnectTimeout:
		return "Timeout"
	case DisconnectBadBehavior:
		return "BadBehavior"
	default:
		return fmt.Sprintf("Unknown(%d)", r)
	}
}

// =============================================================================
// 인벤토리 타입
// =============================================================================

// InvType은 인벤토리 항목의 타입입니다.
type InvType uint8

const (
	// InvTypeTx는 트랜잭션입니다.
	InvTypeTx InvType = iota

	// InvTypeBlock은 블록입니다.
	InvTypeBlock
)

// String은 인벤토리 타입을 문자열로 반환합니다.
func (t InvType) String() string {
	switch t {
	case InvTypeTx:
		return "Tx"
	case InvTypeBlock:
		return "Block"
	default:
		return fmt.Sprintf("Unknown(%d)", t)
	}
}
