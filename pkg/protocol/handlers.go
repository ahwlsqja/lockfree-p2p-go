package protocol

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// =============================================================================
// Ping/Pong - 연결 유지 프로토콜
// =============================================================================

// PingPayload는 Ping 메시지의 페이로드입니다.
//
// [Ping/Pong 프로토콜]
// - 주기적으로 Ping 전송 (보통 15~30초)
// - 상대방은 같은 Nonce로 Pong 응답
// - 응답 시간으로 네트워크 지연(latency) 측정 가능
// - 응답 없으면 연결 끊김으로 판단
// 근데 이걸 내가 처음에 나는 핑 페이로드가 항상 일관성이 있을 것 이라고 생각해서 포인터 메서드를 쓰는줄 아니였는데 코드 일관성 때문에 쓰는거라고 한다. Nonce값이 항상 달라져야하니떄문에 관련없다고 한ㄷ.
// Nonce가 같으면 어떤 Ping에 대한 응답인지 구분을 못 한다. 그래서 재사용하면 안 된다.
type PingPayload struct {
	// Nonce는 Ping/Pong 매칭을 위한 랜덤 값입니다.
	// Pong에서 같은 Nonce를 반환해야 유효한 응답
	Nonce uint64
}

// Encode는 PingPayload를 바이트 슬라이스로 인코딩합니다.
func (p *PingPayload) Encode() []byte {
	// 8바이트 크기 버퍼만듬 [0, 0, 0, 0, 0, 0, 0, 0]
	buf := make([]byte, 8)
	// uint64 숫자를 바이트 슬라이스에 저장하는 패키지 라고한다.
	// var nonce uint64 = 12345678901234567890
	// 이 숫자를 8바이트로 변환해서 buf에 저장
	// 결과: buf = [0xAB, 0x54, 0xA9, 0x8C, 0xEB, 0x1F, 0x0A, 0xD2]
	// BigEndian은 숫자를 바이트로 저장하는 "순서"라고 하고 
	// 숫자: 0x12345678이라면 BigEndian(빅엔디안): [0x12, 0x34, 0x56, 0x78] 큰 자릿수가 먼저온다
	// LittleEndain(리틀엔디안): [0x78, 0x56, 0x34, 0x12] 작은 자릿수가 먼저온다
	// 근데 왜 BigEndian을 쓸까. 
	// 네트워크 표준이 BigEndiand 이라고 한다
	// 다른 컴퓨터와 통신할 때 바이트 순서가 같아야한다고 한다
	// 숫자르 바이트로 저장!!!
	binary.BigEndian.PutUint64(buf, p.Nonce)
	return buf
}

// DecodePingPayload는 바이트 슬라이스에서 PingPayload를 디코딩합니다.
// 
func DecodePingPayload(data []byte) (*PingPayload, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("Ping 데이터 부족: %d < 8", len(data))
	}
	return &PingPayload{
		// 바이트를 숫자로 읽기!
		Nonce: binary.BigEndian.Uint64(data),
	}, nil
}

// NewPingMessage는 Ping 메시지를 생성합니다.
func NewPingMessage(nonce uint64) *Message {
	payload := &PingPayload{Nonce: nonce}
	// 숫자를 바이트로 인코드
	data := payload.Encode()
	return &Message{
		Type:     MsgPing,
		Payload:  data,
		// 데이터가 손상됐는지 확인하는 "지문"을 만든 것이라고 한다.
		// IEEE가 CRC32 계산에 쓰는 공식(다양식) 종류라고한다...
		Checksum: crc32.ChecksumIEEE(data),
	}
}

// PongPayload는 Pong 메시지의 페이로드입니다.
type PongPayload struct {
	Nonce uint64
}

// Encode는 PongPayload를 바이트 슬라이스로 인코딩합니다.
func (p *PongPayload) Encode() []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, p.Nonce)
	return buf
}

// DecodePongPayload는 바이트 슬라이스에서 PongPayload를 디코딩합니다.
func DecodePongPayload(data []byte) (*PongPayload, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("Pong 데이터 부족: %d < 8", len(data))
	}
	return &PongPayload{
		Nonce: binary.BigEndian.Uint64(data),
	}, nil
}

// NewPongMessage는 Pong 메시지를 생성합니다.
func NewPongMessage(nonce uint64) *Message {
	payload := &PongPayload{Nonce: nonce}
	data := payload.Encode()
	return &Message{
		Type:     MsgPong,
		Payload:  data,
		Checksum: crc32.ChecksumIEEE(data),
	}
}

// =============================================================================
// Disconnect - 연결 종료 메시지
// =============================================================================

// DisconnectPayload는 Disconnect 메시지의 페이로드입니다.
type DisconnectPayload struct {
	// Reason은 종료 이유 코드입니다.
	Reason DisconnectReason

	// Message는 추가 설명입니다 (선택적).
	Message string
}

// Encode는 DisconnectPayload를 바이트 슬라이스로 인코딩합니다.
func (d *DisconnectPayload) Encode() []byte {
	msgBytes := []byte(d.Message)
	buf := make([]byte, 1+2+len(msgBytes))

	// Reason
	buf[0] = byte(d.Reason)

	// Message (길이 prefix)
	binary.BigEndian.PutUint16(buf[1:], uint16(len(msgBytes)))
	copy(buf[3:], msgBytes)

	return buf
}

// DecodeDisconnectPayload는 바이트 슬라이스에서 DisconnectPayload를 디코딩합니다.
func DecodeDisconnectPayload(data []byte) (*DisconnectPayload, error) {
	if len(data) < 3 {
		return nil, fmt.Errorf("Disconnect 데이터 부족: %d < 3", len(data))
	}

	d := &DisconnectPayload{
		Reason: DisconnectReason(data[0]),
	}

	msgLen := binary.BigEndian.Uint16(data[1:3])
	if len(data) < 3+int(msgLen) {
		return nil, fmt.Errorf("Disconnect 메시지 데이터 부족")
	}
	d.Message = string(data[3 : 3+msgLen])

	return d, nil
}

// NewDisconnectMessage는 Disconnect 메시지를 생성합니다.
func NewDisconnectMessage(reason DisconnectReason, message string) *Message {
	payload := &DisconnectPayload{Reason: reason, Message: message}
	data := payload.Encode()
	return &Message{
		Type:     MsgDisconnect,
		Payload:  data,
		Checksum: crc32.ChecksumIEEE(data),
	}
}

// =============================================================================
// GetPeers / Peers - 피어 디스커버리
// =============================================================================

// GetPeersPayload는 GetPeers 메시지의 페이로드입니다.
//
// [피어 디스커버리 프로토콜]
// 1. 새 노드가 시드 노드에 연결
// 2. GetPeers 메시지 전송
// 3. Peers 메시지로 알려진 피어 목록 수신
// 4. 받은 피어들에게 연결 시도
type GetPeersPayload struct {
	// MaxPeers는 요청하는 최대 피어 수입니다.
	// 0이면 기본값 사용
	MaxPeers uint16
}

// Encode는 GetPeersPayload를 바이트 슬라이스로 인코딩합니다.
func (g *GetPeersPayload) Encode() []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, g.MaxPeers)
	return buf
}

// DecodeGetPeersPayload는 바이트 슬라이스에서 GetPeersPayload를 디코딩합니다.
func DecodeGetPeersPayload(data []byte) (*GetPeersPayload, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("GetPeers 데이터 부족: %d < 2", len(data))
	}
	return &GetPeersPayload{
		MaxPeers: binary.BigEndian.Uint16(data),
	}, nil
}

// NewGetPeersMessage는 GetPeers 메시지를 생성합니다.
func NewGetPeersMessage(maxPeers uint16) *Message {
	payload := &GetPeersPayload{MaxPeers: maxPeers}
	data := payload.Encode()
	return &Message{
		Type:     MsgGetPeers,
		Payload:  data,
		Checksum: crc32.ChecksumIEEE(data),
	}
}

// PeerInfo는 피어 정보입니다.
type PeerInfo struct {
	// NodeID는 노드 식별자입니다.
	NodeID [16]byte

	// IP는 IP 주소입니다 (IPv4 또는 IPv6).
	// IPv4: 4바이트, IPv6: 16바이트
	IP []byte

	// Port는 리스닝 포트입니다.
	Port uint16
}

// Encode는 PeerInfo를 바이트 슬라이스로 인코딩합니다.
func (p *PeerInfo) Encode() []byte {
	// NodeID(16) + IPLen(1) + IP(4 or 16) + Port(2)
	buf := make([]byte, 16+1+len(p.IP)+2)
	offset := 0

	// NodeID
	copy(buf[offset:], p.NodeID[:])
	offset += 16

	// IP (길이 prefix)
	buf[offset] = byte(len(p.IP))
	offset++
	copy(buf[offset:], p.IP)
	offset += len(p.IP)

	// Port
	binary.BigEndian.PutUint16(buf[offset:], p.Port)

	return buf
}

// DecodePeerInfo는 바이트 슬라이스에서 PeerInfo를 디코딩합니다.
func DecodePeerInfo(data []byte) (*PeerInfo, int, error) {
	// 최소 크기: NodeID(16) + IPLen(1) + Port(2) = 19
	if len(data) < 19 {
		return nil, 0, fmt.Errorf("PeerInfo 데이터 부족: %d < 19", len(data))
	}

	p := &PeerInfo{}
	offset := 0

	// NodeID
	copy(p.NodeID[:], data[offset:offset+16])
	offset += 16

	// IP
	ipLen := int(data[offset])
	offset++
	if ipLen != 4 && ipLen != 16 {
		return nil, 0, fmt.Errorf("잘못된 IP 길이: %d", ipLen)
	}
	if len(data) < offset+ipLen+2 {
		return nil, 0, fmt.Errorf("IP 데이터 부족")
	}
	p.IP = make([]byte, ipLen)
	copy(p.IP, data[offset:offset+ipLen])
	offset += ipLen

	// Port
	p.Port = binary.BigEndian.Uint16(data[offset:])
	offset += 2

	return p, offset, nil
}

// PeersPayload는 Peers 메시지의 페이로드입니다.
type PeersPayload struct {
	Peers []PeerInfo
}

// Encode는 PeersPayload를 바이트 슬라이스로 인코딩합니다.
func (p *PeersPayload) Encode() []byte {
	// 피어 수 (2바이트) + 각 피어 정보
	var totalSize int = 2
	encodedPeers := make([][]byte, len(p.Peers))

	for i, peer := range p.Peers {
		encoded := peer.Encode()
		encodedPeers[i] = encoded
		totalSize += len(encoded)
	}

	buf := make([]byte, totalSize)
	offset := 0

	// 피어 수
	binary.BigEndian.PutUint16(buf[offset:], uint16(len(p.Peers)))
	offset += 2

	// 각 피어 정보
	for _, encoded := range encodedPeers {
		copy(buf[offset:], encoded)
		offset += len(encoded)
	}

	return buf
}

// DecodePeersPayload는 바이트 슬라이스에서 PeersPayload를 디코딩합니다.
func DecodePeersPayload(data []byte) (*PeersPayload, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("Peers 데이터 부족: %d < 2", len(data))
	}

	peerCount := binary.BigEndian.Uint16(data[0:2])
	offset := 2

	peers := make([]PeerInfo, 0, peerCount)
	for i := uint16(0); i < peerCount; i++ {
		if offset >= len(data) {
			return nil, fmt.Errorf("피어 %d 데이터 부족", i)
		}

		peer, consumed, err := DecodePeerInfo(data[offset:])
		if err != nil {
			return nil, fmt.Errorf("피어 %d 디코딩 실패: %w", i, err)
		}

		peers = append(peers, *peer)
		offset += consumed
	}

	return &PeersPayload{Peers: peers}, nil
}

// NewPeersMessage는 Peers 메시지를 생성합니다.
func NewPeersMessage(peers []PeerInfo) *Message {
	payload := &PeersPayload{Peers: peers}
	data := payload.Encode()
	return &Message{
		Type:     MsgPeers,
		Payload:  data,
		Checksum: crc32.ChecksumIEEE(data),
	}
}

// =============================================================================
// Inv / GetData - 데이터 교환 프로토콜
// =============================================================================

// InvItem은 인벤토리 항목입니다.
type InvItem struct {
	// Type은 항목 타입입니다 (Tx, Block 등).
	Type InvType

	// Hash는 항목의 해시입니다 (32바이트).
	Hash [32]byte
}

// Encode는 InvItem을 바이트 슬라이스로 인코딩합니다.
func (i *InvItem) Encode() []byte {
	buf := make([]byte, 33) // Type(1) + Hash(32)
	buf[0] = byte(i.Type)
	copy(buf[1:], i.Hash[:])
	return buf
}

// DecodeInvItem은 바이트 슬라이스에서 InvItem을 디코딩합니다.
func DecodeInvItem(data []byte) (*InvItem, error) {
	if len(data) < 33 {
		return nil, fmt.Errorf("InvItem 데이터 부족: %d < 33", len(data))
	}

	item := &InvItem{
		Type: InvType(data[0]),
	}
	copy(item.Hash[:], data[1:33])

	return item, nil
}

// InvPayload는 Inv 메시지의 페이로드입니다.
//
// [Inv 프로토콜]
// - 새 트랜잭션/블록이 있으면 Inv로 해시만 알림
// - 상대방이 해당 데이터가 없으면 GetData로 요청
// - 이미 있으면 무시 → 대역폭 절약
type InvPayload struct {
	Items []InvItem
}

// Encode는 InvPayload를 바이트 슬라이스로 인코딩합니다.
func (p *InvPayload) Encode() []byte {
	// Count(2) + Items
	totalSize := 2 + len(p.Items)*33
	buf := make([]byte, totalSize)

	// Count
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(p.Items)))

	// Items
	offset := 2
	for _, item := range p.Items {
		copy(buf[offset:], item.Encode())
		offset += 33
	}

	return buf
}

// DecodeInvPayload는 바이트 슬라이스에서 InvPayload를 디코딩합니다.
func DecodeInvPayload(data []byte) (*InvPayload, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("Inv 데이터 부족: %d < 2", len(data))
	}

	count := binary.BigEndian.Uint16(data[0:2])
	expectedSize := 2 + int(count)*33

	if len(data) < expectedSize {
		return nil, fmt.Errorf("Inv 데이터 부족: %d < %d", len(data), expectedSize)
	}

	items := make([]InvItem, count)
	offset := 2

	for i := uint16(0); i < count; i++ {
		item, err := DecodeInvItem(data[offset:])
		if err != nil {
			return nil, fmt.Errorf("InvItem %d 디코딩 실패: %w", i, err)
		}
		items[i] = *item
		offset += 33
	}

	return &InvPayload{Items: items}, nil
}

// NewInvMessage는 Inv 메시지를 생성합니다.
func NewInvMessage(items []InvItem) *Message {
	payload := &InvPayload{Items: items}
	data := payload.Encode()
	return &Message{
		Type:     MsgInv,
		Payload:  data,
		Checksum: crc32.ChecksumIEEE(data),
	}
}

// GetDataPayload는 GetData 메시지의 페이로드입니다.
// Inv와 동일한 구조
type GetDataPayload = InvPayload

// DecodeGetDataPayload는 바이트 슬라이스에서 GetDataPayload를 디코딩합니다.
func DecodeGetDataPayload(data []byte) (*GetDataPayload, error) {
	return DecodeInvPayload(data)
}

// NewGetDataMessage는 GetData 메시지를 생성합니다.
func NewGetDataMessage(items []InvItem) *Message {
	payload := &GetDataPayload{Items: items}
	data := payload.Encode()
	return &Message{
		Type:     MsgGetData,
		Payload:  data,
		Checksum: crc32.ChecksumIEEE(data),
	}
}
