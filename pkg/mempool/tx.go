// Package mempool은 블록에 포함되기를 기다리는 트랜잭션들을 관리합니다.
//
// [Mempool이란?]
// "Memory Pool"의 줄임말로, 아직 블록에 포함되지 않은 트랜잭션들이
// 대기하는 임시 저장소입니다.
//
// [트랜잭션 생명주기]
// 1. 사용자가 트랜잭션 생성 및 서명
// 2. 노드에 제출 → Mempool에 추가
// 3. 네트워크로 전파 (가십)
// 4. 채굴자/검증자가 Mempool에서 트랜잭션 선택
// 5. 블록에 포함
// 6. Mempool에서 제거
//
// [Bitcoin vs Ethereum Mempool]
// - Bitcoin: 수수료(satoshi/byte) 기준 우선순위
// - Ethereum: Gas Price 기준 우선순위 + Nonce 순서
package mempool

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"time"
)

// =============================================================================
// 트랜잭션 구조체
// =============================================================================

// Tx는 트랜잭션을 나타냅니다.
//
// [실제 블록체인에서는]
// - Bitcoin: inputs, outputs, locktime 등
// - Ethereum: nonce, to, value, data, gasPrice, gasLimit 등
//
// [이 구현에서는]
// 학습 목적으로 단순화된 구조 사용
type Tx struct {
	// Hash는 트랜잭션의 고유 식별자입니다.
	//
	// [계산 방법]
	// SHA256(From || To || Value || Nonce || Data || Timestamp)
	//
	// [왜 해시를 ID로 쓰나?]
	// - 내용이 같으면 해시도 같음 (중복 검출)
	// - 내용이 조금만 달라도 해시가 완전히 다름 (무결성)
	// - 고정 길이 (32바이트)로 인덱싱 효율적
	Hash TxHash

	// From은 송신자 주소입니다.
	// 실제로는 공개키에서 파생되거나 서명에서 복원
	From Address

	// To는 수신자 주소입니다.
	To Address

	// Value는 전송 금액입니다.
	// 단위는 구현에 따라 다름 (wei, satoshi 등)
	Value uint64

	// Nonce는 송신자의 트랜잭션 순번입니다.
	//
	// [역할]
	// 1. 리플레이 공격 방지 (같은 트랜잭션 재사용 불가)
	// 2. 트랜잭션 순서 보장 (nonce 0, 1, 2... 순서대로 처리)
	//
	// [Ethereum에서의 nonce]
	// - 계정별로 0부터 시작
	// - 이전 nonce가 처리되어야 다음 것 처리
	// - nonce가 건너뛰면 그 트랜잭션은 대기
	Nonce uint64

	// GasPrice는 가스 단위당 지불할 금액입니다.
	//
	// [Mempool 우선순위 결정]
	// GasPrice가 높을수록 먼저 블록에 포함됨
	// 채굴자/검증자는 수수료 수입을 최대화하려고 함
	GasPrice uint64

	// GasLimit은 최대 사용 가스량입니다.
	//
	// [가스 모델]
	// - 각 연산에 가스 비용이 있음
	// - GasLimit까지만 실행, 초과 시 실패
	// - 실제 사용한 가스 * GasPrice = 수수료
	GasLimit uint64

	// Data는 트랜잭션 데이터입니다.
	// - 일반 전송: 비어있거나 메모
	// - 컨트랙트 호출: 함수 시그니처 + 인자
	// - 컨트랙트 배포: 바이트코드
	Data []byte

	// Timestamp는 트랜잭션 생성 시간입니다.
	Timestamp time.Time

	// ReceivedAt은 Mempool에 추가된 시간입니다.
	// Timestamp와 다를 수 있음 (네트워크 지연 등)
	ReceivedAt time.Time
}

// TxHash는 트랜잭션 해시입니다 (32바이트).
type TxHash [32]byte

// String은 해시를 16진수 문자열로 반환합니다.
func (h TxHash) String() string {
	return hex.EncodeToString(h[:])
}

// ShortString은 해시의 앞 8자만 반환합니다.
func (h TxHash) ShortString() string {
	return hex.EncodeToString(h[:4])
}

// IsZero는 해시가 0인지 확인합니다.
func (h TxHash) IsZero() bool {
	for _, b := range h {
		if b != 0 {
			return false
		}
	}
	return true
}

// Address는 계정 주소입니다 (20바이트).
//
// [실제 구현]
// - Bitcoin: RIPEMD160(SHA256(pubkey)) + checksum
// - Ethereum: Keccak256(pubkey)[12:32]
type Address [20]byte

// String은 주소를 16진수 문자열로 반환합니다.
func (a Address) String() string {
	return "0x" + hex.EncodeToString(a[:])
}

// ShortString은 주소의 앞 6자 + ... + 뒤 4자를 반환합니다.
func (a Address) ShortString() string {
	s := hex.EncodeToString(a[:])
	return "0x" + s[:6] + "..." + s[len(s)-4:]
}

// =============================================================================
// 트랜잭션 생성 및 유효성 검사
// =============================================================================

// NewTx는 새로운 트랜잭션을 생성합니다.
func NewTx(from, to Address, value, nonce, gasPrice, gasLimit uint64, data []byte) *Tx {
	tx := &Tx{
		From:      from,
		To:        to,
		Value:     value,
		Nonce:     nonce,
		GasPrice:  gasPrice,
		GasLimit:  gasLimit,
		Data:      data,
		Timestamp: time.Now(),
	}

	// 해시 계산
	tx.Hash = tx.ComputeHash()

	return tx
}

// ComputeHash는 트랜잭션 해시를 계산합니다.
//
// [해시에 포함되는 필드]
// - From, To, Value, Nonce, GasPrice, GasLimit, Data, Timestamp
//
// [해시에 포함되지 않는 필드]
// - ReceivedAt (Mempool 메타데이터)
// - Hash 자체 (순환 참조 방지)
func (tx *Tx) ComputeHash() TxHash {
	hasher := sha256.New()

	// From
	hasher.Write(tx.From[:])

	// To
	hasher.Write(tx.To[:])

	// Value (8바이트 빅엔디안)
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, tx.Value)
	hasher.Write(buf)

	// Nonce
	binary.BigEndian.PutUint64(buf, tx.Nonce)
	hasher.Write(buf)

	// GasPrice
	binary.BigEndian.PutUint64(buf, tx.GasPrice)
	hasher.Write(buf)

	// GasLimit
	binary.BigEndian.PutUint64(buf, tx.GasLimit)
	hasher.Write(buf)

	// Data
	hasher.Write(tx.Data)

	// Timestamp (UnixNano)
	binary.BigEndian.PutUint64(buf, uint64(tx.Timestamp.UnixNano()))
	hasher.Write(buf)

	var hash TxHash
	copy(hash[:], hasher.Sum(nil))
	return hash
}

// Fee는 최대 수수료를 반환합니다.
//
// [계산]
// Fee = GasPrice * GasLimit
//
// [주의]
// 실제 수수료는 GasPrice * GasUsed (사용한 가스)
// 여기서는 최대치만 계산
func (tx *Tx) Fee() uint64 {
	return tx.GasPrice * tx.GasLimit
}

// Size는 트랜잭션의 대략적인 크기(바이트)를 반환합니다.
//
// [크기 계산]
// - 고정 필드: 20 + 20 + 8 + 8 + 8 + 8 = 72바이트
// - Data: 가변
// - 오버헤드 (해시, 시간 등): 약 50바이트
//
// [Mempool에서의 용도]
// - 메모리 사용량 추정
// - 블록 크기 제한 확인
func (tx *Tx) Size() int {
	return 72 + len(tx.Data) + 50
}

// ValidationError는 트랜잭션 유효성 검사 에러입니다.
type ValidationError struct {
	Field   string
	Message string
}

func (e ValidationError) Error() string {
	return e.Field + ": " + e.Message
}

// Validate는 트랜잭션의 기본 유효성을 검사합니다.
//
// [검사 항목]
// 1. 해시 일치
// 2. GasLimit > 0
// 3. GasPrice > 0 (또는 설정에 따라)
// 4. From != zero address
//
// [검사하지 않는 항목]
// - 잔액 확인 (상태 DB 필요)
// - Nonce 확인 (상태 DB 필요)
// - 서명 검증 (서명 필드 필요)
func (tx *Tx) Validate() error {
	// 해시 확인
	if tx.Hash != tx.ComputeHash() {
		return ValidationError{Field: "Hash", Message: "해시 불일치"}
	}

	// GasLimit 확인
	if tx.GasLimit == 0 {
		return ValidationError{Field: "GasLimit", Message: "가스 리밋은 0보다 커야 함"}
	}

	// From 주소 확인
	var zeroAddr Address
	if tx.From == zeroAddr {
		return ValidationError{Field: "From", Message: "송신자 주소가 비어있음"}
	}

	return nil
}

// =============================================================================
// 우선순위 계산
// =============================================================================

// Priority는 트랜잭션의 Mempool 우선순위를 계산합니다.
//
// [우선순위 결정 요소]
// 1. GasPrice (높을수록 우선)
// 2. 대기 시간 (오래될수록 약간 우선)
//
// [반환값]
// 높을수록 높은 우선순위
func (tx *Tx) Priority() uint64 {
	// 기본: GasPrice
	priority := tx.GasPrice

	// 대기 시간 보너스 (초당 1씩 추가)
	// 너무 오래 대기한 트랜잭션도 언젠가는 처리되도록
	waitTime := time.Since(tx.ReceivedAt).Seconds()
	if waitTime > 0 {
		priority += uint64(waitTime)
	}

	return priority
}

// =============================================================================
// 직렬화/역직렬화 (네트워크 전송용)
// =============================================================================

// Encode는 트랜잭션을 바이트로 인코딩합니다.
//
// [포맷]
// | From(20) | To(20) | Value(8) | Nonce(8) | GasPrice(8) | GasLimit(8) |
// | DataLen(4) | Data(variable) | Timestamp(8) |
func (tx *Tx) Encode() []byte {
	// 크기 계산
	size := 20 + 20 + 8 + 8 + 8 + 8 + 4 + len(tx.Data) + 8

	buf := make([]byte, size)
	offset := 0

	// From
	copy(buf[offset:], tx.From[:])
	offset += 20

	// To
	copy(buf[offset:], tx.To[:])
	offset += 20

	// Value
	binary.BigEndian.PutUint64(buf[offset:], tx.Value)
	offset += 8

	// Nonce
	binary.BigEndian.PutUint64(buf[offset:], tx.Nonce)
	offset += 8

	// GasPrice
	binary.BigEndian.PutUint64(buf[offset:], tx.GasPrice)
	offset += 8

	// GasLimit
	binary.BigEndian.PutUint64(buf[offset:], tx.GasLimit)
	offset += 8

	// DataLen
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(tx.Data)))
	offset += 4

	// Data
	copy(buf[offset:], tx.Data)
	offset += len(tx.Data)

	// Timestamp
	binary.BigEndian.PutUint64(buf[offset:], uint64(tx.Timestamp.UnixNano()))

	return buf
}

// DecodeTx는 바이트에서 트랜잭션을 디코딩합니다.
func DecodeTx(data []byte) (*Tx, error) {
	if len(data) < 84 { // 최소 크기: 20+20+8+8+8+8+4+8
		return nil, ValidationError{Field: "Data", Message: "데이터 크기가 너무 작음"}
	}

	tx := &Tx{}
	offset := 0

	// From
	copy(tx.From[:], data[offset:offset+20])
	offset += 20

	// To
	copy(tx.To[:], data[offset:offset+20])
	offset += 20

	// Value
	tx.Value = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	// Nonce
	tx.Nonce = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	// GasPrice
	tx.GasPrice = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	// GasLimit
	tx.GasLimit = binary.BigEndian.Uint64(data[offset:])
	offset += 8

	// DataLen
	dataLen := binary.BigEndian.Uint32(data[offset:])
	offset += 4

	// 데이터 크기 검증
	if len(data) < offset+int(dataLen)+8 {
		return nil, ValidationError{Field: "Data", Message: "데이터 크기 불일치"}
	}

	// Data
	tx.Data = make([]byte, dataLen)
	copy(tx.Data, data[offset:offset+int(dataLen)])
	offset += int(dataLen)

	// Timestamp
	tx.Timestamp = time.Unix(0, int64(binary.BigEndian.Uint64(data[offset:])))

	// 해시 계산
	tx.Hash = tx.ComputeHash()

	return tx, nil
}
