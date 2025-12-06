// Package transport는 TCP 기반 네트워크 전송 계층을 구현
// 이 패키지는 P2P 네트워크의 가장 하위 레이어로, 실제 바이트 송수신을 담당
package transport

import "sync"

// Buffer Pool - Zero-Allocation 네트워킹을 위한 버퍼 재사용

// 기본 버퍼 크기 상수
const (
	// DefaultBufferSize는 네트워크 I/O에 사용되는 기본 버퍼 크기입니다.
	//
	// [왜 4096바이트인가?]
	// - 일반적인 이더넷 MTU(Maximum Transmission Unit)는 1500바이트
	// - TCP MSS(Maximum Segment Size)는 약 1460바이트 (MTU - IP헤더20 - TCP헤더20)
	// - 4096 = 약 3개의 TCP 세그먼트를 담을 수 있는 크기
	// - 대부분의 P2P 메시지(트랜잭션, 피어 정보 등)가 이 안에 들어옴
	// - 페이지 크기(4KB)와 일치하여 메모리 정렬에 유리
	DefaultBufferSize = 4096

	// LargeBufferSize는 블록 같은 큰 데이터 전송용 버퍼입니다.
	//
	// [왜 64KB인가?]
	// - TCP 수신 윈도우 기본 크기가 보통 64KB
	// - 블록 데이터는 수 KB ~ 수십 KB까지 다양
	// - 너무 크면 메모리 낭비, 너무 작으면 여러 번 읽어야 함
	LargeBufferSize = 65536
)

// bufferPool은 일반 크기 버퍼를 재사용하기 위한 풀임
//
// [sync.Pool이 하는 일]
// 1. Get() 호출 시: 풀에서 사용 가능한 버퍼 반환 (없으면 New 호출)
// 2. Put() 호출 시: 버퍼를 풀에 반환 (재사용 가능하게)
//
// [내부 동작 - Go 런타임 레벨]
// - sync.Pool은 P(Processor)마다 로컬 풀을 가짐
// - Get() 시 먼저 로컬 풀 확인 → 없으면 다른 P의 풀에서 훔쳐옴(steal)
// - 이 방식으로 락 경쟁(lock contention) 최소화
//
// [GC와의 관계]
// - GC가 일어나면 풀이 비워질 수 있음 (victim cache 제외)
// - 풀은 캐시일 뿐, 영구 저장소가 아님
// - 따라서 풀에서 꺼낸 객체는 항상 초기화 필요
var bufferPool = sync.Pool{
	New: func() interface{} {
		// make([]byte, size)가 하는 일:
		// 1. 힙 메모리 할당 (mallocgc 호출)
		// 2. 슬라이스 헤더 생성 (ptr, len, cap)
		// 3. 메모리를 0으로 초기화
		//
		// 포인터로 반환하는 이유:
		// - 슬라이스 자체가 24바이트 헤더 (ptr 8 + len 8 + cap 8)
		// - interface{}로 변환 시 슬라이스 헤더가 복사됨
		// - 포인터로 하면 8바이트만 복사 → 더 효율적
		buf := make([]byte, DefaultBufferSize)
		return &buf
	},
}

// largeBufferPool은 큰 데이터(블록 등) 전송용 버퍼 풀입니다.
var largeBufferPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, LargeBufferSize)
		return &buf
	},
}

// GetBuffer는 풀에서 일반 크기 버퍼를 가져옵니다.
//
// [사용 패턴]
//
//	buf := GetBuffer()
//	defer PutBuffer(buf)  // 반드시 반환!
//	n, err := conn.Read(buf)
//
// [주의사항]
// 1. 반환된 버퍼의 내용은 이전 사용자의 데이터일 수 있음 → 덮어쓰기 전 의존 금지
// 2. 버퍼를 반환(Put) 후에는 절대 접근하면 안 됨 → use-after-free 버그
// 3. 버퍼의 참조를 저장하면 안 됨 → 다른 곳에서 재사용될 수 있음
func GetBuffer() []byte {
	// Get()이 반환하는 타입: interface{}
	// 타입 단언(type assertion)으로 *[]byte로 변환
	// 역참조(*)로 실제 슬라이스 획득
	return *(bufferPool.Get().(*[]byte))
}

// PutBuffer는 버퍼를 풀에 반환합니다.
//
// [왜 길이를 cap으로 복구하나?]
// 사용 중에 buf = buf[:n] 같이 길이가 줄어들 수 있음
// 다음 사용자가 전체 버퍼를 쓸 수 있도록 원래 크기로 복구
//
// [보안 고려사항]
// 실제 프로덕션에서는 버퍼를 0으로 클리어할 수 있음
// 민감한 데이터(개인키 등)가 남아있을 수 있기 때문
// 하지만 성능상 이유로 여기서는 생략
func PutBuffer(buf []byte) {
	// 길이를 원래 용량(capacity)으로 복구
	// buf[:cap(buf)]는 새 슬라이스 헤더를 만들지만,
	// 같은 underlying array를 가리킴
	buf = buf[:cap(buf)]
	bufferPool.Put(&buf)
}

// GetLargeBuffer는 큰 데이터 전송용 버퍼를 가져옵니다.
//
// [언제 사용하나?]
// - 블록 데이터 수신 (수 KB ~ 수십 KB)
// - 대량의 트랜잭션 배치 처리
// - 피어 목록이 매우 긴 경우
func GetLargeBuffer() []byte {
	return *(largeBufferPool.Get().(*[]byte))
}

// PutLargeBuffer는 큰 버퍼를 풀에 반환합니다.
func PutLargeBuffer(buf []byte) {
	buf = buf[:cap(buf)]
	largeBufferPool.Put(&buf)
}

// =============================================================================
// 버퍼 풀 통계 (디버깅/모니터링용)
// =============================================================================

// BufferPoolStats는 버퍼 풀 사용 통계입니다.
// 실제 sync.Pool은 내부 통계를 제공하지 않으므로,
// 필요시 래퍼를 만들어 추적할 수 있습니다.
type BufferPoolStats struct {
	Gets       uint64 // Get 호출 횟수
	Puts       uint64 // Put 호출 횟수
	News       uint64 // New 함수 호출 횟수 (풀이 비어서 새로 생성)
	InUse      uint64 // 현재 사용 중인 버퍼 수 (Gets - Puts)
}

// 참고: 실제 통계 추적이 필요하면 atomic 카운터를 추가해야 함
// 현재는 구조체 정의만 제공
