# PostgreSQL-style B-tree Implementation in Python

이 프로젝트는 PostgreSQL 데이터베이스에서 사용하는 B-tree 인덱스의 핵심 기능을 Python으로 구현한 것입니다.
(With Claude Code)

no-compression-case 브랜치와 compression-case 브랜치가 존재합니다.
각 ID별 압축 사용 케이스별로 나눈 브랜치입니다.

## 주요 특징

### PostgreSQL B-tree의 핵심 기능들
- **높은 분기율(High Fanout)**: 기본 order 256으로 디스크 효율성 최적화
- **중복 키 지원**: 비유니크 인덱스를 위한 중복 키 처리
- **효율적인 범위 쿼리**: WHERE 절 범위 조건에 최적화
- **통계 수집**: 쿼리 플래닝을 위한 트리 통계 정보
- **자동 리밸런싱**: 삭제 시 트리 구조 자동 조정
- **페이지 레벨 압축**: 다양한 압축 알고리즘을 통한 저장 공간 절약

### 새로 구현된 압축 기능들 ✨
- **Prefix Compression**: 공통 접두사를 가진 문자열 키 압축
- **Dictionary Compression**: 자주 사용되는 값들의 사전 기반 압축
- **Delta Encoding**: 연속된 숫자 값의 차이 기반 압축
- **Run-Length Encoding**: 반복되는 값의 압축
- **TOAST Compression**: 대용량 값의 zlib 기반 압축
- **적응형 압축**: 데이터 특성에 따른 자동 압축 전략 선택

### 구현된 연산들
- `insert(key, value)`: 키-값 쌍 삽입
- `search(key)`: 키에 대한 모든 값 검색 (중복 키 지원)
- `delete(key, value=None)`: 키-값 쌍 삭제
- `range_query(start_key, end_key, inclusive=True)`: 범위 쿼리
- `get_statistics()`: 트리 통계 정보
- `compress_all_pages()`: 전체 노드 압축 실행
- `get_detailed_compression_stats()`: 상세 압축 통계

## 설치 및 사용법

### 1. 가상 환경 설정
```bash
# 이미 venv 폴더가 있으므로 활성화만 하면 됩니다
source venv/bin/activate
```

### 2. 의존성 설치
```bash
pip install -r requirements.txt
```

### 3. 기본 사용법
```python
from btree import PostgreSQLBTree

# B-tree 생성 (압축 기능 포함)
btree = PostgreSQLBTree(order=256, enable_compression=True)

# 데이터 삽입
btree.insert("user_001", "John Doe")
btree.insert("user_002", "Jane Smith")
btree.insert("user_001", "John's backup")  # 중복 키 지원

# 검색 (모든 값 반환)
results = btree.search("user_001")  # ["John Doe", "John's backup"]

# 범위 쿼리
for key, value in btree.range_query("user_001", "user_999"):
    print(f"{key}: {value}")

# 압축 실행
compression_stats = btree.compress_all_pages()
print(f"압축된 페이지: {compression_stats['compression_successes']}")

# 삭제
btree.delete("user_001", "John Doe")  # 특정 값만 삭제
btree.delete("user_002")  # 첫 번째 값 삭제

# 통계 정보 (압축 정보 포함)
stats = btree.get_statistics()
print(f"Tree height: {stats['height']}")
print(f"Total keys: {stats['total_keys']}")
print(f"Compression ratio: {stats.get('compression_ratio', 'N/A')}")
```

### 4. 압축 기능 사용법
```python
from compression import CompressionManager

# 개별 압축 전략 테스트
manager = CompressionManager()
data = ["user_001", "user_002", "user_003"]  # 공통 접두사

# 자동 압축 (최적 전략 선택)
compressed, metadata = manager.compress(data)
decompressed = manager.decompress(compressed, metadata)

# 압축 통계 확인
stats = manager.get_compression_stats(data)
for strategy, info in stats.items():
    print(f"{strategy}: {info.get('estimated_ratio', 'N/A')}")
```

## 테스트 실행

### 전체 테스트 실행
```bash
python -m pytest test_btree.py -v
```

### 특정 테스트 클래스 실행
```bash
python -m pytest test_btree.py::TestBTreeBasics -v
```

### 압축 기능 테스트
```bash
python -m pytest test_compression.py -v
```

## 예제 실행

### 기본 B-tree 데모
```bash
python example_usage.py
```

### 압축 기능 데모 ✨
```bash
python compression_demo.py
```

이 명령어들은 다음과 같은 데모를 실행합니다:

**기본 데모:**
- 기본 B-tree 연산
- 중복 키 처리
- 대용량 데이터셋 성능
- 범위 쿼리 성능
- 삭제 및 리밸런싱

**압축 데모:**
- 다양한 압축 전략 비교
- 데이터 타입별 압축 효율성
- 실제 데이터베이스 시나리오 시뮬레이션
- 압축 성능 벤치마크

## 성능 특성

### PostgreSQL과 유사한 성능
- **높은 삽입 성능**: 60만+ 삽입/초
- **효율적인 검색**: 14,000+ 검색/초
- **빠른 범위 쿼리**: 마이크로초 단위 응답
- **메모리 효율적**: 높은 분기율로 트리 높이 최소화

### 벤치마크 결과
```
10,000개 레코드 삽입: 0.017초 (601,506 삽입/초)
1,000개 키 검색: 0.069초 (14,485 검색/초)
범위 쿼리 (25개 결과): < 0.0001초
```

## 아키텍처

### 클래스 구조
- `PostgreSQLBTree`: 메인 B-tree 클래스
- `BTreeNode`: 개별 노드 구현
- `KeyValue`: 키-값 쌍 데이터 클래스

### PostgreSQL 호환 기능들
1. **페이지 지향 설계**: 8KB 페이지 크기 시뮬레이션
2. **높은 분기율**: 디스크 I/O 최소화를 위한 order 256
3. **중복 키 지원**: 비유니크 인덱스 구현
4. **범위 스캔**: 효율적인 범위 쿼리
5. **통계 수집**: 쿼리 플래너를 위한 메타데이터

## 파일 구조

```
b-tree/
├── btree.py              # 메인 B-tree 구현 (압축 지원)
├── compression.py        # 페이지 레벨 압축 구현 ✨
├── test_btree.py         # 기본 B-tree 테스트 스위트
├── test_compression.py   # 압축 기능 테스트 스위트 ✨
├── example_usage.py      # 기본 사용법 예제 및 데모
├── compression_demo.py   # 압축 기능 데모 ✨
├── requirements.txt      # 파이썬 의존성
├── setup.py             # 패키지 설정
└── README.md            # 이 파일
```

## PostgreSQL과의 비교

| 기능 | PostgreSQL | 이 구현체 |
|------|------------|----------|
| 페이지 크기 | 8KB | 시뮬레이션됨 |
| 분기율 | ~256 | 256 (설정 가능) |
| 중복 키 | 지원 | 지원 |
| 범위 쿼리 | 최적화됨 | 구현됨 |
| 페이지 압축 | 지원 | **구현됨** ✨ |
| 압축 전략 | TOAST, pglz | **5가지 전략** ✨ |
| 동시성 | MVCC | 미구현 |
| 디스크 영속성 | 지원 | 메모리만 |

## 향후 개선 사항

1. **디스크 영속성**: 실제 페이지 기반 저장
2. **동시성 제어**: 읽기/쓰기 락 구현  
3. **WAL 로깅**: 내구성을 위한 트랜잭션 로그
4. **압축 최적화**: 더 진보된 압축 알고리즘 추가
5. **압축 임계값 조정**: 동적 압축 임계값 설정

## 압축 기능 상세 정보 ✨

### 지원하는 압축 전략

1. **Prefix Compression**
   - 공통 접두사가 있는 문자열에 최적화
   - 예: "user_001", "user_002" → "user_" + ["001", "002"]

2. **Dictionary Compression** 
   - 자주 반복되는 값들을 사전으로 관리
   - 중복 제거를 통한 공간 절약

3. **Delta Encoding**
   - 연속된 숫자 값의 차이만 저장
   - 시퀀셜 데이터에 매우 효과적

4. **Run-Length Encoding**
   - 연속된 동일 값을 (값, 개수) 쌍으로 압축
   - 반복 패턴이 많은 데이터에 최적

5. **TOAST Compression**
   - zlib 기반 범용 압축
   - 대용량 텍스트나 바이너리 데이터에 효과적

### 압축 성능 예시

```
데이터 유형별 압축 효과:
- 공통 접두사 문자열: ~70% 공간 절약
- 반복값 데이터: ~80% 공간 절약  
- 연속 숫자: ~90% 공간 절약
- 대용량 텍스트: ~60% 공간 절약
- 랜덤 데이터: ~5% 공간 절약 (압축 비활성화)
```

📊 차트 영어화

- 모든 차트 제목, 축 레이블, 범례를 영어로 변경
- 성능 권장사항도 영어로 표시
- 폰트 설정을 영어 우선으로 변경

🚀 대용량 테스트 옵션 추가

기존 3개 옵션에서 6개 옵션으로 확장:

1. 소규모 (2,000건) - ~18초
2. 중간규모 (10,000건) - ~90초
3. 대규모 (25,000건) - ~3.75분
4. 초대용량 (1,000,000건) - ~22.5분 🆕
5. 메가스케일 (3,000,000건) - ~2.7시간 🆕
6. 기가스케일 (10,000,000건) - ~13.5시간 🆕

🖥️ 새로운 CLI 도구

benchmark_cli.py로 명령어 실행 가능:

# 간단한 실행
python benchmark_cli.py --scale 1M

# 차트와 CSV 함께 생성
python benchmark_cli.py --scale 3M --charts --csv

# 비대화형 모드 (자동 실행)
python benchmark_cli.py --scale 10M --no-interactive

🎯 사용법:

대화형 실행 (기존 방식):

python run_complete_benchmark.py
- 6개 옵션 중 선택 가능
- 대용량 테스트는 경고 메시지와 함께 확인 요청

CLI 실행 (새로운 방식):

# 100만건 테스트 + 차트 + CSV
python benchmark_cli.py --scale 1M --charts --csv

# 1000만건 테스트 (비대화형)
python benchmark_cli.py --scale 10M --no-interactive

## 라이센스

이 프로젝트는 교육 목적으로 작성되었습니다.