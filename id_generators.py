"""
ID 생성기 모듈

다양한 ID 유형을 생성하는 클래스들:
- UUIDv4: 완전 랜덤 UUID
- UUIDv7: 시간 기반 정렬 가능한 UUID  
- Sequential ID: 순차적 정수 ID
- ULID: 시간 기반 정렬 가능한 ID
- KSUID: K-Sortable Unique ID
- String ID: 일반 문자열 ID (최대 30자)
- Prefixed String ID: 접두사가 있는 문자열 ID (압축 테스트용)
"""

import uuid
import time
import random
import string
from abc import ABC, abstractmethod
from typing import List

try:
    from ulid import ULID
    ULID_AVAILABLE = True
except ImportError:
    ULID_AVAILABLE = False

try:
    from ksuid import ksuid
    KSUID_AVAILABLE = True
except ImportError:
    KSUID_AVAILABLE = False


class IDGenerator(ABC):
    """ID 생성기 추상 클래스"""

    @abstractmethod
    def generate(self) -> str:
        """단일 ID 생성"""
        pass

    @abstractmethod
    def generate_batch(self, count: int) -> List[str]:
        """배치로 ID 생성"""
        pass

    @abstractmethod
    def get_name(self) -> str:
        """ID 유형 이름 반환"""
        pass

    @abstractmethod
    def get_description(self) -> str:
        """ID 유형 설명 반환"""
        pass


class UUIDv4Generator(IDGenerator):
    """UUIDv4 생성기 - 완전 랜덤"""

    def generate(self) -> str:
        return str(uuid.uuid4())

    def generate_batch(self, count: int) -> List[str]:
        return [str(uuid.uuid4()) for _ in range(count)]

    def get_name(self) -> str:
        return "UUIDv4"

    def get_description(self) -> str:
        return "완전 랜덤 UUID - B-tree에서 최악의 성능 (랜덤 삽입)"


class UUIDv7Generator(IDGenerator):
    """UUIDv7 생성기 - 시간 기반 정렬 가능"""

    def generate(self) -> str:
        # UUIDv7은 Python 3.12+에서 지원, 간단한 구현 사용
        timestamp = int(time.time() * 1000)  # milliseconds
        random_part = random.getrandbits(74)  # 74 random bits

        # UUIDv7 format: timestamp(48) + ver(4) + random(12) + var(2) + random(62)
        uuid_int = (timestamp << 80) | (0x7 << 76) | (random_part >> 2)
        uuid_int |= (0x2 << 62) | (random_part & 0x3FFFFFFFFFFFFFFF)

        return str(uuid.UUID(int=uuid_int))

    def generate_batch(self, count: int) -> List[str]:
        return [self.generate() for _ in range(count)]

    def get_name(self) -> str:
        return "UUIDv7"

    def get_description(self) -> str:
        return "시간 기반 정렬 가능한 UUID - 좋은 B-tree 성능"


class SequentialIDGenerator(IDGenerator):
    """순차적 정수 ID 생성기"""

    def __init__(self, start: int = 1):
        self.current = start

    def generate(self) -> str:
        result = str(self.current)
        self.current += 1
        return result

    def generate_batch(self, count: int) -> List[str]:
        result = [str(i) for i in range(self.current, self.current + count)]
        self.current += count
        return result

    def get_name(self) -> str:
        return "Sequential ID"

    def get_description(self) -> str:
        return "순차적 정수 ID - B-tree에서 최고의 성능 (순차 삽입)"


class ULIDGenerator(IDGenerator):
    """ULID 생성기 - 시간 기반 정렬 가능"""

    def __init__(self):
        self.available = ULID_AVAILABLE
        if not self.available:
            print("Warning: ULID library not available, "
                  "using fallback implementation")

    def generate(self) -> str:
        if self.available:
            try:
                # Try common ULID APIs from different implementations
                # 1) class/module may expose a `new()` factory
                if hasattr(ULID, 'new') and callable(getattr(ULID, 'new')):
                    return str(ULID.new())

                # 2) some libs expose `from_timestamp` or similar
                if hasattr(ULID, 'from_timestamp') and callable(getattr(ULID, 'from_timestamp')):
                    try:
                        # many implementations expect seconds or milliseconds
                        # pass seconds as float; wrapper will handle conversion if needed
                        return str(ULID.from_timestamp(time.time()))
                    except TypeError:
                        # fallback to milliseconds if seconds signature doesn't match
                        return str(ULID.from_timestamp(int(time.time() * 1000)))

                # 3) lastly try calling the class directly (some libs support ULID())
                return str(ULID())
            except TypeError:
                # Some ULID implementations require a buffer/bytes in the ctor
                # Fall back to our internal string-based implementation below
                pass
            except Exception:
                # Any other unexpected error -> fallback
                pass
        # Fallback implementation
        timestamp = int(time.time() * 1000)
        
        # Base32 encoding for timestamp (10 characters)
        base32_chars = '0123456789ABCDEFGHJKMNPQRSTVWXYZ'
        timestamp_base32 = ''
        temp_timestamp = timestamp
        for _ in range(10):
            timestamp_base32 = base32_chars[temp_timestamp % 32] + timestamp_base32
            temp_timestamp //= 32
        
        # Random part (16 characters)
        random_part = ''.join(random.choices(base32_chars, k=16))
        
        # ULID format: 10 chars timestamp + 16 chars randomness
        return timestamp_base32 + random_part

    def generate_batch(self, count: int) -> List[str]:
        return [self.generate() for _ in range(count)]

    def get_name(self) -> str:
        return "ULID"

    def get_description(self) -> str:
        return ("Universally Unique Lexicographically Sortable "
                "Identifier - 우수한 B-tree 성능")


class KSUIDGenerator(IDGenerator):
    """KSUID 생성기 - K-Sortable Unique ID"""

    def __init__(self):
        self.available = KSUID_AVAILABLE
        if not self.available:
            print("Warning: KSUID library not available, "
                  "using fallback implementation")

    def generate(self) -> str:
        if self.available:
            return str(ksuid())
        # Fallback implementation - KSUID uses Base62
        timestamp = int(time.time()) - 1400000000  # KSUID epoch
        
        # Base62 encoding for proper KSUID format
        base62_chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
        
        # Encode timestamp in base62 (4 chars should be enough)
        timestamp_base62 = ''
        temp_timestamp = timestamp
        for _ in range(4):
            timestamp_base62 = base62_chars[temp_timestamp % 62] + timestamp_base62
            temp_timestamp //= 62
        
        # Random part (23 characters for total 27)
        random_part = ''.join(random.choices(base62_chars, k=23))
        
        return timestamp_base62 + random_part

    def generate_batch(self, count: int) -> List[str]:
        return [self.generate() for _ in range(count)]

    def get_name(self) -> str:
        return "KSUID"

    def get_description(self) -> str:
        return "K-Sortable Unique Identifier - 좋은 B-tree 성능"


class StringIDGenerator(IDGenerator):
    """완전 무작위 문자열 ID 생성기 (최대 30자)"""

    def __init__(self, max_length: int = 30, min_length: int = 10):
        self.max_length = max_length
        self.min_length = min_length
        # 다양한 문자 집합 사용 (대소문자, 숫자, 일부 특수문자)
        self.char_set = string.ascii_letters + string.digits + '_-'

    def generate(self) -> str:
        # 완전 무작위 길이
        length = random.randint(self.min_length, self.max_length)
        # 완전 무작위 문자 조합
        return ''.join(random.choices(self.char_set, k=length))

    def generate_batch(self, count: int) -> List[str]:
        return [self.generate() for _ in range(count)]

    def get_name(self) -> str:
        return f"Random String ({self.min_length}-{self.max_length})"

    def get_description(self) -> str:
        return (f"완전 무작위 문자열 ID ({self.min_length}-{self.max_length}자) - "
                "B-tree에서 UUIDv4와 유사한 최악 성능")


class PrefixedStringIDGenerator(IDGenerator):
    """접두사가 있는 문자열 ID 생성기 (압축 테스트용)"""

    def __init__(self, prefix: str = "USER", max_length: int = 30):
        self.prefix = prefix
        self.max_length = max_length
        self.counter = 0

    def generate(self) -> str:
        self.counter += 1
        # 접두사 + 무작위 부분 + 카운터
        remaining_length = (self.max_length - len(self.prefix) - 8)
        if remaining_length > 5:
            random_part = ''.join(random.choices(
                string.ascii_letters + string.digits,
                k=random.randint(5, remaining_length)))
            return f"{self.prefix}_{random_part}_{self.counter:06d}"
        return f"{self.prefix}_{self.counter:06d}"

    def generate_batch(self, count: int) -> List[str]:
        return [self.generate() for _ in range(count)]

    def get_name(self) -> str:
        return f"Prefixed String ({self.prefix})"

    def get_description(self) -> str:
        return f"접두사 문자열 ID ({self.prefix}) - 압축에 유리한 패턴"


class IDGeneratorFactory:
    """ID 생성기 팩토리"""

    @staticmethod
    def get_all_generators() -> List[IDGenerator]:
        """모든 사용 가능한 ID 생성기 반환"""
        generators = [
            UUIDv4Generator(),
            UUIDv7Generator(),
            SequentialIDGenerator(),
            ULIDGenerator(),
            KSUIDGenerator(),
            StringIDGenerator(max_length=30, min_length=30),  # 무작위 문자열
            StringIDGenerator(max_length=20, min_length=20),  # 다른 길이
            PrefixedStringIDGenerator(prefix="USER", max_length=30),
            PrefixedStringIDGenerator(prefix="ORD", max_length=20),
        ]
        return generators

    @staticmethod
    def get_generator_by_name(name: str) -> IDGenerator:
        """이름으로 ID 생성기 반환"""
        generators = {
            "uuidv4": UUIDv4Generator(),
            "uuidv7": UUIDv7Generator(),
            "sequential": SequentialIDGenerator(),
            "ulid": ULIDGenerator(),
            "ksuid": KSUIDGenerator(),
            "string": StringIDGenerator(),
        }

        if name.lower() not in generators:
            raise ValueError(f"Unknown generator: {name}")

        return generators[name.lower()]

    @staticmethod
    def get_performance_categories() -> dict:
        """성능 카테고리별로 생성기 분류"""
        return {
            "최고 성능 (순차적)": [SequentialIDGenerator()],
            "우수한 성능 (시간 기반 정렬)": [
                UUIDv7Generator(),
                ULIDGenerator(),
                KSUIDGenerator()
            ],
            "가변 성능 (문자열)": [
                StringIDGenerator(max_length=30),
                StringIDGenerator(max_length=20),
                PrefixedStringIDGenerator(prefix="USER", max_length=30),
                PrefixedStringIDGenerator(prefix="ORD", max_length=20)
            ],
            "최악 성능 (완전 랜덤)": [UUIDv4Generator()]
        }


def demonstrate_id_generators():
    """ID 생성기 데모"""
    print("=== ID 생성기 데모 ===\n")

    generators = IDGeneratorFactory.get_all_generators()

    for generator in generators:
        print(f"{generator.get_name()}:")
        print(f"  설명: {generator.get_description()}")

        # 샘플 ID 생성
        sample_ids = generator.generate_batch(5)
        print("  샘플 ID들:")
        for i, id_val in enumerate(sample_ids, 1):
            print(f"    {i}. {id_val}")

        # ID 길이 통계
        lengths = [len(id_val) for id_val in sample_ids]
        avg_length = sum(lengths) / len(lengths)
        print(f"  평균 길이: {avg_length:.1f}자")
        print()


if __name__ == "__main__":
    demonstrate_id_generators()