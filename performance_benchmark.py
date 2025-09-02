"""
B-tree ID 유형별 성능 벤치마크 시스템

다양한 ID 유형에 대한 B-tree 성능을 측정하고 비교:
- 삽입 성능
- 검색 성능  
- 범위 쿼리 성능
- 메모리 사용량
- 압축 효율성
- 페이지 분할 횟수
"""

import time
import random
import gc
import sys
import traceback
import tracemalloc
from typing import Dict, List, Any
from dataclasses import dataclass
from btree import PostgreSQLBTree
from id_generators import IDGeneratorFactory, IDGenerator


@dataclass
class PerformanceMetrics:
    """성능 측정 결과 저장 클래스"""
    id_type: str
    record_count: int
    
    # 삽입 성능
    insert_time: float
    insert_rate: float  # records/sec
    
    # 검색 성능
    search_time: float
    search_rate: float  # searches/sec
    
    # 범위 쿼리 성능
    range_query_time: float
    range_query_rate: float  # queries/sec
    
    # 메모리 사용량
    memory_usage_mb: float
    
    # B-tree 통계
    tree_height: int
    leaf_pages: int
    internal_pages: int
    
    # 압축 통계
    compression_ratio: float
    compressed_pages: int
    space_saved_bytes: int
    
    # 페이지 분할 통계 (추정)
    estimated_splits: int
    
    # ID 특성
    avg_id_length: float
    id_description: str


class BTreePerformanceBenchmark:
    """B-tree 성능 벤치마크 클래스"""
    
    def __init__(self, order: int = 50, enable_compression: bool = False):  # 압축 비활성화
        self.order = order
        self.enable_compression = enable_compression
        self.results: List[PerformanceMetrics] = []
    
    def benchmark_single_id_type(
        self, 
        generator: IDGenerator, 
        record_count: int = 10000,
        search_sample_size: int = 1000,
        range_query_count: int = 100
    ) -> PerformanceMetrics:
        """단일 ID 유형에 대한 종합 성능 벤치마크"""
        
        print(f"\n=== {generator.get_name()} 벤치마크 시작 ===")
        print(f"레코드 수: {record_count:,}")
        
        # 메모리 추적 시작
        tracemalloc.start()
        gc.collect()
        
        # B-tree 초기화
        btree = PostgreSQLBTree(order=self.order, enable_compression=self.enable_compression)
        
        # 1. ID 생성
        print("ID 생성 중...")
        ids = generator.generate_batch(record_count)
        avg_id_length = sum(len(id_str) for id_str in ids) / len(ids)
        
        # 2. 삽입 성능 측정
        print("삽입 성능 측정 중...")
        insert_start = time.perf_counter()
        
        for i, id_str in enumerate(ids):
            btree.insert(id_str, f"Record_{i}")
            
            if (i + 1) % 1000 == 0:
                print(f"  삽입 진행률: {i+1:,}/{record_count:,}")
        
        insert_time = time.perf_counter() - insert_start
        insert_rate = record_count / insert_time if insert_time > 0 else 0
        
        # B-tree 통계 수집
        tree_stats = btree.get_statistics()
        
        # 3. 압축 실행 및 측정 (조건부)
        if self.enable_compression:
            print("압축 실행 중...")
            compression_stats = btree.compress_all_pages()
            detailed_compression = btree.get_detailed_compression_stats()
        else:
            print("압축 비활성화됨")
            compression_stats = {'compression_successes': 0}
            detailed_compression = {'overall_compression_ratio': 1.0, 'total_original_size': 0, 'total_compressed_size': 0}
        
        compression_ratio = detailed_compression.get('overall_compression_ratio', 1.0)
        compressed_pages = compression_stats.get('compression_successes', 0)
        space_saved = (detailed_compression.get('total_original_size', 0) - 
                      detailed_compression.get('total_compressed_size', 0))
        
        # 4. 검색 성능 측정
        print("검색 성능 측정 중...")
        search_sample = random.sample(ids, min(search_sample_size, len(ids)))
        
        search_start = time.perf_counter()
        successful_searches = 0
        
        for search_id in search_sample:
            results = btree.search(search_id)
            if results:
                successful_searches += 1
        
        search_time = time.perf_counter() - search_start
        search_rate = len(search_sample) / search_time if search_time > 0 else 0
        
        # 5. 범위 쿼리 성능 측정
        print("범위 쿼리 성능 측정 중...")
        range_start = time.perf_counter()
        
        total_range_results = 0

        # Pre-sort ids once for range queries to avoid repeated O(n log n) work
        if generator.get_name() == "Sequential ID":
            sorted_ids = None
        else:
            sorted_ids = sorted(ids)

        for _ in range(range_query_count):
            # 랜덤한 범위 선택
            max_start = max(0, len(ids) - 10)  # 최소 10개는 남겨둠
            start_idx = random.randint(0, max_start)

            # 범위 크기를 데이터 크기에 맞게 조정
            max_range_size = min(100, len(ids) - start_idx)
            range_size = random.randint(1, max(1, max_range_size))
            end_idx = min(start_idx + range_size, len(ids) - 1)

            if generator.get_name() == "Sequential ID":
                # 순차 ID는 숫자 기반 범위 쿼리
                start_key = str(start_idx + 1)
                end_key = str(end_idx + 1)
            else:
                # 다른 ID들은 정렬된 ID 기준 범위 쿼리
                start_key = sorted_ids[start_idx]
                end_key = sorted_ids[end_idx]

            range_results = list(btree.range_query(start_key, end_key))
            total_range_results += len(range_results)
        
        range_time = time.perf_counter() - range_start
        range_rate = range_query_count / range_time if range_time > 0 else 0
        
        # 6. 메모리 사용량 측정
        current, peak = tracemalloc.get_traced_memory()
        memory_usage_mb = peak / (1024 * 1024)  # Convert to MB
        tracemalloc.stop()
        
        # 7. 페이지 분할 추정
        # 이론적으로 순차 삽입은 분할이 적고, 랜덤 삽입은 분할이 많음
        theoretical_min_splits = record_count // self.order
        if generator.get_name() == "Sequential ID":
            estimated_splits = theoretical_min_splits
        elif generator.get_name() == "UUIDv4":
            estimated_splits = int(theoretical_min_splits * 2.5)  # 랜덤 삽입
        else:
            estimated_splits = int(theoretical_min_splits * 1.5)  # 시간 기반
        
        print(f"벤치마크 완료 - 성공한 검색: {successful_searches}/{len(search_sample)}")
        print(f"범위 쿼리 결과: {total_range_results}개 레코드")
        
        return PerformanceMetrics(
            id_type=generator.get_name(),
            record_count=record_count,
            insert_time=insert_time,
            insert_rate=insert_rate,
            search_time=search_time,
            search_rate=search_rate,
            range_query_time=range_time,
            range_query_rate=range_rate,
            memory_usage_mb=memory_usage_mb,
            tree_height=tree_stats['height'],
            leaf_pages=tree_stats['leaf_pages'],
            internal_pages=tree_stats['internal_pages'],
            compression_ratio=compression_ratio,
            compressed_pages=compressed_pages,
            space_saved_bytes=space_saved,
            estimated_splits=estimated_splits,
            avg_id_length=avg_id_length,
            id_description=generator.get_description()
        )
    
    def benchmark_all_id_types(
        self, 
        record_count: int = 10000,
        search_sample_size: int = 1000,
        range_query_count: int = 100
    ) -> List[PerformanceMetrics]:
        """모든 ID 유형에 대한 벤치마크 실행"""
        
        print("=" * 60)
        print("B-tree ID 유형별 성능 벤치마크")
        print("=" * 60)
        
        generators = IDGeneratorFactory.get_all_generators()
        self.results = []
        
        for i, generator in enumerate(generators, 1):
            print(f"\n[{i}/{len(generators)}] {generator.get_name()} 벤치마크")
            
            try:
                metrics = self.benchmark_single_id_type(
                    generator, record_count, search_sample_size, range_query_count
                )
                self.results.append(metrics)
            except Exception as e:
                print(f"오류 발생: {e}")
                print(f"상세 오류:\n{traceback.format_exc()}")
                continue
            
            # 메모리 정리
            gc.collect()
        
        return self.results
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """성능 요약 통계 반환"""
        if not self.results:
            return {}
        
        summary = {
            '총_ID_유형수': len(self.results),
            '최고_삽입_성능': max(self.results, key=lambda x: x.insert_rate),
            '최고_검색_성능': max(self.results, key=lambda x: x.search_rate),
            '최고_범위쿼리_성능': max(self.results, key=lambda x: x.range_query_rate),
            '최고_압축효율': min(self.results, key=lambda x: x.compression_ratio),
            '최소_메모리사용': min(self.results, key=lambda x: x.memory_usage_mb),
            '최소_트리높이': min(self.results, key=lambda x: x.tree_height),
            '최소_페이지분할': min(self.results, key=lambda x: x.estimated_splits),
        }
        
        return summary
    
    def print_results_table(self):
        """결과를 표 형태로 출력"""
        if not self.results:
            print("벤치마크 결과가 없습니다.")
            return
        
        print("\n" + "=" * 120)
        print("B-tree ID 유형별 성능 비교 결과")
        print("=" * 120)
        
        # 헤더
        header = (
            f"{'ID 유형':<15} {'삽입율':<10} {'검색율':<10} {'범위쿼리':<10} "
            f"{'메모리MB':<8} {'높이':<4} {'압축률':<6} {'분할수':<6} {'ID길이':<6}"
        )
        print(header)
        print("-" * 120)
        
        # 삽입 성능순으로 정렬
        sorted_results = sorted(self.results, key=lambda x: x.insert_rate, reverse=True)
        
        for metrics in sorted_results:
            row = (
                f"{metrics.id_type:<15} "
                f"{metrics.insert_rate:>8.0f}/s "
                f"{metrics.search_rate:>8.0f}/s "
                f"{metrics.range_query_rate:>8.0f}/s "
                f"{metrics.memory_usage_mb:>6.1f} "
                f"{metrics.tree_height:>4d} "
                f"{metrics.compression_ratio:>6.2f} "
                f"{metrics.estimated_splits:>6d} "
                f"{metrics.avg_id_length:>6.1f}"
            )
            print(row)
        
        print("-" * 120)
        
        # 요약 통계
        summary = self.get_performance_summary()
        print("\n성능 요약:")
        print(f"  최고 삽입 성능: {summary['최고_삽입_성능'].id_type} ({summary['최고_삽입_성능'].insert_rate:,.0f} records/sec)")
        print(f"  최고 검색 성능: {summary['최고_검색_성능'].id_type} ({summary['최고_검색_성능'].search_rate:,.0f} searches/sec)")
        print(f"  최고 압축 효율: {summary['최고_압축효율'].id_type} (압축률 {summary['최고_압축효율'].compression_ratio:.2f})")
        print(f"  최소 메모리 사용: {summary['최소_메모리사용'].id_type} ({summary['최소_메모리사용'].memory_usage_mb:.1f} MB)")


def run_quick_benchmark():
    """빠른 벤치마크 실행"""
    benchmark = BTreePerformanceBenchmark(order=20, enable_compression=True)
    results = benchmark.benchmark_all_id_types(
        record_count=5000,  # 적은 수로 빠른 테스트
        search_sample_size=500,
        range_query_count=50
    )
    
    benchmark.print_results_table()
    return results


def run_comprehensive_benchmark():
    """종합적인 벤치마크 실행"""
    benchmark = BTreePerformanceBenchmark(order=50, enable_compression=True)
    results = benchmark.benchmark_all_id_types(
        record_count=50000,  # 큰 데이터셋
        search_sample_size=2000,
        range_query_count=200
    )
    
    benchmark.print_results_table()
    return results


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--quick":
        print("빠른 벤치마크 실행 중...")
        run_quick_benchmark()
    else:
        print("종합 벤치마크 실행 중...")
        run_comprehensive_benchmark()