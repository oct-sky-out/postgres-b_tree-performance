"""
완전한 B-tree ID 성능 벤치마크 및 시각화 실행 스크립트

이 스크립트는:
1. 다양한 ID 유형에 대한 성능 벤치마크 실행
2. 결과를 표와 차트로 시각화  
3. 종합 성능 리포트 생성
"""

import os
import sys
import time
from performance_benchmark import BTreePerformanceBenchmark, PerformanceMetrics
from performance_visualizer import PerformanceVisualizer, create_performance_report
from typing import List

def run_comprehensive_id_benchmark():
    """포괄적인 ID 성능 벤치마크 실행"""
    
    print("=" * 80)
    print("🚀 B-tree ID 유형별 종합 성능 벤치마크")
    print("=" * 80)
    
    # 벤치마크 설정
    benchmark_configs = [
        {
            'name': '소규모 테스트 (빠른 실행)',
            'record_count': 2000,
            'search_sample_size': 200,
            'range_query_count': 20,
            'order': 15
        },
        {
            'name': '중간 규모 테스트 (성능 차이 명확)',
            'record_count': 10000,
            'search_sample_size': 1000,
            'range_query_count': 100,
            'order': 30
        },
        {
            'name': '대규모 테스트 (극명한 성능 차이)',
            'record_count': 25000,
            'search_sample_size': 2000,
            'range_query_count': 200,
            'order': 50
        },
        {
            'name': '초대용량 테스트 - 100만건 (매우 극명한 차이)',
            'record_count': 1000000,
            'search_sample_size': 10000,
            'range_query_count': 1000,
            'order': 100
        },
        {
            'name': '메가스케일 테스트 - 300만건 (극한 성능 차이)',
            'record_count': 3000000,
            'search_sample_size': 30000,
            'range_query_count': 3000,
            'order': 150
        },
        {
            'name': '기가스케일 테스트 - 1000만건 (최극한 성능 차이)',
            'record_count': 10000000,
            'search_sample_size': 100000,
            'range_query_count': 10000,
            'order': 200
        }
    ]
    
    # 사용자가 테스트 규모 선택
    print("테스트 규모를 선택하세요:")
    for i, config in enumerate(benchmark_configs, 1):
        print(f"  {i}. {config['name']}")
        print(f"     - 레코드 수: {config['record_count']:,}")
        print(f"     - 예상 실행 시간: {estimate_runtime(config['record_count'])}")
        print()
    
    while True:
        try:
            choice = input(f"선택 (1-{len(benchmark_configs)}): ")
            choice_idx = int(choice) - 1
            if 0 <= choice_idx < len(benchmark_configs):
                selected_config = benchmark_configs[choice_idx]
                break
            else:
                print(f"올바른 선택지를 입력하세요 (1-{len(benchmark_configs)})")
        except (ValueError, KeyboardInterrupt):
            print("프로그램을 종료합니다.")
            return None
    
    print(f"\n선택된 테스트: {selected_config['name']}")
    print(f"예상 실행 시간: {estimate_runtime(selected_config['record_count'])}")
    print()
    
    # 벤치마크 실행
    start_time = time.time()
    
    benchmark = BTreePerformanceBenchmark(
        order=selected_config['order'], 
        enable_compression=False  # 압축 비활성화
    )
    
    results = benchmark.benchmark_all_id_types(
        record_count=selected_config['record_count'],
        search_sample_size=selected_config['search_sample_size'],
        range_query_count=selected_config['range_query_count']
    )
    
    total_time = time.time() - start_time
    
    if not results:
        print("❌ 벤치마크 실행 중 오류가 발생했습니다.")
        return None
    
    print(f"\n✅ 벤치마크 완료! (총 실행 시간: {total_time:.1f}초)")
    
    # 결과 출력
    benchmark.print_results_table()
    
    return results

def estimate_runtime(record_count: int) -> str:
    """예상 실행 시간 계산"""
    # ID 유형 9개 * 대략적인 시간 계산 (패턴 ID 추가로 9개)
    base_time = record_count * 9 * 0.0001  # 초
    
    # 대용량 데이터의 경우 비선형적 증가 고려
    if record_count >= 1000000:
        base_time *= 1.5  # 메모리 압박으로 인한 추가 시간
    if record_count >= 3000000:
        base_time *= 2.0  # 더 큰 메모리 압박
    if record_count >= 10000000:
        base_time *= 3.0  # 극한 메모리 압박
    
    if base_time < 60:
        return f"약 {base_time:.0f}초"
    elif base_time < 3600:
        return f"약 {base_time/60:.1f}분"
    else:
        return f"약 {base_time/3600:.1f}시간"

def analyze_results(results: List[PerformanceMetrics]):
    """결과 분석 및 인사이트 제공"""
    
    print("\n" + "=" * 80)
    print("📊 성능 분석 결과")
    print("=" * 80)
    
    # 최고/최악 성능 분석
    best_insert = max(results, key=lambda x: x.insert_rate)
    worst_insert = min(results, key=lambda x: x.insert_rate)
    best_search = max(results, key=lambda x: x.search_rate)
    best_compression = min(results, key=lambda x: x.compression_ratio)
    
    print(f"🏆 성능 챔피언:")
    print(f"   삽입 성능: {best_insert.id_type} ({best_insert.insert_rate:,.0f} records/sec)")
    print(f"   검색 성능: {best_search.id_type} ({best_search.search_rate:,.0f} searches/sec)")
    print(f"   압축 효율: {best_compression.id_type} (압축률 {best_compression.compression_ratio:.2f})")
    
    # 성능 차이 분석
    insert_ratio = best_insert.insert_rate / worst_insert.insert_rate
    print(f"\n📈 성능 차이:")
    print(f"   최고 vs 최악 삽입 성능 차이: {insert_ratio:.1f}배")
    print(f"   ({best_insert.id_type} vs {worst_insert.id_type})")
    
    # 실용적 권장사항
    print(f"\n💡 실용적 권장사항:")
    
    if best_insert.id_type == "Sequential ID":
        print(f"   • 최고 성능이 필요하고 순차 ID가 허용되는 경우: Sequential ID")
    
    time_based_ids = [r for r in results if any(x in r.id_type for x in ['UUIDv7', 'ULID', 'KSUID'])]
    if time_based_ids:
        best_time_based = max(time_based_ids, key=lambda x: x.insert_rate)
        print(f"   • 분산 시스템에서 시간 정렬이 필요한 경우: {best_time_based.id_type}")
    
    uuid4_result = next((r for r in results if r.id_type == 'UUIDv4'), None)
    if uuid4_result:
        print(f"   • 완전한 무작위성이 필요한 경우: UUIDv4 (성능 희생 각오)")
    
    # 압축 효과 분석
    compression_results = sorted(results, key=lambda x: x.compression_ratio)
    print(f"\n🗜️ 압축 효과 순위:")
    for i, result in enumerate(compression_results[:3], 1):
        space_saved_mb = result.space_saved_bytes / (1024 * 1024)
        print(f"   {i}. {result.id_type}: 압축률 {result.compression_ratio:.2f} "
              f"({space_saved_mb:.1f}MB 절약)")

def main():
    """메인 실행 함수"""
    
    print("B-tree ID 성능 벤치마크 및 시각화 도구")
    print()
    
    # 1. 벤치마크 실행
    results = run_comprehensive_id_benchmark()
    
    if not results:
        return
    
    # 2. 결과 분석
    analyze_results(results)
    
    # 3. 시각화 생성 여부 확인
    print(f"\n" + "=" * 80)
    print("📊 결과 시각화")
    print("=" * 80)
    
    create_charts = input("성능 차트를 생성하시겠습니까? (y/N): ").lower().strip()
    
    if create_charts in ['y', 'yes', '예', 'ㅇ']:
        try:
            print("\n차트 생성 중...")
            
            visualizer = PerformanceVisualizer(results)
            
            # 종합 대시보드 생성
            print("1. 종합 성능 대시보드 생성 중...")
            visualizer.create_comprehensive_dashboard('btree_performance_dashboard.png')
            
            # 상세 분석 차트 생성
            print("2. 상세 분석 차트 생성 중...")  
            visualizer.create_detailed_comparison('btree_detailed_analysis.png')
            
            print("\n✅ 시각화 완료!")
            print("생성된 파일:")
            print("  📊 btree_performance_dashboard.png - 종합 성능 대시보드")
            print("  📈 btree_detailed_analysis.png - 상세 분석 차트")
            
        except Exception as e:
            print(f"❌ 차트 생성 중 오류 발생: {e}")
            print("차트 없이 벤치마크 결과만 사용하세요.")
    
    # 4. CSV 결과 저장
    save_csv = input("\n결과를 CSV 파일로 저장하시겠습니까? (y/N): ").lower().strip()
    
    if save_csv in ['y', 'yes', '예', 'ㅇ']:
        try:
            import pandas as pd
            
            # DataFrame 생성
            data = []
            for result in results:
                data.append({
                    'ID_Type': result.id_type,
                    'Records': result.record_count,
                    'Insert_Rate_per_sec': result.insert_rate,
                    'Search_Rate_per_sec': result.search_rate,
                    'Range_Query_Rate_per_sec': result.range_query_rate,
                    'Memory_Usage_MB': result.memory_usage_mb,
                    'Tree_Height': result.tree_height,
                    'Leaf_Pages': result.leaf_pages,
                    'Internal_Pages': result.internal_pages,
                    'Compression_Ratio': result.compression_ratio,
                    'Space_Saved_Bytes': result.space_saved_bytes,
                    'Estimated_Page_Splits': result.estimated_splits,
                    'Avg_ID_Length': result.avg_id_length,
                    'Description': result.id_description
                })
            
            df = pd.DataFrame(data)
            filename = f'btree_benchmark_results_{time.strftime("%Y%m%d_%H%M%S")}.csv'
            df.to_csv(filename, index=False)
            
            print(f"✅ 결과가 {filename}에 저장되었습니다.")
            
        except ImportError:
            print("❌ pandas가 설치되지 않아 CSV 저장을 할 수 없습니다.")
        except Exception as e:
            print(f"❌ CSV 저장 중 오류 발생: {e}")
    
    print(f"\n🎉 B-tree ID 성능 분석이 완료되었습니다!")
    print("결과를 참고하여 프로젝트에 최적화된 ID 유형을 선택하세요.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n프로그램이 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류가 발생했습니다: {e}")
        import traceback
        traceback.print_exc()