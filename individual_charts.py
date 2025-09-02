#!/usr/bin/env python3
"""
Individual Chart Generator for B-tree Performance Analysis

Breaks down the comprehensive dashboard into individual charts
for detailed examination of each performance aspect.
"""

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from typing import List
from performance_benchmark import PerformanceMetrics
from performance_visualizer import PerformanceVisualizer
import os

class IndividualChartGenerator:
    """개별 차트 생성기"""
    
    def __init__(self, results: List[PerformanceMetrics]):
        self.visualizer = PerformanceVisualizer(results)
        self.results = results
        self.df = self.visualizer.df
        self.colors = self.visualizer.colors
    
    def generate_all_individual_charts(self, output_dir: str = "individual_charts"):
        """모든 개별 차트를 생성"""
        # 출력 디렉토리 생성
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        print(f"🎨 Generating individual charts in '{output_dir}' directory...")
        
        # 1. Insert Performance Chart
        self._create_insert_performance_chart(f"{output_dir}/01_insert_performance.png")
        
        # 2. Search Performance Chart
        self._create_search_performance_chart(f"{output_dir}/02_search_performance.png")
        
        # 3. Range Query Performance Chart
        self._create_range_query_performance_chart(f"{output_dir}/03_range_query_performance.png")
        
        # 4. Memory Usage vs Tree Height Chart
        self._create_memory_vs_height_chart(f"{output_dir}/04_memory_vs_height.png")
        
        # 5. Compression Efficiency Chart
        self._create_compression_efficiency_chart(f"{output_dir}/05_compression_efficiency.png")
        
        # 6. Page Splits Chart
        self._create_page_splits_chart(f"{output_dir}/06_page_splits.png")
        
        # 7. Performance Radar Chart
        self._create_performance_radar_chart(f"{output_dir}/07_performance_radar.png")
        
        # 8. Page Split Analysis Chart (NEW)
        self._create_page_split_analysis_chart(f"{output_dir}/08_page_split_analysis.png")
        
        # 9. Insertion Pattern Impact Chart (NEW)
        self._create_insertion_pattern_impact_chart(f"{output_dir}/09_insertion_pattern_impact.png")
        
    # 10. Tree Structure Efficiency Chart (NEW)
        self._create_tree_structure_efficiency_chart(f"{output_dir}/10_tree_structure_efficiency.png")
        
        # 11. Performance Heatmap
        self._create_performance_heatmap_chart(f"{output_dir}/11_performance_heatmap.png")

        # 12. Overall Performance Score by ID Type
        self._create_overall_performance_score_chart(f"{output_dir}/12_overall_performance_score.png")

        # 13. Performance Recommendations (text summary)
        self._create_performance_recommendation_chart(f"{output_dir}/13_performance_recommendations.png")

        print(f"✅ All 13 individual charts generated successfully!")
        print(f"📁 Charts saved in: {os.path.abspath(output_dir)}")
    
    def _create_insert_performance_chart(self, save_path: str):
        """삽입 성능 개별 차트"""
        fig, ax = plt.subplots(figsize=(12, 8))
        self.visualizer._plot_insert_performance(ax)
        plt.title('B-tree Insert Performance by ID Type', fontsize=16, fontweight='bold', pad=20)
        plt.tight_layout()
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"  📊 1/11: Insert performance chart saved")
    
    def _create_search_performance_chart(self, save_path: str):
        """검색 성능 개별 차트"""
        fig, ax = plt.subplots(figsize=(12, 8))
        self.visualizer._plot_search_performance(ax)
        plt.title('B-tree Search Performance by ID Type', fontsize=16, fontweight='bold', pad=20)
        plt.tight_layout()
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"  🔍 2/11: Search performance chart saved")
    
    def _create_range_query_performance_chart(self, save_path: str):
        """범위 쿼리 성능 개별 차트"""
        fig, ax = plt.subplots(figsize=(12, 8))
        self.visualizer._plot_range_query_performance(ax)
        plt.title('B-tree Range Query Performance by ID Type', fontsize=16, fontweight='bold', pad=20)
        plt.tight_layout()
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"  📍 3/11: Range query performance chart saved")
    
    def _create_memory_vs_height_chart(self, save_path: str):
        """메모리 사용량 vs 트리 높이 개별 차트"""
        fig, ax = plt.subplots(figsize=(12, 8))
        self.visualizer._plot_memory_vs_height(ax)
        plt.title('Memory Usage vs Tree Height Analysis', fontsize=16, fontweight='bold', pad=20)
        plt.tight_layout()
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"  💾 4/11: Memory vs height chart saved")
    
    def _create_compression_efficiency_chart(self, save_path: str):
        """압축 효율성 개별 차트"""
        fig, ax = plt.subplots(figsize=(12, 8))
        self.visualizer._plot_compression_efficiency(ax)
        plt.title('Compression Efficiency by ID Type', fontsize=16, fontweight='bold', pad=20)
        plt.tight_layout()
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"  🗜️ 5/11: Compression efficiency chart saved")
    
    def _create_page_splits_chart(self, save_path: str):
        """페이지 분할 개별 차트"""
        fig, ax = plt.subplots(figsize=(12, 8))
        self.visualizer._plot_page_splits(ax)
        plt.title('Estimated Page Splits by ID Type', fontsize=16, fontweight='bold', pad=20)
        plt.tight_layout()
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"  📄 6/11: Page splits chart saved")
    
    def _create_performance_radar_chart(self, save_path: str):
        """성능 레이더 개별 차트"""
        fig, ax = plt.subplots(figsize=(10, 10), subplot_kw=dict(projection='polar'))
        self.visualizer._plot_performance_radar(ax)
        plt.title('Performance Radar Chart (Top 4 ID Types)', fontsize=16, fontweight='bold', y=1.05)
        plt.tight_layout()
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"  🕸️ 7/11: Performance radar chart saved")
    
    def _create_page_split_analysis_chart(self, save_path: str):
        """페이지 분할 분석 개별 차트"""
        fig, ax = plt.subplots(figsize=(12, 8))
        self.visualizer._plot_page_split_analysis(ax)
        plt.title('Page Split Pattern Analysis - Multiplier vs Sequential Baseline', 
                 fontsize=16, fontweight='bold', pad=20)
        plt.tight_layout()
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"  ⚡ 8/11: Page split analysis chart saved")
    
    def _create_insertion_pattern_impact_chart(self, save_path: str):
        """삽입 패턴 영향 개별 차트"""
        fig, ax = plt.subplots(figsize=(12, 8))
        self.visualizer._plot_insertion_pattern_impact(ax)
        plt.title('Insertion Pattern Impact on B-tree Performance', 
                 fontsize=16, fontweight='bold', pad=20)
        plt.tight_layout()
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"  🎯 9/11: Insertion pattern impact chart saved")
    
    def _create_tree_structure_efficiency_chart(self, save_path: str):
        """트리 구조 효율성 개별 차트"""
        fig, ax = plt.subplots(figsize=(12, 8))
        self.visualizer._plot_tree_structure_efficiency(ax)
        plt.title('Tree Structure Efficiency Analysis - Height vs Performance Score', 
                 fontsize=16, fontweight='bold', pad=20)
        plt.tight_layout()
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"  🌳 10/11: Tree structure efficiency chart saved")
    
    def _create_performance_heatmap_chart(self, save_path: str):
        """성능 히트맵 개별 차트"""
        fig, ax = plt.subplots(figsize=(14, 8))
        self.visualizer._plot_performance_heatmap(ax)
        plt.title('Overall Performance Score Heatmap by ID Type', 
                 fontsize=16, fontweight='bold', pad=20)
        plt.tight_layout()
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"  🔥 11/11: Performance heatmap chart saved")

    def _create_overall_performance_score_chart(self, save_path: str):
        """Overall performance score bar chart"""
        fig, ax = plt.subplots(figsize=(12, 8))
        # Reuse the detailed comparison's total score logic
        df_score = self.df.copy()
        df_score['Total_Score'] = (
            (df_score['Insert_Rate'] / df_score['Insert_Rate'].max()) * 0.4 +
            (df_score['Search_Rate'] / df_score['Search_Rate'].max()) * 0.3 +
            (df_score['Range_Query_Rate'] / df_score['Range_Query_Rate'].max()) * 0.2 +
            (1 - df_score['Memory_MB'] / df_score['Memory_MB'].max()) * 0.1
        ) * 100

        df_sorted = df_score.sort_values('Total_Score', ascending=True)
        colors = [self.colors.get(id_type, '#808080') for id_type in df_sorted['ID_Type']]
        bars = ax.barh(df_sorted['ID_Type'], df_sorted['Total_Score'], color=colors)
        ax.set_xlabel('Overall Performance Score')
        ax.set_title('Overall Performance Score by ID Type', fontweight='bold')
        ax.grid(axis='x', alpha=0.3)

        for bar, score in zip(bars, df_sorted['Total_Score']):
            ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2, f'{score:.1f}', va='center', fontsize=9)

        plt.tight_layout()
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"  🏆 12/13: Overall performance score chart saved")

    def _create_performance_recommendation_chart(self, save_path: str):
        """Performance recommendation text rendered as image"""
        fig, ax = plt.subplots(figsize=(10, 8))
        ax.axis('off')

        # Produce recommendations text from visualizer data
        best_insert = self.df.loc[self.df['Insert_Rate'].idxmax(), 'ID_Type']
        best_search = self.df.loc[self.df['Search_Rate'].idxmax(), 'ID_Type']
        best_compression = self.df.loc[self.df['Compression_Ratio'].idxmin(), 'ID_Type']
        best_memory = self.df.loc[self.df['Memory_MB'].idxmin(), 'ID_Type']

        recommendations = (
            f"Performance Recommendations:\n\n"
            f"Best Insert Performance: {best_insert}\n"
            f"Best Search Performance: {best_search}\n"
            f"Best Compression Efficiency: {best_compression}\n"
            f"Minimum Memory Usage: {best_memory}\n\n"
            "Usage Guidelines:\n"
            f"- High-volume inserts: {best_insert}\n"
            f"- Fast search queries: {best_search}\n"
            f"- Storage optimized: {best_compression}\n"
            f"- Memory constrained: {best_memory}\n"
        )

        ax.text(0.01, 0.99, recommendations, transform=ax.transAxes, fontsize=12, va='top', family='monospace')
        plt.tight_layout()
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"  💡 13/13: Performance recommendation image saved")
    
    def create_summary_report(self, output_dir: str = "individual_charts"):
        """차트 요약 리포트 생성"""
        summary_path = f"{output_dir}/README.md"
        
        with open(summary_path, 'w', encoding='utf-8') as f:
            f.write("""# B-tree ID Performance Analysis - Individual Charts

This directory contains 11 individual charts extracted from the comprehensive dashboard.

## Chart Index

### Core Performance Metrics
1. **`01_insert_performance.png`** - Insert rate comparison (records/sec)
2. **`02_search_performance.png`** - Search rate comparison (searches/sec)
3. **`03_range_query_performance.png`** - Range query performance (queries/sec)

### Resource Usage Analysis
4. **`04_memory_vs_height.png`** - Memory usage vs tree height scatter plot
5. **`05_compression_efficiency.png`** - Compression ratio comparison (lower = better)
6. **`06_page_splits.png`** - Estimated page split counts

### Advanced Analysis
7. **`07_performance_radar.png`** - Multi-dimensional performance radar (top 4)
8. **`08_page_split_analysis.png`** - Page split multiplier vs sequential baseline
9. **`09_insertion_pattern_impact.png`** - Impact of insertion patterns on performance
10. **`10_tree_structure_efficiency.png`** - Tree height vs efficiency score
11. **`11_performance_heatmap.png`** - Overall performance score matrix

## Key Insights

### Page Split Impact
- **Sequential ID**: ~1.0x splits (baseline, best performance)
- **Time-based IDs**: ~1.5x splits (UUIDv7, ULID, KSUID)
- **Pattern IDs**: ~1.3x splits (Prefixed strings)
- **Random IDs**: ~2.5x splits (UUIDv4, Random strings)

### Performance Categories
- **🟢 Best**: Sequential ID (ordered insertion)
- **🔵 Good**: Time-based IDs (semi-ordered)
- **🟡 Moderate**: Pattern-based IDs (prefix-ordered)
- **🔴 Worst**: Random IDs (unordered insertion)

### Usage Recommendations
- **High-volume inserts**: Sequential ID
- **Distributed systems**: UUIDv7/ULID
- **Storage optimization**: Pattern-based IDs
- **Complete randomness needed**: UUIDv4 (with performance cost)

---
Generated by B-tree Performance Analysis Tool
""")
        
        print(f"📝 Summary report saved: {os.path.abspath(summary_path)}")


def main():
    """개별 차트 생성 메인 함수"""
    print("🎨 B-tree Individual Chart Generator")
    print("=" * 50)
    
    # 기존 결과가 있는지 확인
    try:
        from run_complete_benchmark import run_comprehensive_id_benchmark
        
        print("📊 Running benchmark to generate fresh data...")
        results = run_comprehensive_id_benchmark()
        
        if not results:
            print("❌ No benchmark results available. Please run a benchmark first.")
            return
        
        # 개별 차트 생성
        chart_generator = IndividualChartGenerator(results)
        chart_generator.generate_all_individual_charts()
        chart_generator.create_summary_report()
        
        print("\n🎉 Individual chart generation completed!")
        print("💡 View each chart separately for detailed analysis.")
        
    except Exception as e:
        print(f"❌ Error generating charts: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()