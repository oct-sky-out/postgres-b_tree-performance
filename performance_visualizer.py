"""
B-tree ID 성능 벤치마크 시각화 모듈

성능 차이를 명확하게 보여주는 차트와 그래프를 생성합니다.
- 삽입 성능 비교
- 검색 성능 비교  
- 메모리 사용량 비교
- 트리 구조 특성 비교
- 압축 효율성 비교
"""

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from typing import List
from performance_benchmark import PerformanceMetrics, BTreePerformanceBenchmark
import warnings
warnings.filterwarnings('ignore')

# English font settings
plt.rcParams['font.family'] = ['DejaVu Sans', 'Arial', 'sans-serif']
plt.rcParams['axes.unicode_minus'] = False

class PerformanceVisualizer:
    """성능 벤치마크 결과 시각화 클래스"""
    
    def __init__(self, results: List[PerformanceMetrics]):
        self.results = results
        self.df = self._create_dataframe()
        
        # 색상 팔레트 설정 (ID 유형별)
        self.colors = {
            'Sequential ID': '#2E8B57',           # 초록 (최고 성능)
            'UUIDv7': '#4169E1',                 # 파랑 (우수)
            'ULID': '#FF6347',                   # 토마토색 (우수)
            'KSUID': '#FFD700',                  # 금색 (우수)
            'Random String (10-30)': '#DA70D6',   # 자주색 (가변)
            'Random String (10-20)': '#DDA0DD',   # 연한 자주색 (가변)
            'Prefixed String (USER)': '#32CD32',  # 라임색 (패턴)
            'Prefixed String (ORD)': '#98FB98',   # 연한 라임색 (패턴)
            'UUIDv4': '#DC143C',                 # 빨강 (최악)
        }
    
    def _create_dataframe(self) -> pd.DataFrame:
        """벤치마크 결과를 DataFrame으로 변환"""
        data = []
        for result in self.results:
            data.append({
                'ID_Type': result.id_type,
                'Insert_Rate': result.insert_rate,
                'Search_Rate': result.search_rate,
                'Range_Query_Rate': result.range_query_rate,
                'Memory_MB': result.memory_usage_mb,
                'Tree_Height': result.tree_height,
                'Compression_Ratio': result.compression_ratio,
                'Space_Saved_MB': result.space_saved_bytes / (1024 * 1024),
                'Estimated_Splits': result.estimated_splits,
                'Avg_ID_Length': result.avg_id_length,
                'Record_Count': result.record_count,
                'Insert_Time': result.insert_time,
                'Search_Time': result.search_time
            })
        
        return pd.DataFrame(data)
    
    def create_comprehensive_dashboard(self, save_path: str = None):
        """종합 성능 대시보드 생성"""
        fig = plt.figure(figsize=(24, 20))
        gs = fig.add_gridspec(5, 3, hspace=0.3, wspace=0.3)
        
        # 1. 삽입 성능 비교 (막대 그래프)
        ax1 = fig.add_subplot(gs[0, 0])
        self._plot_insert_performance(ax1)
        
        # 2. 검색 성능 비교 (막대 그래프)
        ax2 = fig.add_subplot(gs[0, 1])
        self._plot_search_performance(ax2)
        
        # 3. 범위 쿼리 성능 비교 (막대 그래프)
        ax3 = fig.add_subplot(gs[0, 2])
        self._plot_range_query_performance(ax3)
        
        # 4. 메모리 사용량 vs 트리 높이 (산점도)
        ax4 = fig.add_subplot(gs[1, 0])
        self._plot_memory_vs_height(ax4)
        
        # 5. 압축 효율성 비교 (막대 그래프)
        ax5 = fig.add_subplot(gs[1, 1])
        self._plot_compression_efficiency(ax5)
        
        # 6. 페이지 분할 비교 (막대 그래프)
        ax6 = fig.add_subplot(gs[1, 2])
        self._plot_page_splits(ax6)
        
        # 7. 전체 성능 레이더 차트 (상위 4개 ID 유형)
        ax7 = fig.add_subplot(gs[2, :], projection='polar')
        self._plot_performance_radar(ax7)
        
        # 8. 페이지 분할 패턴 분석 (새로운 차트)
        ax8 = fig.add_subplot(gs[3, 0])
        self._plot_page_split_analysis(ax8)
        
        # 9. 삽입 순서별 성능 영향 (새로운 차트)
        ax9 = fig.add_subplot(gs[3, 1])
        self._plot_insertion_pattern_impact(ax9)
        
        # 10. 트리 구조 효율성 (새로운 차트)
        ax10 = fig.add_subplot(gs[3, 2])
        self._plot_tree_structure_efficiency(ax10)
        
        # 11. 성능 종합 점수 (히트맵)
        ax11 = fig.add_subplot(gs[4, :])
        self._plot_performance_heatmap(ax11)
        
        plt.suptitle('B-tree ID Performance Comprehensive Analysis\nPage Split Patterns and Insertion Order Impact', fontsize=20, y=0.96)
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.show()
        
        return fig
    
    def _plot_insert_performance(self, ax):
        """삽입 성능 차트"""
        df_sorted = self.df.sort_values('Insert_Rate', ascending=True)
        colors = [self.colors.get(id_type, '#808080') for id_type in df_sorted['ID_Type']]
        
        bars = ax.barh(df_sorted['ID_Type'], df_sorted['Insert_Rate'], color=colors)
        ax.set_xlabel('Insert Rate (records/sec)')
        ax.set_title('Insert Performance by ID Type', fontweight='bold')
        
        # 수치 표시
        for bar, rate in zip(bars, df_sorted['Insert_Rate']):
            ax.text(bar.get_width() + max(df_sorted['Insert_Rate']) * 0.01, 
                   bar.get_y() + bar.get_height()/2, 
                   f'{rate:,.0f}', va='center', fontsize=9)
        
        ax.grid(axis='x', alpha=0.3)
    
    def _plot_search_performance(self, ax):
        """검색 성능 차트"""
        df_sorted = self.df.sort_values('Search_Rate', ascending=True)
        colors = [self.colors.get(id_type, '#808080') for id_type in df_sorted['ID_Type']]
        
        bars = ax.barh(df_sorted['ID_Type'], df_sorted['Search_Rate'], color=colors)
        ax.set_xlabel('Search Rate (searches/sec)')
        ax.set_title('Search Performance by ID Type', fontweight='bold')
        
        for bar, rate in zip(bars, df_sorted['Search_Rate']):
            ax.text(bar.get_width() + max(df_sorted['Search_Rate']) * 0.01, 
                   bar.get_y() + bar.get_height()/2, 
                   f'{rate:,.0f}', va='center', fontsize=9)
        
        ax.grid(axis='x', alpha=0.3)
    
    def _plot_range_query_performance(self, ax):
        """범위 쿼리 성능 차트"""
        df_sorted = self.df.sort_values('Range_Query_Rate', ascending=True)
        colors = [self.colors.get(id_type, '#808080') for id_type in df_sorted['ID_Type']]
        
        bars = ax.barh(df_sorted['ID_Type'], df_sorted['Range_Query_Rate'], color=colors)
        ax.set_xlabel('Range Query Rate (queries/sec)')
        ax.set_title('Range Query Performance by ID Type', fontweight='bold')
        
        for bar, rate in zip(bars, df_sorted['Range_Query_Rate']):
            ax.text(bar.get_width() + max(df_sorted['Range_Query_Rate']) * 0.01, 
                   bar.get_y() + bar.get_height()/2, 
                   f'{rate:.1f}', va='center', fontsize=9)
        
        ax.grid(axis='x', alpha=0.3)
    
    def _plot_memory_vs_height(self, ax):
        """메모리 사용량 vs 트리 높이 산점도"""
        for _, row in self.df.iterrows():
            color = self.colors.get(row['ID_Type'], '#808080')
            ax.scatter(row['Memory_MB'], row['Tree_Height'], 
                      c=color, s=100, alpha=0.7, label=row['ID_Type'])
            ax.annotate(row['ID_Type'], (row['Memory_MB'], row['Tree_Height']),
                       xytext=(5, 5), textcoords='offset points', fontsize=8)
        
        ax.set_xlabel('Memory Usage (MB)')
        ax.set_ylabel('Tree Height')
        ax.set_title('Memory Usage vs Tree Height', fontweight='bold')
        ax.grid(alpha=0.3)
    
    def _plot_compression_efficiency(self, ax):
        """압축 효율성 차트"""
        df_sorted = self.df.sort_values('Compression_Ratio', ascending=True)
        colors = [self.colors.get(id_type, '#808080') for id_type in df_sorted['ID_Type']]
        
        bars = ax.barh(df_sorted['ID_Type'], df_sorted['Compression_Ratio'], color=colors)
        ax.set_xlabel('Compression Ratio (lower is better)')
        ax.set_title('Compression Efficiency by ID Type', fontweight='bold')
        
        for bar, ratio in zip(bars, df_sorted['Compression_Ratio']):
            ax.text(bar.get_width() + 0.01, 
                   bar.get_y() + bar.get_height()/2, 
                   f'{ratio:.2f}', va='center', fontsize=9)
        
        ax.grid(axis='x', alpha=0.3)
        ax.axvline(x=1.0, color='red', linestyle='--', alpha=0.5, label='No compression')
    
    def _plot_page_splits(self, ax):
        """페이지 분할 횟수 차트"""
        df_sorted = self.df.sort_values('Estimated_Splits', ascending=True)
        colors = [self.colors.get(id_type, '#808080') for id_type in df_sorted['ID_Type']]
        
        bars = ax.barh(df_sorted['ID_Type'], df_sorted['Estimated_Splits'], color=colors)
        ax.set_xlabel('Estimated Page Splits')
        ax.set_title('Page Splits by ID Type', fontweight='bold')
        
        for bar, splits in zip(bars, df_sorted['Estimated_Splits']):
            ax.text(bar.get_width() + max(df_sorted['Estimated_Splits']) * 0.01, 
                   bar.get_y() + bar.get_height()/2, 
                   f'{splits:,}', va='center', fontsize=9)
        
        ax.grid(axis='x', alpha=0.3)
    
    def _plot_performance_radar(self, ax):
        """성능 레이더 차트 (상위 4개 ID 유형)"""
        # 상위 4개 ID 유형 선택 (삽입 성능 기준)
        top_4 = self.df.nlargest(4, 'Insert_Rate')
        
        # 성능 지표 정규화 (0-1 범위)
        metrics = ['Insert_Rate', 'Search_Rate', 'Range_Query_Rate']
        labels = ['Insert Performance', 'Search Performance', 'Range Query Performance']
        
        # 각 지표를 0-1로 정규화
        normalized_data = {}
        for metric in metrics:
            max_val = self.df[metric].max()
            min_val = self.df[metric].min()
            for _, row in top_4.iterrows():
                if row['ID_Type'] not in normalized_data:
                    normalized_data[row['ID_Type']] = []
                normalized_val = (row[metric] - min_val) / (max_val - min_val)
                normalized_data[row['ID_Type']].append(normalized_val)
        
        # 레이더 차트 그리기
        angles = np.linspace(0, 2 * np.pi, len(labels), endpoint=False).tolist()
        angles += angles[:1]  # 닫힌 다각형을 위해
        
        for id_type, values in normalized_data.items():
            values += values[:1]  # 닫힌 다각형을 위해
            color = self.colors.get(id_type, '#808080')
            ax.plot(angles, values, 'o-', linewidth=2, label=id_type, color=color)
            ax.fill(angles, values, alpha=0.25, color=color)
        
        ax.set_xticks(angles[:-1])
        ax.set_xticklabels(labels)
        ax.set_ylim(0, 1)
        ax.set_title('Performance Radar Chart (Top 4 ID Types)', fontweight='bold', y=1.08)
        ax.legend(loc='upper right', bbox_to_anchor=(1.3, 1.0))
        ax.grid(True)
    
    def _plot_page_split_analysis(self, ax):
        """페이지 분할 패턴 분석 차트"""
        # ID 유형별 페이지 분할 배수 계산
        df_analysis = self.df.copy()
        
        # 순차적 삽입 대비 페이지 분할 배수
        sequential_splits = df_analysis[df_analysis['ID_Type'] == 'Sequential ID']['Estimated_Splits'].iloc[0]
        df_analysis['Split_Multiplier'] = df_analysis['Estimated_Splits'] / sequential_splits
        
        # 데이터 준비
        split_data = []
        colors_list = []
        
        for _, row in df_analysis.iterrows():
            split_data.append(row['Split_Multiplier'])
            colors_list.append(self.colors.get(row['ID_Type'], '#808080'))
        
        # 방사형 차트 생성
        bars = ax.barh(df_analysis['ID_Type'], split_data, color=colors_list)
        
        # 기준선 추가 (Sequential ID = 1.0)
        ax.axvline(x=1.0, color='green', linestyle='--', linewidth=2, 
                  alpha=0.7, label='Sequential Baseline')
        ax.axvline(x=2.0, color='orange', linestyle='--', linewidth=1, 
                  alpha=0.7, label='2x More Splits')
        ax.axvline(x=3.0, color='red', linestyle='--', linewidth=1, 
                  alpha=0.7, label='3x More Splits')
        
        # 라벨 및 제목
        ax.set_xlabel('Page Split Multiplier (vs Sequential)')
        ax.set_title('Page Split Pattern Analysis', fontweight='bold')
        ax.legend(loc='lower right')
        
        # 수치 표시
        for bar, multiplier in zip(bars, split_data):
            ax.text(bar.get_width() + 0.05, bar.get_y() + bar.get_height()/2, 
                   f'{multiplier:.1f}x', va='center', fontsize=9, fontweight='bold')
        
        ax.grid(axis='x', alpha=0.3)
        ax.set_xlim(0, max(split_data) * 1.1)
    
    def _plot_insertion_pattern_impact(self, ax):
        """삽입 순서별 성능 영향 분석"""
        # ID 유형을 삽입 패턴별로 분류
        pattern_categories = {
            'Sequential\\n(Ordered)': ['Sequential ID'],
            'Time-Based\\n(Semi-ordered)': ['UUIDv7', 'ULID', 'KSUID'],
            'Pattern-Based\\n(Prefix-ordered)': [id_type for id_type in self.df['ID_Type'] 
                                               if 'Prefixed' in id_type],
            'Random\\n(Unordered)': ['UUIDv4'] + [id_type for id_type in self.df['ID_Type'] 
                                            if 'Random String' in id_type]
        }
        
        # 각 카테고리별 평균 성능 계산
        category_performance = {}
        
        for category, id_types in pattern_categories.items():
            category_data = self.df[self.df['ID_Type'].isin(id_types)]
            if not category_data.empty:
                avg_insert = category_data['Insert_Rate'].mean()
                avg_splits = category_data['Estimated_Splits'].mean()
                category_performance[category] = {'insert_rate': avg_insert, 'splits': avg_splits}
        
        # 데이터 추출
        categories = list(category_performance.keys())
        insert_rates = [category_performance[cat]['insert_rate'] for cat in categories]
        
        # 색상 선택
        category_colors = ['#2E8B57', '#4169E1', '#FFD700', '#DC143C']  # 순서대로
        
        # 막대 그래프
        bars = ax.bar(categories, insert_rates, color=category_colors, alpha=0.7)
        
        ax.set_ylabel('Average Insert Rate (records/sec)')
        ax.set_title('Insertion Pattern Impact on Performance', fontweight='bold')
        
        # 수치 표시
        for bar, rate in zip(bars, insert_rates):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(insert_rates)*0.01,
                   f'{rate:,.0f}', ha='center', va='bottom', fontsize=9, fontweight='bold')
        
        ax.tick_params(axis='x', rotation=45)
        ax.grid(axis='y', alpha=0.3)
    
    def _plot_tree_structure_efficiency(self, ax):
        """트리 구조 효율성 분석"""
        # 효율성 지표 계산: 높이 대비 성능
        df_efficiency = self.df.copy()
        df_efficiency['Height_Efficiency'] = df_efficiency['Insert_Rate'] / df_efficiency['Tree_Height']
        
        # 개선된 Structure Score 계산
        insert_norm = df_efficiency['Insert_Rate'] / df_efficiency['Insert_Rate'].max()
        search_norm = df_efficiency['Search_Rate'] / df_efficiency['Search_Rate'].max()
        
        # 높이 정규화: 범위가 있을 때만 계산
        height_range = df_efficiency['Tree_Height'].max() - df_efficiency['Tree_Height'].min()
        if height_range > 0:
            height_norm = (df_efficiency['Tree_Height'].max() - df_efficiency['Tree_Height']) / height_range
        else:
            height_norm = 0.5  # 모든 높이가 같으면 중간값
        
        # 페이지 분할 정규화
        split_range = df_efficiency['Estimated_Splits'].max() - df_efficiency['Estimated_Splits'].min()
        if split_range > 0:
            split_norm = (df_efficiency['Estimated_Splits'].max() - df_efficiency['Estimated_Splits']) / split_range
        else:
            split_norm = 0.5  # 모든 분할이 같으면 중간값
        
        df_efficiency['Structure_Score'] = (
            insert_norm * 0.3 +      # 삽입 성능
            search_norm * 0.3 +      # 검색 성능  
            height_norm * 0.2 +      # 트리 높이 (낮을수록 좋음)
            split_norm * 0.2         # 페이지 분할 (적을수록 좋음)
        ) * 100
        
        # 산점도 그래프
        for _, row in df_efficiency.iterrows():
            color = self.colors.get(row['ID_Type'], '#808080')
            ax.scatter(row['Tree_Height'], row['Structure_Score'], 
                      c=color, s=120, alpha=0.7, edgecolors='black', linewidth=1)
            
            # ID 유형 라벨 추가
            ax.annotate(row['ID_Type'], (row['Tree_Height'], row['Structure_Score']),
                       xytext=(5, 5), textcoords='offset points', fontsize=8)
        
        ax.set_xlabel('Tree Height')
        ax.set_ylabel('Structure Efficiency Score')
        ax.set_title('Tree Structure Efficiency Analysis', fontweight='bold')
        
        # 추세선 추가 (높이가 낮을수록 좋음)
        z = np.polyfit(df_efficiency['Tree_Height'], df_efficiency['Structure_Score'], 1)
        p = np.poly1d(z)
        ax.plot(df_efficiency['Tree_Height'], p(df_efficiency['Tree_Height']), 
               "r--", alpha=0.5, label='Efficiency Trend')
        
        ax.legend()
        ax.grid(alpha=0.3)
    
    def _plot_performance_heatmap(self, ax):
        """성능 종합 점수 히트맵"""
        # 성능 점수 계산 (높을수록 좋음)
        df_score = self.df.copy()
        
        # 각 지표별 점수 계산 (0-100 범위)
        df_score['Insert_Score'] = (df_score['Insert_Rate'] / df_score['Insert_Rate'].max()) * 100
        df_score['Search_Score'] = (df_score['Search_Rate'] / df_score['Search_Rate'].max()) * 100
        df_score['Range_Score'] = (df_score['Range_Query_Rate'] / df_score['Range_Query_Rate'].max()) * 100
        df_score['Memory_Score'] = (1 - df_score['Memory_MB'] / df_score['Memory_MB'].max()) * 100
        
        # Tree Structure Score 개선: 높이가 낮을수록 높은 점수
        height_range = df_score['Tree_Height'].max() - df_score['Tree_Height'].min()
        if height_range > 0:
            df_score['Height_Score'] = ((df_score['Tree_Height'].max() - df_score['Tree_Height']) / height_range) * 100
        else:
            df_score['Height_Score'] = 50  # 모든 높이가 같으면 중간 점수
        
        # Compression Score 개선: 압축률이 낮을수록 높은 점수 (1.0 이하면 좋음)
        # 압축률 범위를 고려한 정규화
        compression_range = df_score['Compression_Ratio'].max() - df_score['Compression_Ratio'].min()
        if compression_range > 0:
            df_score['Compression_Score'] = ((df_score['Compression_Ratio'].max() - df_score['Compression_Ratio']) / compression_range) * 100
        else:
            df_score['Compression_Score'] = 50  # 모든 압축률이 같으면 중간 점수
        
        # 히트맵용 데이터 준비
        score_columns = ['Insert_Score', 'Search_Score', 'Range_Score', 
                        'Memory_Score', 'Height_Score', 'Compression_Score']
        score_labels = ['Insert Perf', 'Search Perf', 'Range Query', 'Memory Eff', 'Tree Structure', 'Compression Eff']
        
        heatmap_data = df_score[['ID_Type'] + score_columns].set_index('ID_Type')
        heatmap_data.columns = score_labels
        
        # 히트맵 그리기 (matplotlib만 사용)
        im = ax.imshow(heatmap_data.values, cmap='RdYlGn', aspect='auto', 
                      vmin=0, vmax=100, interpolation='nearest')
        
        # 컬러바 추가
        cbar = plt.colorbar(im, ax=ax, shrink=0.8)
        cbar.set_label('Performance Score')
        
        # 텍스트 어노테이션 추가
        for i in range(len(heatmap_data.index)):
            for j in range(len(heatmap_data.columns)):
                ax.text(j, i, f'{heatmap_data.iloc[i, j]:.0f}',
                       ha="center", va="center", color="black", fontweight='bold')
        
        # 축 설정
        ax.set_xticks(range(len(heatmap_data.columns)))
        ax.set_xticklabels(heatmap_data.columns)
        ax.set_yticks(range(len(heatmap_data.index)))
        ax.set_yticklabels(heatmap_data.index)
        ax.set_title('Overall Performance Score by ID Type', fontweight='bold')
        ax.set_ylabel('')
        
        # 페이지 분할 정보 추가
        ax.text(0.02, 0.98, 'Page Split Impact:', transform=ax.transAxes, 
               fontweight='bold', va='top')
        ax.text(0.02, 0.94, '• Sequential: Minimal splits (best)', transform=ax.transAxes, 
               fontsize=9, va='top', color='green')
        ax.text(0.02, 0.90, '• Time-based: Moderate splits', transform=ax.transAxes, 
               fontsize=9, va='top', color='blue')
        ax.text(0.02, 0.86, '• Random: Maximum splits (worst)', transform=ax.transAxes, 
               fontsize=9, va='top', color='red')
    
    def create_detailed_comparison(self, save_path: str = None):
        """상세 비교 차트 생성"""
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        axes = axes.flatten()
        
        # 1. 삽입 시간 vs 데이터 크기 (로그 스케일)
        ax = axes[0]
        for _, row in self.df.iterrows():
            color = self.colors.get(row['ID_Type'], '#808080')
            ax.scatter(row['Record_Count'], row['Insert_Time'], 
                      c=color, s=100, alpha=0.7, label=row['ID_Type'])
        ax.set_xlabel('Record Count')
        ax.set_ylabel('Insert Time (seconds)')
        ax.set_title('Insert Time by Data Size')
        ax.set_yscale('log')
        ax.grid(alpha=0.3)
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        
        # 2. ID 길이별 성능 분석
        ax = axes[1]
        ax.scatter(self.df['Avg_ID_Length'], self.df['Insert_Rate'], 
                  c=[self.colors.get(id_type, '#808080') for id_type in self.df['ID_Type']], 
                  s=100, alpha=0.7)
        for _, row in self.df.iterrows():
            ax.annotate(row['ID_Type'], (row['Avg_ID_Length'], row['Insert_Rate']),
                       xytext=(5, 5), textcoords='offset points', fontsize=8)
        ax.set_xlabel('Average ID Length (characters)')
        ax.set_ylabel('Insert Rate (records/sec)')
        ax.set_title('ID Length vs Insert Performance')
        ax.grid(alpha=0.3)
        
        # 3. 압축률 vs 공간 절약량
        ax = axes[2]
        ax.scatter(self.df['Compression_Ratio'], self.df['Space_Saved_MB'],
                  c=[self.colors.get(id_type, '#808080') for id_type in self.df['ID_Type']], 
                  s=100, alpha=0.7)
        for _, row in self.df.iterrows():
            ax.annotate(row['ID_Type'], (row['Compression_Ratio'], row['Space_Saved_MB']),
                       xytext=(5, 5), textcoords='offset points', fontsize=8)
        ax.set_xlabel('Compression Ratio')
        ax.set_ylabel('Space Saved (MB)')
        ax.set_title('Compression Ratio vs Space Savings')
        ax.grid(alpha=0.3)
        
        # 4. 트리 높이 vs 페이지 분할
        ax = axes[3]
        ax.scatter(self.df['Tree_Height'], self.df['Estimated_Splits'],
                  c=[self.colors.get(id_type, '#808080') for id_type in self.df['ID_Type']], 
                  s=100, alpha=0.7)
        for _, row in self.df.iterrows():
            ax.annotate(row['ID_Type'], (row['Tree_Height'], row['Estimated_Splits']),
                       xytext=(5, 5), textcoords='offset points', fontsize=8)
        ax.set_xlabel('Tree Height')
        ax.set_ylabel('Estimated Page Splits')
        ax.set_title('Tree Height vs Page Splits')
        ax.grid(alpha=0.3)
        
        # 5. 종합 성능 점수 비교
        ax = axes[4]
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
        ax.set_title('Overall Performance Score by ID Type')
        ax.grid(axis='x', alpha=0.3)
        
        for bar, score in zip(bars, df_sorted['Total_Score']):
            ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2, 
                   f'{score:.1f}', va='center', fontsize=9)
        
        # 6. 성능 권장 사항
        ax = axes[5]
        ax.axis('off')
        
        # 최고 성능 ID 찾기
        best_insert = self.df.loc[self.df['Insert_Rate'].idxmax(), 'ID_Type']
        best_search = self.df.loc[self.df['Search_Rate'].idxmax(), 'ID_Type']
        best_compression = self.df.loc[self.df['Compression_Ratio'].idxmin(), 'ID_Type']
        best_memory = self.df.loc[self.df['Memory_MB'].idxmin(), 'ID_Type']
        
        recommendations = f"""
Performance Recommendations:

🏆 Best Insert Performance: {best_insert}
🔍 Best Search Performance: {best_search}  
📦 Best Compression Efficiency: {best_compression}
💾 Minimum Memory Usage: {best_memory}

Page Split Analysis:
• Sequential ID: ~1x splits (baseline, ordered insertion)
• Time-based IDs: ~1.5x splits (semi-ordered insertion)
• Pattern IDs: ~1.3x splits (prefix helps with ordering)
• Random IDs: ~2.5x splits (worst case, random insertion)

Recommended Use Cases:
• High-volume inserts: {best_insert}
• Fast search queries: {best_search}
• Storage space critical: {best_compression}
• Memory constrained: {best_memory}

General Recommendations:
• Sequential ID: Best performance when predictable IDs are acceptable
• UUIDv7/ULID: For distributed systems needing time-based sorting
• UUIDv4: When complete randomness is required (performance cost)
"""
        
        ax.text(0.05, 0.95, recommendations, transform=ax.transAxes, 
               fontsize=10, verticalalignment='top', fontfamily='monospace')
        
        plt.tight_layout()
        plt.suptitle('B-tree ID Type Detailed Performance Analysis', fontsize=16, y=0.98)
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.show()
        
        return fig


def run_large_scale_benchmark():
    """대규모 벤치마크 실행 (확연한 성능 차이를 위한 큰 데이터셋)"""
    print("=" * 80)
    print("대규모 B-tree ID 성능 벤치마크 실행")
    print("=" * 80)
    print("주의: 이 테스트는 시간이 오래 걸릴 수 있습니다 (10-30분)")
    print()
    
    # 확연한 성능 차이를 위한 대용량 데이터셋 설정
    benchmark = BTreePerformanceBenchmark(order=100, enable_compression=True)
    
    # 단계적으로 데이터 크기 증가하여 차이를 극명하게 보여줌
    test_sizes = [
        (10000, "소규모 테스트"),
        (50000, "중간 규모 테스트"), 
        (100000, "대규모 테스트"),
    ]
    
    all_results = []
    
    for record_count, test_name in test_sizes:
        print(f"\n{test_name} - {record_count:,}개 레코드")
        print("-" * 50)
        
        results = benchmark.benchmark_all_id_types(
            record_count=record_count,
            search_sample_size=min(5000, record_count // 10),
            range_query_count=min(500, record_count // 100)
        )
        
        all_results.extend(results)
        
        # 중간 결과 출력
        print(f"\n{test_name} 결과 요약:")
        benchmark.print_results_table()
    
    # 최종 결과로 가장 큰 데이터셋 사용
    final_results = [r for r in all_results if r.record_count == max(r.record_count for r in all_results)]
    
    return final_results


def create_performance_report(results: List[PerformanceMetrics]):
    """종합 성능 리포트 생성"""
    visualizer = PerformanceVisualizer(results)
    
    print("성능 시각화 리포트 생성 중...")
    
    # 1. 종합 대시보드 생성
    print("1. 종합 대시보드 생성 중...")
    visualizer.create_comprehensive_dashboard('btree_performance_dashboard.png')
    
    # 2. 상세 비교 차트 생성
    print("2. 상세 비교 차트 생성 중...")
    visualizer.create_detailed_comparison('btree_detailed_comparison.png')
    
    print("성능 리포트 생성 완료!")
    print("생성된 파일:")
    print("- btree_performance_dashboard.png (종합 대시보드)")
    print("- btree_detailed_comparison.png (상세 비교)")


if __name__ == "__main__":
    # 대규모 벤치마크 실행
    results = run_large_scale_benchmark()
    
    if results:
        # 시각화 리포트 생성
        create_performance_report(results)
    else:
        print("벤치마크 결과가 없습니다.")