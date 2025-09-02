#!/usr/bin/env python3
"""
B-tree ID Performance Benchmark CLI

Command-line interface for running B-tree performance benchmarks
with various ID types and data scales.

Usage:
    python benchmark_cli.py [options]
    
Examples:
    python benchmark_cli.py --scale small
    python benchmark_cli.py --scale 1M --charts --csv
    python benchmark_cli.py --scale 10M --no-interactive
"""

import argparse
import sys
import time
from typing import Optional, List
from performance_benchmark import BTreePerformanceBenchmark, PerformanceMetrics
from performance_visualizer import PerformanceVisualizer
from run_complete_benchmark import analyze_results


def main():
    parser = argparse.ArgumentParser(
        description='B-tree ID Performance Benchmark CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Scale options:
    small       2,000 records (quick test, ~18 seconds)
    medium      10,000 records (clear differences, ~90 seconds)
    large       25,000 records (dramatic differences, ~3.75 minutes)
    0.1M       100,000 records (dramatic differences, ~2.25 minutes)
    0.3M       300,000 records (dramatic differences, ~6.75 minutes)
    0.5M        500,000 records (dramatic differences, ~11.25 minutes)
    1M          1,000,000 records (very extreme differences, ~22.5 minutes)
    3M          3,000,000 records (extreme performance gap, ~2.7 hours)
    10M         10,000,000 records (maximum performance gap, ~13.5 hours)

Examples:
    python benchmark_cli.py --scale small
    python benchmark_cli.py --scale 1M --charts --csv
    python benchmark_cli.py --scale large --individual-charts
    python benchmark_cli.py --scale 10M --no-interactive
        """
    )
    
    parser.add_argument(
        '--scale', 
        choices=['small', 'medium', 'large', '0.1M', '0.3M', '0.5M', '1M', '3M', '10M'],
        required=True,
        help='Test scale to run'
    )
    
    parser.add_argument(
        '--charts', 
        action='store_true',
        help='Generate performance charts (comprehensive dashboard)'
    )
    
    parser.add_argument(
        '--individual-charts', 
        action='store_true',
        help='Generate individual charts (11 separate charts)'
    )
    
    parser.add_argument(
        '--csv', 
        action='store_true',
        help='Save results to CSV file'
    )
    
    parser.add_argument(
        '--no-interactive', 
        action='store_true',
        help='Run without interactive prompts'
    )
    
    parser.add_argument(
        '--output-dir', 
        default='.',
        help='Directory to save output files (default: current directory)'
    )
    
    args = parser.parse_args()
    
    # Configuration mapping
    config_map = {
        'small': {
            'name': 'Small Scale Test (Quick Execution)',
            'record_count': 2000,
            'search_sample_size': 200,
            'range_query_count': 20,
            'order': 15,
            'estimated_time': '~18 seconds'
        },
        'medium': {
            'name': 'Medium Scale Test (Clear Performance Differences)',
            'record_count': 10000,
            'search_sample_size': 1000,
            'range_query_count': 100,
            'order': 30,
            'estimated_time': '~90 seconds'
        },
        'large': {
            'name': 'Large Scale Test (Dramatic Performance Differences)',
            'record_count': 25000,
            'search_sample_size': 2000,
            'range_query_count': 200,
            'order': 50,
            'estimated_time': '~3.75 minutes'
        },
        '0.1M': {
            'name': 'Large Scale Test - 0.1M Records (Dramatic Performance Differences)',
            'record_count': 100000,
            'search_sample_size': 1000,
            'range_query_count': 100,
            'order': 50,
            'estimated_time': '~2.25 minutes'
        },
        '0.5M': {
            'name': 'Large Scale Test - 0.5M Records (Dramatic Performance Differences)',
            'record_count': 500000,
            'search_sample_size': 5000,
            'range_query_count': 500,
            'order': 50,
            'estimated_time': '~11.25 minutes'
        },
        '0.3M': {
            'name': 'Large Scale Test - 0.3M Records (Dramatic Performance Differences)',
            'record_count': 300000,
            'search_sample_size': 3000,
            'range_query_count': 300,
            'order': 50,
            'estimated_time': '~6.75 minutes'
        },
        '1M': {
            'name': 'Ultra-Large Scale Test - 1M Records (Very Extreme Differences)',
            'record_count': 1000000,
            'search_sample_size': 10000,
            'range_query_count': 1000,
            'order': 100,
            'estimated_time': '~22.5 minutes'
        },
        '3M': {
            'name': 'Mega-Scale Test - 3M Records (Extreme Performance Gap)',
            'record_count': 3000000,
            'search_sample_size': 30000,
            'range_query_count': 3000,
            'order': 150,
            'estimated_time': '~2.7 hours'
        },
        '10M': {
            'name': 'Giga-Scale Test - 10M Records (Maximum Performance Gap)',
            'record_count': 10000000,
            'search_sample_size': 100000,
            'range_query_count': 10000,
            'order': 200,
            'estimated_time': '~13.5 hours'
        }
    }
    
    config = config_map[args.scale]
    
    print("=" * 80)
    print("🚀 B-tree ID Performance Benchmark CLI")
    print("=" * 80)
    print(f"Selected Test: {config['name']}")
    print(f"Records: {config['record_count']:,}")
    print(f"Estimated Runtime: {config['estimated_time']}")
    print()
    
    # Confirmation for large scales
    if not args.no_interactive and args.scale in ['3M', '10M']:
        print("⚠️  WARNING: This is a very large scale test that will take significant time.")
        print(f"   Estimated runtime: {config['estimated_time']}")
        print("   Make sure you have sufficient system resources available.")
        print()
        
        confirm = input("Do you want to continue? (y/N): ").lower().strip()
        if confirm not in ['y', 'yes']:
            print("Benchmark cancelled.")
            return
        print()
    
    # Run benchmark
    print("Starting benchmark...")
    start_time = time.time()
    
    benchmark = BTreePerformanceBenchmark(
        order=config['order'], 
        enable_compression=False  # 압축 비활성화
    )
    
    results = benchmark.benchmark_all_id_types(
        record_count=config['record_count'],
        search_sample_size=config['search_sample_size'],
        range_query_count=config['range_query_count']
    )
    
    total_time = time.time() - start_time
    
    if not results:
        print("❌ Benchmark execution failed.")
        return
    
    print(f"\n✅ Benchmark completed! (Total runtime: {total_time:.1f} seconds)")
    
    # Print results
    benchmark.print_results_table()
    
    # Analyze results
    analyze_results(results)
    
    # Generate charts if requested
    if args.charts or args.individual_charts:
        try:
            print("\n" + "=" * 80)
            print("📊 Generating Performance Charts")
            print("=" * 80)
            
            if args.charts:
                visualizer = PerformanceVisualizer(results)
                
                dashboard_path = f"{args.output_dir}/btree_dashboard_{args.scale}.png"
                detailed_path = f"{args.output_dir}/btree_detailed_{args.scale}.png"
                
                print("1. Creating comprehensive dashboard...")
                visualizer.create_comprehensive_dashboard(dashboard_path)
                
                print("2. Creating detailed analysis chart...")
                visualizer.create_detailed_comparison(detailed_path)
                
                print("\n✅ Comprehensive charts generated successfully!")
                print(f"  📊 {dashboard_path}")
                print(f"  📈 {detailed_path}")
            
            if args.individual_charts:
                from individual_charts import IndividualChartGenerator
                
                print("\n🎨 Creating individual charts...")
                chart_dir = f"{args.output_dir}/individual_charts_{args.scale}"
                chart_generator = IndividualChartGenerator(results)
                chart_generator.generate_all_individual_charts(chart_dir)
                chart_generator.create_summary_report(chart_dir)
                
                print(f"\n✅ Individual charts generated successfully!")
                print(f"  📁 Charts directory: {chart_dir}")
            
        except Exception as e:
            print(f"❌ Error generating charts: {e}")
    
    # Save CSV if requested
    if args.csv:
        try:
            import pandas as pd
            
            print("\n" + "=" * 80)
            print("💾 Saving Results to CSV")
            print("=" * 80)
            
            # Create DataFrame
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
            csv_filename = f"{args.output_dir}/btree_results_{args.scale}_{time.strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(csv_filename, index=False)
            
            print(f"✅ Results saved to {csv_filename}")
            
        except ImportError:
            print("❌ pandas not installed. Cannot save CSV file.")
            print("Install with: pip install pandas")
        except Exception as e:
            print(f"❌ Error saving CSV: {e}")
    
    print(f"\n🎉 B-tree ID performance analysis completed!")
    print("Use the results to choose the optimal ID type for your use case.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nBenchmark cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)