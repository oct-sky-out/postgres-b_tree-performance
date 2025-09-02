"""
PostgreSQL-style B-tree Page Compression Demo

This demonstrates the compression features implemented for the B-tree,
showing how different types of data compress and the performance benefits.
"""

import time
import random
import string
from btree import PostgreSQLBTree, COMPRESSION_AVAILABLE
from compression import CompressionManager

def generate_test_data():
    """Generate various types of test data to demonstrate compression"""
    
    datasets = {}
    
    # 1. Sequential data (good for delta compression)
    datasets['sequential_numbers'] = {
        'description': 'Sequential numbers (1000-2000)',
        'data': [(i, f"Record {i}") for i in range(1000, 2000)],
        'expected_compression': 'Delta encoding should work well'
    }
    
    # 2. Repeated values (good for RLE)
    datasets['status_values'] = {
        'description': 'Status values with high repetition',
        'data': [(i, random.choice(['Active', 'Inactive', 'Pending', 'Archived'])) 
                for i in range(1000)],
        'expected_compression': 'Run-length or dictionary compression should work well'
    }
    
    # 3. Common prefix strings (good for prefix compression)
    datasets['user_ids'] = {
        'description': 'User IDs with common prefix',
        'data': [(f"user_{i:06d}", f"User {i} Profile Data") for i in range(1000)],
        'expected_compression': 'Prefix compression should work well'
    }
    
    # 4. Large text values (good for TOAST compression)
    long_text = "This is a sample text that will be repeated many times to create large values that should compress well with zlib compression. " * 5
    datasets['large_text'] = {
        'description': 'Large text values',
        'data': [(f"doc_{i}", long_text + f" Document {i}") for i in range(200)],
        'expected_compression': 'TOAST (zlib) compression should work well'
    }
    
    # 5. Random data (poor compression)
    datasets['random_data'] = {
        'description': 'Random strings (poor compression expected)',
        'data': [(''.join(random.choices(string.ascii_letters + string.digits, k=10)),
                 ''.join(random.choices(string.ascii_letters + string.digits, k=20)))
                for _ in range(500)],
        'expected_compression': 'Should not compress well'
    }
    
    return datasets

def demonstrate_compression_strategies():
    """Demonstrate individual compression strategies"""
    print("=== Individual Compression Strategy Demonstration ===")
    
    if not COMPRESSION_AVAILABLE:
        print("Compression module not available. Skipping demonstration.")
        return
    
    manager = CompressionManager()
    
    # Test different data types
    test_cases = [
        ("Prefix compression test", ["user_001", "user_002", "user_003", "user_004", "user_005"]),
        ("Dictionary compression test", ["apple", "banana", "apple", "cherry", "banana", "apple"] * 3),
        ("Delta compression test", list(range(100, 150))),
        ("RLE compression test", ["A"] * 20 + ["B"] * 15 + ["C"] * 10),
        ("Random data (no compression)", [''.join(random.choices(string.ascii_letters, k=10)) for _ in range(20)])
    ]
    
    for test_name, data in test_cases:
        print(f"\n{test_name}:")
        
        # Get compression statistics for all strategies
        stats = manager.get_compression_stats(data)
        
        # Find the best strategy
        best_strategy = None
        best_ratio = 1.0
        
        for strategy_name, strategy_stats in stats.items():
            if "error" not in strategy_stats:
                ratio = strategy_stats.get("estimated_ratio", 1.0)
                if ratio < best_ratio:
                    best_ratio = ratio
                    best_strategy = strategy_name
        
        if best_strategy:
            print(f"  Best strategy: {best_strategy}")
            print(f"  Estimated compression ratio: {best_ratio:.2f}")
            
            # Actually compress with the manager
            compressed_data, metadata = manager.compress(data)
            original_size = len(str(data))
            compressed_size = len(compressed_data)
            actual_ratio = compressed_size / original_size
            
            print(f"  Actual compression ratio: {actual_ratio:.2f}")
            print(f"  Space saved: {original_size - compressed_size} bytes")
        else:
            print("  No beneficial compression found")

def demonstrate_btree_compression():
    """Demonstrate B-tree compression features"""
    print("\n=== B-tree Compression Demonstration ===")
    
    if not COMPRESSION_AVAILABLE:
        print("Compression module not available. Skipping demonstration.")
        return
    
    datasets = generate_test_data()
    
    for dataset_name, dataset_info in datasets.items():
        print(f"\n--- Testing: {dataset_info['description']} ---")
        print(f"Expected: {dataset_info['expected_compression']}")
        
        # Create trees with and without compression
        btree_compressed = PostgreSQLBTree(order=20, enable_compression=True)
        btree_uncompressed = PostgreSQLBTree(order=20, enable_compression=False)
        
        data = dataset_info['data']
        
        # Insert data into both trees
        print(f"Inserting {len(data)} records...")
        
        start_time = time.time()
        for key, value in data:
            btree_compressed.insert(key, value)
        compressed_insert_time = time.time() - start_time
        
        start_time = time.time()
        for key, value in data:
            btree_uncompressed.insert(key, value)
        uncompressed_insert_time = time.time() - start_time
        
        # Compress the compressed tree
        start_time = time.time()
        compression_stats = btree_compressed.compress_all_pages()
        compression_time = time.time() - start_time
        
        # Get detailed statistics
        detailed_stats = btree_compressed.get_detailed_compression_stats()
        
        print(f"Results:")
        print(f"  Insert time (compressed tree): {compressed_insert_time:.3f}s")
        print(f"  Insert time (uncompressed tree): {uncompressed_insert_time:.3f}s")
        print(f"  Compression time: {compression_time:.3f}s")
        print(f"  Compression attempts: {compression_stats.get('compression_attempts', 0)}")
        print(f"  Successful compressions: {compression_stats.get('compression_successes', 0)}")
        
        if compression_stats.get('compression_successes', 0) > 0:
            success_rate = compression_stats['compression_successes'] / compression_stats['compression_attempts']
            print(f"  Success rate: {success_rate:.1%}")
            
            if detailed_stats['total_original_size'] > 0:
                ratio = detailed_stats['overall_compression_ratio']
                space_saved = detailed_stats['total_original_size'] - detailed_stats['total_compressed_size']
                print(f"  Overall compression ratio: {ratio:.2f}")
                print(f"  Space saved: {space_saved} bytes")
        else:
            print("  No compression achieved")
        
        # Test search performance on compressed data
        if compression_stats.get('compression_successes', 0) > 0:
            sample_keys = [data[i][0] for i in range(0, len(data), max(1, len(data) // 10))]
            
            start_time = time.time()
            for key in sample_keys:
                results = btree_compressed.search(key)
            compressed_search_time = time.time() - start_time
            
            start_time = time.time()
            for key in sample_keys:
                results = btree_uncompressed.search(key)
            uncompressed_search_time = time.time() - start_time
            
            print(f"  Search time (compressed): {compressed_search_time:.3f}s")
            print(f"  Search time (uncompressed): {uncompressed_search_time:.3f}s")
            
            if uncompressed_search_time > 0:
                search_ratio = compressed_search_time / uncompressed_search_time
                print(f"  Search performance ratio: {search_ratio:.2f}")

def demonstrate_compression_by_level():
    """Demonstrate compression effectiveness by tree level"""
    print("\n=== Compression by Tree Level Analysis ===")
    
    if not COMPRESSION_AVAILABLE:
        print("Compression module not available. Skipping demonstration.")
        return
    
    btree = PostgreSQLBTree(order=10, enable_compression=True)
    
    # Insert a large dataset to create multiple levels
    print("Creating multi-level tree with 2000 records...")
    for i in range(2000):
        key = f"key_{i:06d}"
        value = f"This is a longer value for key {i} that might compress well when repeated patterns emerge"
        if i % 100 == 0:
            value += f" Special marker every 100: {i // 100}"
        btree.insert(key, value)
    
    print(f"Tree height: {btree.height}")
    print(f"Leaf pages: {btree.leaf_pages}")
    print(f"Internal pages: {btree.internal_pages}")
    
    # Compress all pages
    compression_stats = btree.compress_all_pages()
    detailed_stats = btree.get_detailed_compression_stats()
    
    print(f"\nCompression Results by Level:")
    for level, level_stats in detailed_stats['by_level'].items():
        nodes = level_stats['nodes']
        compressed = level_stats['compressed']
        original_size = level_stats['original_size']
        compressed_size = level_stats['compressed_size']
        
        if original_size > 0:
            ratio = compressed_size / original_size
            space_saved = original_size - compressed_size
            success_rate = compressed / nodes if nodes > 0 else 0
            
            print(f"  Level {level}:")
            print(f"    Nodes: {nodes}, Compressed: {compressed}")
            print(f"    Success rate: {success_rate:.1%}")
            print(f"    Compression ratio: {ratio:.2f}")
            print(f"    Space saved: {space_saved} bytes")

def demonstrate_real_world_scenario():
    """Demonstrate a real-world database scenario"""
    print("\n=== Real-World Database Scenario ===")
    
    if not COMPRESSION_AVAILABLE:
        print("Compression module not available. Skipping demonstration.")
        return
    
    print("Simulating a user database with typical data patterns...")
    
    btree = PostgreSQLBTree(order=50, enable_compression=True)
    
    # Simulate real database data
    departments = ["Engineering", "Marketing", "Sales", "HR", "Finance"]
    statuses = ["Active", "Inactive", "On Leave", "Terminated"]
    
    start_time = time.time()
    
    for i in range(5000):
        user_id = f"emp_{i:06d}"
        profile_data = {
            "name": f"Employee {i}",
            "department": random.choice(departments),
            "status": random.choice(statuses),
            "email": f"employee{i}@company.com",
            "hire_date": f"2020-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
            "notes": "Standard employee profile with common fields"
        }
        
        # Convert to string for storage (simulating JSON or similar)
        profile_str = str(profile_data)
        btree.insert(user_id, profile_str)
    
    insert_time = time.time() - start_time
    
    print(f"Inserted 5000 employee records in {insert_time:.2f} seconds")
    
    # Get initial statistics
    initial_stats = btree.get_statistics()
    print(f"Tree height: {initial_stats['height']}")
    print(f"Total pages: {initial_stats['leaf_pages'] + initial_stats['internal_pages']}")
    
    # Compress the database
    start_time = time.time()
    compression_stats = btree.compress_all_pages()
    compression_time = time.time() - start_time
    
    # Get final statistics
    detailed_stats = btree.get_detailed_compression_stats()
    
    print(f"\nCompression completed in {compression_time:.2f} seconds")
    print(f"Pages compressed: {compression_stats.get('compression_successes', 0)} out of {compression_stats.get('compression_attempts', 0)}")
    
    if detailed_stats['total_original_size'] > 0:
        ratio = detailed_stats['overall_compression_ratio']
        space_saved = detailed_stats['total_original_size'] - detailed_stats['total_compressed_size']
        
        print(f"Overall compression ratio: {ratio:.2f}")
        print(f"Space saved: {space_saved:,} bytes ({space_saved/1024:.1f} KB)")
        
        # Estimate storage savings
        print(f"\nEstimated storage benefits:")
        print(f"  Original estimated size: {detailed_stats['total_original_size']:,} bytes")
        print(f"  Compressed estimated size: {detailed_stats['total_compressed_size']:,} bytes")
        print(f"  Storage reduction: {(1 - ratio) * 100:.1f}%")
    
    # Test query performance after compression
    print(f"\nTesting query performance on compressed data...")
    sample_ids = [f"emp_{i:06d}" for i in range(0, 5000, 100)]
    
    start_time = time.time()
    for user_id in sample_ids:
        results = btree.search(user_id)
        assert len(results) == 1  # Verify data integrity
    query_time = time.time() - start_time
    
    print(f"Queried {len(sample_ids)} records in {query_time:.3f} seconds")
    print(f"Average query time: {query_time * 1000 / len(sample_ids):.2f} ms per query")

if __name__ == "__main__":
    print("PostgreSQL-style B-tree Page Compression Demo")
    print("=" * 55)
    
    if not COMPRESSION_AVAILABLE:
        print("ERROR: Compression module is not available!")
        print("Make sure compression.py is in the same directory.")
        exit(1)
    
    demonstrate_compression_strategies()
    demonstrate_btree_compression()
    demonstrate_compression_by_level()
    demonstrate_real_world_scenario()
    
    print("\n" + "=" * 55)
    print("Demo completed successfully!")
    print("\nKey takeaways:")
    print("- Different data types benefit from different compression strategies")
    print("- PostgreSQL-style adaptive compression selects the best strategy")
    print("- Compression can significantly reduce storage requirements")
    print("- Query performance is maintained with automatic decompression")
    print("- Real-world databases can see substantial space savings")