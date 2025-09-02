"""
Comprehensive test suite for PostgreSQL-style B-tree compression
"""

import pytest
import random
import string
import time
from btree import PostgreSQLBTree, COMPRESSION_AVAILABLE
from compression import (
    PrefixCompression, DictionaryCompression, DeltaCompression, 
    RunLengthCompression, TOASTCompression, CompressionManager
)

# Skip all compression tests if compression module is not available
pytestmark = pytest.mark.skipif(not COMPRESSION_AVAILABLE, reason="Compression module not available")


class TestCompressionStrategies:
    """Test individual compression strategies"""
    
    def test_prefix_compression_strings(self):
        """Test prefix compression on strings with common prefixes"""
        compressor = PrefixCompression()
        data = ["user_001", "user_002", "user_003", "user_004", "user_005"]
        
        # Test compression
        compressed, metadata = compressor.compress(data)
        ratio = compressor.estimate_compression_ratio(data)
        
        # Verify compression is beneficial
        assert ratio < 1.0
        assert metadata["compression"] == "prefix"
        
        # Test decompression
        decompressed = compressor.decompress(compressed, metadata)
        assert decompressed == data
    
    def test_prefix_compression_no_common_prefix(self):
        """Test prefix compression on strings without common prefixes"""
        compressor = PrefixCompression()
        data = ["apple", "banana", "cherry", "date", "elderberry"]
        
        compressed, metadata = compressor.compress(data)
        ratio = compressor.estimate_compression_ratio(data)
        
        # Should have limited compression benefit
        assert ratio > 0.8  # Not much compression
        
        decompressed = compressor.decompress(compressed, metadata)
        assert decompressed == data
    
    def test_dictionary_compression(self):
        """Test dictionary compression on repeated values"""
        compressor = DictionaryCompression(min_frequency=2)
        data = ["apple", "banana", "apple", "cherry", "banana", "apple", "date"]
        
        compressed, metadata = compressor.compress(data)
        ratio = compressor.estimate_compression_ratio(data)
        
        # Should provide good compression for repeated items
        assert ratio < 1.0
        assert metadata["compression"] == "dictionary"
        
        decompressed = compressor.decompress(compressed, metadata)
        assert sorted(decompressed) == sorted(data)  # Order might change
    
    def test_delta_compression(self):
        """Test delta compression on sequential numbers"""
        compressor = DeltaCompression()
        data = [100, 101, 102, 103, 104, 105, 106]
        
        compressed, metadata = compressor.compress(data)
        ratio = compressor.estimate_compression_ratio(data)
        
        # Should provide good compression for sequential data
        assert ratio < 0.5  # Very good compression
        assert metadata["compression"] == "delta"
        
        decompressed = compressor.decompress(compressed, metadata)
        assert decompressed == data
    
    def test_delta_compression_random(self):
        """Test delta compression on random numbers"""
        compressor = DeltaCompression()
        data = [random.randint(1, 1000000) for _ in range(100)]
        
        compressed, metadata = compressor.compress(data)
        ratio = compressor.estimate_compression_ratio(data)
        
        # Should not provide good compression for random data
        assert ratio > 0.7
        
        decompressed = compressor.decompress(compressed, metadata)
        assert decompressed == data
    
    def test_run_length_compression(self):
        """Test run-length encoding on repeated values"""
        compressor = RunLengthCompression()
        data = ["A"] * 10 + ["B"] * 5 + ["C"] * 8 + ["A"] * 3
        
        compressed, metadata = compressor.compress(data)
        ratio = compressor.estimate_compression_ratio(data)
        
        # Should provide excellent compression for runs
        assert ratio < 0.3
        assert metadata["compression"] == "rle"
        
        decompressed = compressor.decompress(compressed, metadata)
        assert decompressed == data
    
    def test_toast_compression(self):
        """Test TOAST-style compression with zlib"""
        compressor = TOASTCompression(min_size_threshold=50)
        data = ["This is a long string that should compress well with zlib"] * 10
        
        compressed, metadata = compressor.compress(data)
        ratio = compressor.estimate_compression_ratio(data)
        
        # Should provide good compression for repeated text
        assert ratio < 0.5
        assert metadata["compression"] == "toast"
        
        decompressed = compressor.decompress(compressed, metadata)
        assert decompressed == data


class TestCompressionManager:
    """Test the compression manager's strategy selection"""
    
    def test_automatic_strategy_selection(self):
        """Test that compression manager selects the best strategy"""
        manager = CompressionManager()
        
        # Test with prefix-compressible data
        prefix_data = ["user_001", "user_002", "user_003", "user_004"]
        compressed, metadata = manager.compress(prefix_data)
        
        assert "strategy" in metadata
        decompressed = manager.decompress(compressed, metadata)
        assert decompressed == prefix_data
        
        # Test with delta-compressible data
        delta_data = list(range(100, 200))
        compressed, metadata = manager.compress(delta_data)
        
        assert "strategy" in metadata
        decompressed = manager.decompress(compressed, metadata)
        assert decompressed == delta_data
    
    def test_compression_stats(self):
        """Test compression statistics generation"""
        manager = CompressionManager()
        data = ["test"] * 10 + ["data"] * 5
        
        stats = manager.get_compression_stats(data)
        
        assert isinstance(stats, dict)
        assert len(stats) > 0
        
        for strategy_name, strategy_stats in stats.items():
            if "error" not in strategy_stats:
                assert "estimated_ratio" in strategy_stats
                assert "actual_compressed_size" in strategy_stats


class TestBTreeCompression:
    """Test B-tree integration with compression"""
    
    def test_btree_with_compression_enabled(self):
        """Test B-tree operations with compression enabled"""
        btree = PostgreSQLBTree(order=5, enable_compression=True)
        
        # Insert data that should compress well
        prefix_data = [(f"user_{i:03d}", f"User {i}") for i in range(100)]
        for key, value in prefix_data:
            btree.insert(key, value)
        
        # Verify all data is searchable
        for key, expected_value in prefix_data:
            results = btree.search(key)
            assert len(results) == 1
            assert results[0] == expected_value
        
        # Test compression
        compression_stats = btree.compress_all_pages()
        assert compression_stats['compression_attempts'] > 0
    
    def test_btree_with_compression_disabled(self):
        """Test B-tree operations with compression disabled"""
        btree = PostgreSQLBTree(order=5, enable_compression=False)
        
        # Insert data
        for i in range(50):
            btree.insert(i, f"value_{i}")
        
        # Verify compression is not attempted
        stats = btree.get_statistics()
        assert stats['compression_enabled'] == False
        
        # Try to compress (should return disabled status)
        compression_result = btree.compress_all_pages()
        assert compression_result['compression_enabled'] == False
    
    def test_compression_with_duplicate_keys(self):
        """Test compression with duplicate keys"""
        btree = PostgreSQLBTree(order=5, enable_compression=True)
        
        # Insert duplicate keys with similar values
        base_keys = ["customer_001", "customer_002", "customer_003"]
        for key in base_keys:
            for i in range(5):
                btree.insert(key, f"Order #{i+1}")
        
        # Compress pages
        compression_stats = btree.compress_all_pages()
        
        # Verify data integrity after compression
        for key in base_keys:
            results = btree.search(key)
            assert len(results) == 5
            assert all("Order #" in result for result in results)
    
    def test_range_queries_with_compression(self):
        """Test range queries work correctly with compressed pages"""
        btree = PostgreSQLBTree(order=10, enable_compression=True)
        
        # Insert sequential data
        data = [(i, f"value_{i}") for i in range(100)]
        for key, value in data:
            btree.insert(key, value)
        
        # Compress pages
        btree.compress_all_pages()
        
        # Test range query
        results = list(btree.range_query(25, 35))
        expected = [(i, f"value_{i}") for i in range(25, 36)]
        
        assert len(results) == len(expected)
        for (k1, v1), (k2, v2) in zip(sorted(results), sorted(expected)):
            assert k1 == k2
            assert v1 == v2
    
    def test_deletion_with_compression(self):
        """Test deletion operations work with compressed pages"""
        btree = PostgreSQLBTree(order=5, enable_compression=True)
        
        # Insert data
        keys_to_insert = list(range(1, 21))
        for key in keys_to_insert:
            btree.insert(key, f"value_{key}")
        
        # Compress pages
        btree.compress_all_pages()
        
        # Delete some keys
        keys_to_delete = [5, 10, 15]
        for key in keys_to_delete:
            result = btree.delete(key, f"value_{key}")
            assert result == True
        
        # Verify deletions
        for key in keys_to_delete:
            assert btree.search(key) == []
        
        # Verify remaining keys
        remaining_keys = [k for k in keys_to_insert if k not in keys_to_delete]
        for key in remaining_keys:
            results = btree.search(key)
            assert len(results) == 1
            assert results[0] == f"value_{key}"


class TestCompressionPerformance:
    """Performance tests for compression features"""
    
    def test_compression_performance_strings(self):
        """Test compression performance on string data"""
        btree = PostgreSQLBTree(order=50, enable_compression=True)
        
        # Generate data with common prefixes
        start_time = time.time()
        for i in range(1000):
            key = f"user_{i:06d}"
            value = f"User {i} - some additional data that might compress well"
            btree.insert(key, value)
        insert_time = time.time() - start_time
        
        # Test compression
        start_time = time.time()
        compression_stats = btree.compress_all_pages()
        compression_time = time.time() - start_time
        
        # Test searches after compression
        start_time = time.time()
        for i in range(0, 1000, 10):  # Sample every 10th key
            key = f"user_{i:06d}"
            results = btree.search(key)
            assert len(results) == 1
        search_time = time.time() - start_time
        
        print(f"\nCompression Performance Results:")
        print(f"  Insert time: {insert_time:.3f} seconds")
        print(f"  Compression time: {compression_time:.3f} seconds")
        print(f"  Search time (compressed): {search_time:.3f} seconds")
        print(f"  Compression attempts: {compression_stats.get('compression_attempts', 0)}")
        print(f"  Compression successes: {compression_stats.get('compression_successes', 0)}")
    
    def test_compression_memory_usage(self):
        """Test memory usage with and without compression"""
        # Create two identical trees
        btree_compressed = PostgreSQLBTree(order=20, enable_compression=True)
        btree_uncompressed = PostgreSQLBTree(order=20, enable_compression=False)
        
        # Insert identical data with high redundancy
        common_values = ["Status: Active", "Status: Inactive", "Status: Pending"]
        
        for i in range(500):
            key = f"record_{i:04d}"
            value = random.choice(common_values)
            btree_compressed.insert(key, value)
            btree_uncompressed.insert(key, value)
        
        # Compress one tree
        compression_stats = btree_compressed.compress_all_pages()
        
        # Get detailed statistics
        compressed_details = btree_compressed.get_detailed_compression_stats()
        uncompressed_stats = btree_uncompressed.get_statistics()
        
        print(f"\nMemory Usage Comparison:")
        print(f"  Nodes with compression attempts: {compression_stats.get('compression_attempts', 0)}")
        print(f"  Successful compressions: {compression_stats.get('compression_successes', 0)}")
        if compressed_details['total_original_size'] > 0:
            print(f"  Compression ratio: {compressed_details['overall_compression_ratio']:.2f}")
            print(f"  Space saved: {compressed_details['total_original_size'] - compressed_details['total_compressed_size']} bytes")
    
    def test_different_data_types_compression(self):
        """Test compression effectiveness on different data types"""
        btree = PostgreSQLBTree(order=10, enable_compression=True)
        
        test_cases = [
            ("Sequential integers", list(range(100)), "Should compress well with delta encoding"),
            ("Random integers", [random.randint(1, 1000000) for _ in range(100)], "Should not compress well"),
            ("Repeated strings", ["common_value"] * 50 + ["another_value"] * 30, "Should compress well with RLE"),
            ("Common prefix strings", [f"prefix_{i}" for i in range(100)], "Should compress well with prefix compression"),
            ("Mixed data", [f"user_{i}" if i % 2 == 0 else i for i in range(100)], "Mixed compression effectiveness")
        ]
        
        results = {}
        
        for test_name, data, description in test_cases:
            # Clear the tree
            btree = PostgreSQLBTree(order=10, enable_compression=True)
            
            # Insert test data
            for i, value in enumerate(data):
                btree.insert(i, value)
            
            # Compress and measure
            compression_stats = btree.compress_all_pages()
            detailed_stats = btree.get_detailed_compression_stats()
            
            results[test_name] = {
                'description': description,
                'attempts': compression_stats.get('compression_attempts', 0),
                'successes': compression_stats.get('compression_successes', 0),
                'compression_ratio': detailed_stats.get('overall_compression_ratio', 1.0),
                'success_rate': compression_stats.get('compression_successes', 0) / max(1, compression_stats.get('compression_attempts', 1))
            }
        
        print(f"\nCompression Effectiveness by Data Type:")
        for test_name, stats in results.items():
            print(f"  {test_name}:")
            print(f"    {stats['description']}")
            print(f"    Success rate: {stats['success_rate']:.1%}")
            print(f"    Compression ratio: {stats['compression_ratio']:.2f}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])  # -s to show print statements