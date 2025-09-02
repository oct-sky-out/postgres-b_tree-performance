"""
Comprehensive test suite for PostgreSQL-style B-tree implementation
"""

import pytest
import random
from btree import PostgreSQLBTree, BTreeNode, KeyValue


class TestBTreeBasics:
    """Test basic B-tree operations"""
    
    def test_empty_tree_search(self):
        btree = PostgreSQLBTree(order=3)
        assert btree.search(5) == []
    
    def test_single_insert_search(self):
        btree = PostgreSQLBTree(order=3)
        btree.insert(5, "value5")
        assert btree.search(5) == ["value5"]
        assert btree.size == 1
    
    def test_multiple_inserts(self):
        btree = PostgreSQLBTree(order=3)
        values = [(1, "one"), (2, "two"), (3, "three")]
        
        for key, value in values:
            btree.insert(key, value)
        
        for key, expected_value in values:
            assert btree.search(key) == [expected_value]
        
        assert btree.size == 3
    
    def test_duplicate_keys(self):
        """Test PostgreSQL-style duplicate key support"""
        btree = PostgreSQLBTree(order=3)
        btree.insert(5, "first")
        btree.insert(5, "second")
        btree.insert(5, "third")
        
        results = btree.search(5)
        assert len(results) == 3
        assert set(results) == {"first", "second", "third"}
    
    def test_node_splitting(self):
        """Test that nodes split properly when full"""
        btree = PostgreSQLBTree(order=3)  # Small order to force splits
        
        # Insert enough keys to force multiple splits
        for i in range(10):
            btree.insert(i, f"value{i}")
        
        # All keys should still be searchable
        for i in range(10):
            assert btree.search(i) == [f"value{i}"]
        
        # Tree should have proper height
        assert btree.height >= 2


class TestBTreeDeletion:
    """Test B-tree deletion operations"""
    
    def test_delete_from_leaf(self):
        btree = PostgreSQLBTree(order=3)
        keys = [1, 2, 3, 4, 5]
        
        for key in keys:
            btree.insert(key, f"value{key}")
        
        # Delete a key
        assert btree.delete(3, "value3") == True
        assert btree.search(3) == []
        assert btree.size == 4
        
        # Other keys should remain
        for key in [1, 2, 4, 5]:
            assert btree.search(key) == [f"value{key}"]
    
    def test_delete_nonexistent_key(self):
        btree = PostgreSQLBTree(order=3)
        btree.insert(1, "value1")
        
        assert btree.delete(999) == False
        assert btree.size == 1
    
    def test_delete_duplicate_key(self):
        btree = PostgreSQLBTree(order=3)
        btree.insert(5, "first")
        btree.insert(5, "second")
        btree.insert(5, "third")
        
        # Delete specific value
        assert btree.delete(5, "second") == True
        results = btree.search(5)
        assert len(results) == 2
        assert "second" not in results
        
        # Delete any occurrence
        assert btree.delete(5) == True
        assert len(btree.search(5)) == 1


class TestBTreeRangeQueries:
    """Test PostgreSQL-style range query functionality"""
    
    def test_empty_range_query(self):
        btree = PostgreSQLBTree(order=3)
        results = list(btree.range_query(1, 10))
        assert results == []
    
    def test_simple_range_query(self):
        btree = PostgreSQLBTree(order=3)
        data = [(1, "one"), (3, "three"), (5, "five"), (7, "seven"), (9, "nine")]
        
        for key, value in data:
            btree.insert(key, value)
        
        # Query range [3, 7]
        results = list(btree.range_query(3, 7))
        expected = [(3, "three"), (5, "five"), (7, "seven")]
        assert results == expected
    
    def test_range_query_with_duplicates(self):
        btree = PostgreSQLBTree(order=3)
        btree.insert(5, "first")
        btree.insert(5, "second")
        btree.insert(3, "three")
        btree.insert(7, "seven")
        
        results = list(btree.range_query(3, 7))
        # Should include all values for key 5
        keys = [r[0] for r in results]
        values = [r[1] for r in results]
        
        assert 3 in keys
        assert keys.count(5) == 2
        assert 7 in keys
        assert "first" in values and "second" in values
    
    def test_exclusive_range_query(self):
        btree = PostgreSQLBTree(order=3)
        data = [(1, "one"), (3, "three"), (5, "five"), (7, "seven")]
        
        for key, value in data:
            btree.insert(key, value)
        
        # Exclusive range query [3, 7)
        results = list(btree.range_query(3, 7, inclusive=False))
        assert results == [(5, "five")]


class TestBTreeStatistics:
    """Test PostgreSQL-style statistics collection"""
    
    def test_empty_tree_statistics(self):
        btree = PostgreSQLBTree(order=3)
        stats = btree.get_statistics()
        
        assert stats['height'] == 0
        assert stats['total_keys'] == 0
        assert stats['leaf_pages'] == 0
        assert stats['internal_pages'] == 0
    
    def test_statistics_after_inserts(self):
        btree = PostgreSQLBTree(order=3)
        
        # Insert enough data to create multi-level tree
        for i in range(20):
            btree.insert(i, f"value{i}")
        
        stats = btree.get_statistics()
        
        assert stats['height'] > 1
        assert stats['total_keys'] == 20
        assert stats['leaf_pages'] > 0
        assert stats['avg_keys_per_leaf'] > 0


class TestBTreeStressTest:
    """Stress tests for B-tree implementation"""
    
    def test_large_sequential_insert(self):
        """Test inserting large number of sequential keys"""
        btree = PostgreSQLBTree(order=50)
        n = 1000
        
        # Insert sequential keys
        for i in range(n):
            btree.insert(i, f"value{i}")
        
        # Verify all keys are searchable
        for i in range(n):
            assert btree.search(i) == [f"value{i}"]
        
        assert btree.size == n
    
    def test_large_random_insert(self):
        """Test inserting large number of random keys"""
        btree = PostgreSQLBTree(order=50)
        n = 1000
        keys = list(range(n))
        random.shuffle(keys)
        
        # Insert random keys
        for key in keys:
            btree.insert(key, f"value{key}")
        
        # Verify all keys are searchable
        for key in keys:
            assert btree.search(key) == [f"value{key}"]
        
        assert btree.size == n
    
    def test_mixed_operations(self):
        """Test mixed insert/delete operations"""
        btree = PostgreSQLBTree(order=10)
        inserted_keys = {}  # key -> count of insertions
        
        # Perform mixed operations
        for i in range(500):
            if random.random() < 0.7:  # 70% inserts
                key = random.randint(1, 1000)
                btree.insert(key, f"value{key}")
                inserted_keys[key] = inserted_keys.get(key, 0) + 1
            else:  # 30% deletes
                if inserted_keys:
                    key = random.choice(list(inserted_keys.keys()))
                    if btree.delete(key, f"value{key}"):
                        inserted_keys[key] -= 1
                        if inserted_keys[key] == 0:
                            del inserted_keys[key]
        
        # Verify remaining keys are searchable
        for key, expected_count in inserted_keys.items():
            actual_values = btree.search(key)
            assert len(actual_values) == expected_count
            # All values should be the same
            for value in actual_values:
                assert value == f"value{key}"


class TestKeyValueClass:
    """Test KeyValue dataclass functionality"""
    
    def test_key_value_comparison(self):
        kv1 = KeyValue(5, "five")
        kv2 = KeyValue(10, "ten")
        kv3 = KeyValue(5, "another_five")
        
        assert kv1 < kv2
        assert kv2 > kv1
        assert kv1 == kv3  # Only compares keys
        assert kv1 <= kv3
        assert kv1 >= kv3
    
    def test_key_value_with_primitives(self):
        kv = KeyValue(5, "five")
        
        assert kv < 10
        assert kv > 3
        assert kv == 5
        assert kv <= 5
        assert kv >= 5


class TestBTreeNodeOperations:
    """Test individual node operations"""
    
    def test_node_creation(self):
        node = BTreeNode(order=3, is_leaf=True)
        assert node.is_leaf == True
        assert node.order == 3
        assert len(node.keys) == 0
        assert len(node.children) == 0
    
    def test_node_key_insertion(self):
        node = BTreeNode(order=3, is_leaf=True)
        kv1 = KeyValue(5, "five")
        kv2 = KeyValue(3, "three")
        kv3 = KeyValue(7, "seven")
        
        node.insert_key(kv1)
        node.insert_key(kv2)
        node.insert_key(kv3)
        
        # Keys should be sorted
        keys = [kv.key for kv in node.keys]
        assert keys == [3, 5, 7]
    
    def test_node_splitting(self):
        node = BTreeNode(order=3, is_leaf=True)
        
        # Fill node to capacity
        for i in [1, 2, 3, 4]:
            node.insert_key(KeyValue(i, f"value{i}"))
        
        # Split the node
        right_node, promoted_key = node.split()
        
        # Check split results
        assert len(node.keys) <= 2
        assert len(right_node.keys) <= 2
        assert promoted_key.key in [1, 2, 3, 4]
        
        # All original keys should be preserved (promoted key appears in right node for leaf)
        left_keys = [kv.key for kv in node.keys]
        right_keys = [kv.key for kv in right_node.keys]
        all_keys = left_keys + right_keys
        assert sorted(all_keys) == [1, 2, 3, 4]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])