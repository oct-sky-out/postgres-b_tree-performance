"""
PostgreSQL-style B-tree implementation in Python

This implementation follows PostgreSQL's B-tree design principles:
- High fanout to minimize tree height
- Page-oriented storage for disk efficiency
- Support for duplicate keys
- Range queries and point lookups
- Concurrent access considerations
"""

from typing import Any, List, Optional, Tuple, Iterator
import bisect
from dataclasses import dataclass
from enum import Enum

try:
    from compression import CompressionManager
    COMPRESSION_AVAILABLE = True
except ImportError:
    COMPRESSION_AVAILABLE = False


class NodeType(Enum):
    INTERNAL = "internal"
    LEAF = "leaf"


@dataclass
class KeyValue:
    """Represents a key-value pair with comparison support"""
    key: Any
    value: Any
    
    def __lt__(self, other):
        if isinstance(other, KeyValue):
            return self.key < other.key
        return self.key < other
    
    def __le__(self, other):
        if isinstance(other, KeyValue):
            return self.key <= other.key
        return self.key <= other
    
    def __eq__(self, other):
        if isinstance(other, KeyValue):
            return self.key == other.key
        return self.key == other
    
    def __gt__(self, other):
        if isinstance(other, KeyValue):
            return self.key > other.key
        return self.key > other
    
    def __ge__(self, other):
        if isinstance(other, KeyValue):
            return self.key >= other.key
        return self.key >= other


class BTreeNode:
    """
    B-tree node implementation following PostgreSQL patterns
    
    PostgreSQL uses a page size of 8KB by default, which influences
    the maximum number of keys per node. We simulate this behavior.
    Now includes page-level compression support.
    """
    
    def __init__(self, order: int, is_leaf: bool = True, enable_compression: bool = True):
        self.order = order  # Maximum number of keys
        self.is_leaf = is_leaf
        self.keys: List[KeyValue] = []
        self.children: List['BTreeNode'] = []
        self.parent: Optional['BTreeNode'] = None
        
        # PostgreSQL-specific optimizations
        self.high_key: Optional[KeyValue] = None  # Rightmost key for range queries
        self.page_id: int = 0  # Simulated page identifier
        
        # Compression support
        self.enable_compression = enable_compression and COMPRESSION_AVAILABLE
        self.compression_manager = CompressionManager() if self.enable_compression else None
        self._compressed_data: Optional[bytes] = None
        self._compression_metadata: Optional[dict] = None
        self._is_compressed = False
    
    def is_full(self) -> bool:
        """Check if node has reached maximum capacity"""
        return len(self.keys) >= self.order
    
    def is_minimal(self) -> bool:
        """Check if node has minimum number of keys (for deletion)"""
        min_keys = (self.order + 1) // 2 - 1
        return len(self.keys) <= min_keys
    
    def find_key_position(self, key: Any) -> int:
        """Find the position where key should be inserted"""
        self.ensure_decompressed()
        dummy_kv = KeyValue(key, None)
        return bisect.bisect_left(self.keys, dummy_kv)
    
    def insert_key(self, key_value: KeyValue, child: Optional['BTreeNode'] = None):
        """Insert key-value pair at appropriate position"""
        self.ensure_decompressed()
        pos = self.find_key_position(key_value.key)
        self.keys.insert(pos, key_value)
        
        if child is not None and not self.is_leaf:
            self.children.insert(pos + 1, child)
            child.parent = self
    
    def remove_key(self, index: int) -> KeyValue:
        """Remove key at given index"""
        self.ensure_decompressed()
        key_value = self.keys.pop(index)
        if not self.is_leaf and index < len(self.children):
            self.children.pop(index + 1)
        return key_value
    
    def split(self) -> Tuple['BTreeNode', KeyValue]:
        """
        Split node when full, following PostgreSQL's split strategy
        Returns new right node and the key that moves up
        """
        self.ensure_decompressed()
        mid = len(self.keys) // 2
        
        # Create new right node
        right_node = BTreeNode(self.order, self.is_leaf, self.enable_compression)
        right_node.parent = self.parent
        
        if self.is_leaf:
            # For leaf nodes, promote the middle key but keep it in right node
            right_node.keys = self.keys[mid:]
            promoted_key = self.keys[mid]
            self.keys = self.keys[:mid]
        else:
            # For internal nodes, promote middle key and don't keep it
            right_node.keys = self.keys[mid + 1:]
            promoted_key = self.keys[mid]
            self.keys = self.keys[:mid]
            
            # Move children
            right_node.children = self.children[mid + 1:]
            self.children = self.children[:mid + 1]
            
            # Update parent pointers
            for child in right_node.children:
                child.parent = right_node
        
        # Set high key for PostgreSQL-style range queries
        if self.keys:
            self.high_key = self.keys[-1]
        
        return right_node, promoted_key
    
    def compress_page(self) -> bool:
        """
        Compress the node's data if beneficial
        Returns True if compression was applied
        """
        if not self.enable_compression or self._is_compressed or not self.keys:
            return False
        
        # Extract keys and values separately for better compression
        keys_data = [kv.key for kv in self.keys]
        values_data = [kv.value for kv in self.keys]
        
        try:
            # Compress keys and values separately
            keys_compressed, keys_meta = self.compression_manager.compress(keys_data)
            values_compressed, values_meta = self.compression_manager.compress(values_data)
            
            # Check if compression is beneficial
            original_size = len(str(self.keys).encode())
            compressed_size = len(keys_compressed) + len(values_compressed)
            
            if compressed_size < original_size * 0.8:  # At least 20% savings
                self._compressed_data = {
                    'keys_data': keys_compressed,
                    'values_data': values_compressed,
                    'keys_meta': keys_meta,
                    'values_meta': values_meta,
                    'original_size': original_size,
                    'compressed_size': compressed_size
                }
                self._is_compressed = True
                
                # Clear original data to save memory
                self.keys.clear()
                return True
        except Exception:
            # Compression failed, continue without compression
            pass
        
        return False
    
    def decompress_page(self):
        """Decompress the node's data if it was compressed"""
        if not self._is_compressed or not self._compressed_data:
            return
        
        try:
            # Decompress keys and values
            keys_data = self.compression_manager.decompress(
                self._compressed_data['keys_data'],
                self._compressed_data['keys_meta']
            )
            values_data = self.compression_manager.decompress(
                self._compressed_data['values_data'],
                self._compressed_data['values_meta']
            )
            
            # Reconstruct KeyValue objects
            self.keys = [KeyValue(k, v) for k, v in zip(keys_data, values_data)]
            
            # Clear compressed data
            self._compressed_data = None
            self._is_compressed = False
        except Exception:
            # Decompression failed - this is a serious error
            raise RuntimeError("Failed to decompress node data")
    
    def ensure_decompressed(self):
        """Ensure node is decompressed before operations"""
        if self._is_compressed:
            self.decompress_page()
    
    def get_compression_stats(self) -> dict:
        """Get compression statistics for this node"""
        if not self._is_compressed or not self._compressed_data:
            return {
                'compressed': False,
                'original_size': len(str(self.keys).encode()) if self.keys else 0,
                'compressed_size': 0,
                'compression_ratio': 1.0
            }
        
        original_size = self._compressed_data['original_size']
        compressed_size = self._compressed_data['compressed_size']
        
        return {
            'compressed': True,
            'original_size': original_size,
            'compressed_size': compressed_size,
            'compression_ratio': compressed_size / original_size if original_size > 0 else 1.0,
            'space_saved': original_size - compressed_size
        }


class PostgreSQLBTree:
    """
    PostgreSQL-style B-tree implementation
    
    Key features:
    - Variable order (fanout) with default optimized for 8KB pages
    - Support for duplicate keys
    - Efficient range queries
    - Page-oriented design principles
    """
    
    def __init__(self, order: int = 256, enable_compression: bool = True):
        """
        Initialize B-tree with specified order
        
        PostgreSQL typically uses order ~256 for integer keys (8KB pages)
        """
        self.order = order
        self.enable_compression = enable_compression
        self.root: Optional[BTreeNode] = None
        self.size = 0
        
        # Statistics for PostgreSQL-like query planning
        self.height = 0
        self.leaf_pages = 0
        self.internal_pages = 0
        
        # Compression statistics
        self.compression_stats = {
            'total_compressed_pages': 0,
            'total_original_size': 0,
            'total_compressed_size': 0,
            'compression_attempts': 0,
            'compression_successes': 0
        }
    
    def search(self, key: Any) -> List[Any]:
        """
        Search for all values associated with the given key
        PostgreSQL allows duplicate keys, so we return a list
        """
        if self.root is None:
            return []
        
        values = []
        self._search_recursive(self.root, key, values)
        return values
    
    def _search_recursive(self, node: BTreeNode, key: Any, values: List[Any]):
        """Recursively search for all occurrences of key"""
        if node.is_leaf:
            # Search all keys in leaf node
            for kv in node.keys:
                if kv.key == key:
                    values.append(kv.value)
        else:
            # Search in internal node
            i = 0
            while i < len(node.keys):
                if key <= node.keys[i].key:
                    self._search_recursive(node.children[i], key, values)
                i += 1
            # Check rightmost child
            self._search_recursive(node.children[i], key, values)
    
    def insert(self, key: Any, value: Any):
        """Insert key-value pair into the B-tree"""
        if self.root is None:
            self.root = BTreeNode(self.order, is_leaf=True, enable_compression=self.enable_compression)
            self.height = 1
            self.leaf_pages = 1
        
        key_value = KeyValue(key, value)
        self._insert_recursive(self.root, key_value)
        self.size += 1
    
    def delete(self, key: Any, value: Any = None) -> bool:
        """
        Delete key-value pair from the B-tree
        If value is None, delete first occurrence of key
        """
        if self.root is None:
            return False
        
        result = self._delete_recursive(self.root, key, value)
        if result:
            self.size -= 1
            
            # Update root if it became empty
            if len(self.root.keys) == 0 and not self.root.is_leaf:
                self.root = self.root.children[0]
                self.root.parent = None
                self.height -= 1
                self.internal_pages -= 1
        
        return result
    
    def range_query(self, start_key: Any, end_key: Any, 
                   inclusive: bool = True) -> Iterator[Tuple[Any, Any]]:
        """
        Perform range query between start_key and end_key
        This is one of PostgreSQL B-tree's key strengths
        """
        if self.root is None:
            return
        
        # Find the starting leaf node
        node = self._find_leaf_node(start_key)
        pos = node.find_key_position(start_key)
        
        # Traverse leaves from left to right
        while node is not None:
            while pos < len(node.keys):
                key_value = node.keys[pos]
                
                if key_value.key > end_key:
                    return
                if key_value.key < start_key:
                    pos += 1
                    continue
                if not inclusive and (key_value.key == start_key or key_value.key == end_key):
                    pos += 1
                    continue
                
                yield (key_value.key, key_value.value)
                pos += 1
            
            # Move to next leaf (PostgreSQL uses sibling pointers)
            node = self._get_next_leaf(node)
            pos = 0
    
    def get_statistics(self) -> dict:
        """Return PostgreSQL-style statistics for query planning"""
        base_stats = {
            'height': self.height,
            'leaf_pages': self.leaf_pages,
            'internal_pages': self.internal_pages,
            'total_keys': self.size,
            'avg_keys_per_leaf': self.size / max(1, self.leaf_pages)
        }
        
        if self.enable_compression:
            base_stats.update({
                'compression_enabled': True,
                'compressed_pages': self.compression_stats['total_compressed_pages'],
                'compression_ratio': (
                    self.compression_stats['total_compressed_size'] / 
                    max(1, self.compression_stats['total_original_size'])
                ),
                'compression_success_rate': (
                    self.compression_stats['compression_successes'] /
                    max(1, self.compression_stats['compression_attempts'])
                )
            })
        else:
            base_stats['compression_enabled'] = False
        
        return base_stats
    
    def _find_leaf_node(self, key: Any) -> BTreeNode:
        """Find the leaf node where key should be located"""
        node = self.root
        if node is None:
            raise ValueError("Cannot find leaf node in empty tree")
        
        while not node.is_leaf:
            # Use bisect-based position lookup for O(log n) child selection
            pos = node.find_key_position(key)
            node = node.children[pos]
        
        return node
    
    def _insert_recursive(self, node: BTreeNode, key_value: KeyValue):
        """Recursively insert key-value pair"""
        if node.is_leaf:
            node.insert_key(key_value)
            
            if node.is_full():
                self._split_node(node)
        else:
            pos = node.find_key_position(key_value.key)
            child = node.children[pos]
            self._insert_recursive(child, key_value)
    
    def _split_node(self, node: BTreeNode):
        """Split a full node and propagate changes upward"""
        if node.parent is None:
            # Create new root
            new_root = BTreeNode(self.order, is_leaf=False)
            new_root.children.append(node)
            node.parent = new_root
            self.root = new_root
            self.height += 1
            self.internal_pages += 1
        
        right_node, promoted_key = node.split()
        
        if node.is_leaf:
            self.leaf_pages += 1
        else:
            self.internal_pages += 1
        
        parent = node.parent
        parent.insert_key(promoted_key, right_node)
        
        if parent.is_full():
            self._split_node(parent)
    
    def _delete_recursive(self, node: BTreeNode, key: Any, value: Any = None) -> bool:
        """Recursively delete key-value pair"""
        if node.is_leaf:
            return self._delete_from_leaf(node, key, value)
        else:
            # Find the correct child
            pos = 0
            while pos < len(node.keys) and key >= node.keys[pos].key:
                pos += 1
            child = node.children[pos]
            result = self._delete_recursive(child, key, value)
            
            if result and child.is_minimal() and child != self.root:
                self._rebalance_node(child)
            
            return result
    
    def _delete_from_leaf(self, node: BTreeNode, key: Any, value: Any = None) -> bool:
        """Delete key-value pair from leaf node"""
        for i, kv in enumerate(node.keys):
            if kv.key == key and (value is None or kv.value == value):
                node.remove_key(i)
                return True
        return False
    
    def _rebalance_node(self, node: BTreeNode):
        """Rebalance node after deletion (borrow or merge)"""
        parent = node.parent
        if parent is None:
            return
        
        # Find node's position in parent
        node_index = parent.children.index(node)
        
        # Try to borrow from left sibling
        if node_index > 0:
            left_sibling = parent.children[node_index - 1]
            if not left_sibling.is_minimal():
                self._borrow_from_left(node, left_sibling, parent, node_index - 1)
                return
        
        # Try to borrow from right sibling
        if node_index < len(parent.children) - 1:
            right_sibling = parent.children[node_index + 1]
            if not right_sibling.is_minimal():
                self._borrow_from_right(node, right_sibling, parent, node_index)
                return
        
        # Merge with sibling
        if node_index > 0:
            left_sibling = parent.children[node_index - 1]
            self._merge_nodes(left_sibling, node, parent, node_index - 1)
        else:
            right_sibling = parent.children[node_index + 1]
            self._merge_nodes(node, right_sibling, parent, node_index)
    
    def _borrow_from_left(self, node: BTreeNode, left_sibling: BTreeNode, 
                         parent: BTreeNode, parent_key_index: int):
        """Borrow key from left sibling"""
        if node.is_leaf:
            borrowed_key = left_sibling.keys.pop()
            node.keys.insert(0, borrowed_key)
            parent.keys[parent_key_index] = borrowed_key
        else:
            borrowed_key = left_sibling.keys.pop()
            borrowed_child = left_sibling.children.pop()
            
            node.keys.insert(0, parent.keys[parent_key_index])
            node.children.insert(0, borrowed_child)
            borrowed_child.parent = node
            
            parent.keys[parent_key_index] = borrowed_key
    
    def _borrow_from_right(self, node: BTreeNode, right_sibling: BTreeNode,
                          parent: BTreeNode, parent_key_index: int):
        """Borrow key from right sibling"""
        if node.is_leaf:
            borrowed_key = right_sibling.keys.pop(0)
            node.keys.append(borrowed_key)
            parent.keys[parent_key_index] = right_sibling.keys[0] if right_sibling.keys else borrowed_key
        else:
            borrowed_key = right_sibling.keys.pop(0)
            borrowed_child = right_sibling.children.pop(0)
            
            node.keys.append(parent.keys[parent_key_index])
            node.children.append(borrowed_child)
            borrowed_child.parent = node
            
            parent.keys[parent_key_index] = borrowed_key
    
    def _merge_nodes(self, left_node: BTreeNode, right_node: BTreeNode,
                    parent: BTreeNode, parent_key_index: int):
        """Merge two nodes"""
        if not left_node.is_leaf:
            left_node.keys.append(parent.keys[parent_key_index])
        
        left_node.keys.extend(right_node.keys)
        left_node.children.extend(right_node.children)
        
        # Update parent pointers
        for child in right_node.children:
            child.parent = left_node
        
        # Remove key and child from parent
        parent.remove_key(parent_key_index)
        
        if right_node.is_leaf:
            self.leaf_pages -= 1
        else:
            self.internal_pages -= 1
    
    def _get_next_leaf(self, node: BTreeNode) -> Optional[BTreeNode]:
        """Get next leaf node for range queries (simplified implementation)"""
        if node.parent is None:
            return None
        
        parent = node.parent
        node_index = parent.children.index(node)
        
        if node_index < len(parent.children) - 1:
            # Go to right sibling's leftmost leaf
            next_node = parent.children[node_index + 1]
            while not next_node.is_leaf:
                next_node = next_node.children[0]
            return next_node
        
        # Go up and find next subtree
        while parent.parent is not None:
            grandparent = parent.parent
            parent_index = grandparent.children.index(parent)
            if parent_index < len(grandparent.children) - 1:
                next_node = grandparent.children[parent_index + 1]
                while not next_node.is_leaf:
                    next_node = next_node.children[0]
                return next_node
            parent = grandparent
        
        return None
    
    def print_tree(self, node: Optional[BTreeNode] = None, level: int = 0):
        """Print tree structure for debugging"""
        if node is None:
            node = self.root
        
        if node is None:
            print("Empty tree")
            return
        
        indent = "  " * level
        keys = [kv.key for kv in node.keys]
        node_type = "LEAF" if node.is_leaf else "INTERNAL"
        print(f"{indent}{node_type}: {keys}")
        
        if not node.is_leaf:
            for child in node.children:
                self.print_tree(child, level + 1)
    
    def compress_all_pages(self) -> dict:
        """
        Compress all nodes in the tree
        Returns compression statistics
        """
        if not self.enable_compression:
            return {'compression_enabled': False}
        
        def compress_subtree(node: BTreeNode):
            if node is None:
                return
            
            # Compress children first
            for child in node.children:
                compress_subtree(child)
            
            # Try to compress this node
            self.compression_stats['compression_attempts'] += 1
            if node.compress_page():
                self.compression_stats['compression_successes'] += 1
                self.compression_stats['total_compressed_pages'] += 1
                
                # Update compression statistics
                stats = node.get_compression_stats()
                if stats['compressed']:
                    self.compression_stats['total_original_size'] += stats['original_size']
                    self.compression_stats['total_compressed_size'] += stats['compressed_size']
        
        compress_subtree(self.root)
        return self.compression_stats.copy()
    
    def decompress_all_pages(self):
        """Decompress all nodes in the tree"""
        def decompress_subtree(node: BTreeNode):
            if node is None:
                return
            
            node.decompress_page()
            for child in node.children:
                decompress_subtree(child)
        
        decompress_subtree(self.root)
        
        # Reset compression statistics
        self.compression_stats = {
            'total_compressed_pages': 0,
            'total_original_size': 0,
            'total_compressed_size': 0,
            'compression_attempts': 0,
            'compression_successes': 0
        }
    
    def get_detailed_compression_stats(self) -> dict:
        """Get detailed compression statistics for all nodes"""
        stats = {
            'total_nodes': 0,
            'compressed_nodes': 0,
            'total_original_size': 0,
            'total_compressed_size': 0,
            'by_level': {},
            'compression_strategies': {}
        }
        
        def collect_stats(node: BTreeNode, level: int):
            if node is None:
                return
            
            stats['total_nodes'] += 1
            node_stats = node.get_compression_stats()
            
            if level not in stats['by_level']:
                stats['by_level'][level] = {
                    'nodes': 0,
                    'compressed': 0,
                    'original_size': 0,
                    'compressed_size': 0
                }
            
            stats['by_level'][level]['nodes'] += 1
            stats['by_level'][level]['original_size'] += node_stats['original_size']
            stats['by_level'][level]['compressed_size'] += node_stats['compressed_size']
            
            if node_stats['compressed']:
                stats['compressed_nodes'] += 1
                stats['by_level'][level]['compressed'] += 1
            
            stats['total_original_size'] += node_stats['original_size']
            stats['total_compressed_size'] += node_stats['compressed_size']
            
            for child in node.children:
                collect_stats(child, level + 1)
        
        collect_stats(self.root, 0)
        
        if stats['total_original_size'] > 0:
            stats['overall_compression_ratio'] = stats['total_compressed_size'] / stats['total_original_size']
        else:
            stats['overall_compression_ratio'] = 1.0
        
        return stats