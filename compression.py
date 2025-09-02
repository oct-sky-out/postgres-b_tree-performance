"""
PostgreSQL-style page-level compression implementation

This module implements various compression techniques used in PostgreSQL:
- Prefix compression for keys with common prefixes
- Dictionary compression for frequently occurring values
- Delta encoding for sequential numeric keys
- Run-length encoding for repeated values
- TOAST-style compression for large values
"""

import zlib
import pickle
from typing import Any, Dict, List, Tuple, Union, Optional
from abc import ABC, abstractmethod
from collections import Counter, defaultdict
import struct


class CompressionStrategy(ABC):
    """Abstract base class for compression strategies"""
    
    @abstractmethod
    def compress(self, data: List[Any]) -> Tuple[bytes, Dict[str, Any]]:
        """Compress data and return compressed bytes + metadata"""
        pass
    
    @abstractmethod
    def decompress(self, compressed_data: bytes, metadata: Dict[str, Any]) -> List[Any]:
        """Decompress data using metadata"""
        pass
    
    @abstractmethod
    def estimate_compression_ratio(self, data: List[Any]) -> float:
        """Estimate compression ratio (0-1, lower is better compression)"""
        pass


class PrefixCompression(CompressionStrategy):
    """
    PostgreSQL-style prefix compression
    
    Removes common prefixes from string keys, similar to PostgreSQL's
    index tuple header compression.
    """
    
    def compress(self, data: List[Any]) -> Tuple[bytes, Dict[str, Any]]:
        if not data or not all(isinstance(item, str) for item in data):
            # Fallback to no compression for non-string data
            return pickle.dumps(data), {"compression": "none"}
        
        # Find common prefix
        if len(data) == 1:
            common_prefix = ""
        else:
            common_prefix = self._find_common_prefix(data)
        
        # Remove prefix from all strings
        suffixes = [s[len(common_prefix):] for s in data]
        
        # Serialize
        compressed_data = {
            "prefix": common_prefix,
            "suffixes": suffixes
        }
        
        return pickle.dumps(compressed_data), {"compression": "prefix", "prefix_len": len(common_prefix)}
    
    def decompress(self, compressed_data: bytes, metadata: Dict[str, Any]) -> List[Any]:
        if metadata.get("compression") == "none":
            return pickle.loads(compressed_data)
        
        data = pickle.loads(compressed_data)
        prefix = data["prefix"]
        suffixes = data["suffixes"]
        
        return [prefix + suffix for suffix in suffixes]
    
    def estimate_compression_ratio(self, data: List[Any]) -> float:
        if not data or not all(isinstance(item, str) for item in data):
            return 1.0  # No compression possible
        
        original_size = sum(len(s) for s in data)
        if original_size == 0:
            return 1.0
        
        common_prefix = self._find_common_prefix(data)
        compressed_size = len(common_prefix) + sum(len(s) - len(common_prefix) for s in data)
        
        return compressed_size / original_size
    
    def _find_common_prefix(self, strings: List[str]) -> str:
        """Find the longest common prefix among strings"""
        if not strings:
            return ""
        
        min_str = min(strings)
        max_str = max(strings)
        
        for i, char in enumerate(min_str):
            if char != max_str[i]:
                return min_str[:i]
        
        return min_str


class DictionaryCompression(CompressionStrategy):
    """
    PostgreSQL-style dictionary compression
    
    Creates a dictionary of frequently occurring values and replaces
    them with short references, similar to PostgreSQL's TOAST compression.
    """
    
    def __init__(self, min_frequency: int = 2, max_dict_size: int = 256):
        self.min_frequency = min_frequency
        self.max_dict_size = max_dict_size
    
    def compress(self, data: List[Any]) -> Tuple[bytes, Dict[str, Any]]:
        # Count frequencies
        counter = Counter(data)
        
        # Build dictionary of frequent items
        frequent_items = {
            item: idx for idx, (item, count) in enumerate(
                counter.most_common(self.max_dict_size)
            ) if count >= self.min_frequency
        }
        
        if not frequent_items:
            # No compression beneficial
            return pickle.dumps(data), {"compression": "none"}
        
        # Create reverse dictionary
        reverse_dict = {idx: item for item, idx in frequent_items.items()}
        
        # Encode data
        encoded_data = []
        for item in data:
            if item in frequent_items:
                encoded_data.append(("ref", frequent_items[item]))
            else:
                encoded_data.append(("val", item))
        
        compressed_data = {
            "dictionary": reverse_dict,
            "encoded": encoded_data
        }
        
        return pickle.dumps(compressed_data), {
            "compression": "dictionary", 
            "dict_size": len(frequent_items)
        }
    
    def decompress(self, compressed_data: bytes, metadata: Dict[str, Any]) -> List[Any]:
        if metadata.get("compression") == "none":
            return pickle.loads(compressed_data)
        
        data = pickle.loads(compressed_data)
        dictionary = data["dictionary"]
        encoded = data["encoded"]
        
        result = []
        for entry_type, value in encoded:
            if entry_type == "ref":
                result.append(dictionary[value])
            else:
                result.append(value)
        
        return result
    
    def estimate_compression_ratio(self, data: List[Any]) -> float:
        counter = Counter(data)
        frequent_items = [
            (item, count) for item, count in counter.most_common(self.max_dict_size)
            if count >= self.min_frequency
        ]
        
        if not frequent_items:
            return 1.0
        
        # Estimate savings: each frequent item reference saves space
        original_size = len(pickle.dumps(data))
        
        # Simplified estimation
        total_savings = sum(count - 1 for _, count in frequent_items)
        estimated_compressed_size = original_size - (total_savings * 0.5)  # Rough estimate
        
        return max(0.1, estimated_compressed_size / original_size)


class DeltaCompression(CompressionStrategy):
    """
    Delta encoding for sequential numeric data
    
    Stores only differences between consecutive values,
    similar to PostgreSQL's numeric compression.
    """
    
    def compress(self, data: List[Any]) -> Tuple[bytes, Dict[str, Any]]:
        if not data or not all(isinstance(item, (int, float)) for item in data):
            return pickle.dumps(data), {"compression": "none"}
        
        if len(data) <= 1:
            return pickle.dumps(data), {"compression": "none"}
        
        # Calculate deltas
        base_value = data[0]
        deltas = [data[i] - data[i-1] for i in range(1, len(data))]
        
        compressed_data = {
            "base": base_value,
            "deltas": deltas
        }
        
        return pickle.dumps(compressed_data), {
            "compression": "delta", 
            "base_value": base_value,
            "delta_count": len(deltas)
        }
    
    def decompress(self, compressed_data: bytes, metadata: Dict[str, Any]) -> List[Any]:
        if metadata.get("compression") == "none":
            return pickle.loads(compressed_data)
        
        data = pickle.loads(compressed_data)
        base = data["base"]
        deltas = data["deltas"]
        
        result = [base]
        current = base
        
        for delta in deltas:
            current += delta
            result.append(current)
        
        return result
    
    def estimate_compression_ratio(self, data: List[Any]) -> float:
        if not data or not all(isinstance(item, (int, float)) for item in data) or len(data) <= 1:
            return 1.0
        
        # Check if data is reasonably sequential (small deltas)
        deltas = [abs(data[i] - data[i-1]) for i in range(1, len(data))]
        avg_delta = sum(deltas) / len(deltas)
        max_value = max(abs(x) for x in data)
        
        # If deltas are much smaller than values, compression is beneficial
        if max_value > 0 and avg_delta / max_value < 0.1:
            return 0.3  # Good compression
        else:
            return 0.8  # Poor compression


class RunLengthCompression(CompressionStrategy):
    """
    Run-length encoding for repeated values
    
    Encodes consecutive identical values as (value, count) pairs.
    """
    
    def compress(self, data: List[Any]) -> Tuple[bytes, Dict[str, Any]]:
        if not data:
            return pickle.dumps([]), {"compression": "rle", "runs": 0}
        
        # Encode runs
        runs = []
        current_value = data[0]
        current_count = 1
        
        for i in range(1, len(data)):
            if data[i] == current_value:
                current_count += 1
            else:
                runs.append((current_value, current_count))
                current_value = data[i]
                current_count = 1
        
        # Add final run
        runs.append((current_value, current_count))
        
        return pickle.dumps(runs), {
            "compression": "rle", 
            "runs": len(runs),
            "original_length": len(data)
        }
    
    def decompress(self, compressed_data: bytes, metadata: Dict[str, Any]) -> List[Any]:
        runs = pickle.loads(compressed_data)
        
        result = []
        for value, count in runs:
            result.extend([value] * count)
        
        return result
    
    def estimate_compression_ratio(self, data: List[Any]) -> float:
        if not data:
            return 1.0
        
        # Count runs
        runs = 1
        for i in range(1, len(data)):
            if data[i] != data[i-1]:
                runs += 1
        
        # If we have many runs, compression isn't beneficial
        compression_ratio = (runs * 2) / len(data)  # Each run takes 2 units (value, count)
        return min(1.0, compression_ratio)


class TOASTCompression(CompressionStrategy):
    """
    TOAST-style compression for large values using zlib
    
    Similar to PostgreSQL's TOAST (The Oversized-Attribute Storage Technique)
    """
    
    def __init__(self, min_size_threshold: int = 100):
        self.min_size_threshold = min_size_threshold
    
    def compress(self, data: List[Any]) -> Tuple[bytes, Dict[str, Any]]:
        # Serialize first to check size
        serialized = pickle.dumps(data)
        
        if len(serialized) < self.min_size_threshold:
            return serialized, {"compression": "none"}
        
        # Use zlib compression
        compressed = zlib.compress(serialized, level=6)
        
        return compressed, {
            "compression": "toast",
            "original_size": len(serialized),
            "compressed_size": len(compressed)
        }
    
    def decompress(self, compressed_data: bytes, metadata: Dict[str, Any]) -> List[Any]:
        if metadata.get("compression") == "none":
            return pickle.loads(compressed_data)
        
        # Decompress with zlib
        decompressed = zlib.decompress(compressed_data)
        return pickle.loads(decompressed)
    
    def estimate_compression_ratio(self, data: List[Any]) -> float:
        serialized = pickle.dumps(data)
        if len(serialized) < self.min_size_threshold:
            return 1.0
        
        # Quick estimation using zlib at fast compression
        compressed = zlib.compress(serialized, level=1)
        return len(compressed) / len(serialized)


class CompressionManager:
    """
    Manages multiple compression strategies and selects the best one
    
    Similar to PostgreSQL's adaptive compression selection
    """
    
    def __init__(self):
        self.strategies = {
            "prefix": PrefixCompression(),
            "dictionary": DictionaryCompression(),
            "delta": DeltaCompression(),
            "rle": RunLengthCompression(),
            "toast": TOASTCompression()
        }
    
    def compress(self, data: List[Any]) -> Tuple[bytes, Dict[str, Any]]:
        """Select best compression strategy and compress data"""
        if not data:
            return pickle.dumps([]), {"compression": "none"}
        
        best_strategy = None
        best_ratio = 1.0
        best_name = "none"
        
        # Test all strategies and pick the best one
        for name, strategy in self.strategies.items():
            try:
                ratio = strategy.estimate_compression_ratio(data)
                if ratio < best_ratio:
                    best_ratio = ratio
                    best_strategy = strategy
                    best_name = name
            except Exception:
                continue  # Skip strategies that fail
        
        if best_strategy is None:
            # Fallback to no compression
            return pickle.dumps(data), {"compression": "none"}
        
        try:
            compressed_data, metadata = best_strategy.compress(data)
            metadata["strategy"] = best_name
            metadata["estimated_ratio"] = best_ratio
            return compressed_data, metadata
        except Exception:
            # Fallback to no compression
            return pickle.dumps(data), {"compression": "none"}
    
    def decompress(self, compressed_data: bytes, metadata: Dict[str, Any]) -> List[Any]:
        """Decompress data using stored metadata"""
        compression_type = metadata.get("compression", "none")
        strategy_name = metadata.get("strategy", compression_type)
        
        if compression_type == "none" or strategy_name not in self.strategies:
            return pickle.loads(compressed_data)
        
        strategy = self.strategies[strategy_name]
        return strategy.decompress(compressed_data, metadata)
    
    def get_compression_stats(self, data: List[Any]) -> Dict[str, Dict[str, Any]]:
        """Get compression statistics for all strategies"""
        stats = {}
        
        for name, strategy in self.strategies.items():
            try:
                ratio = strategy.estimate_compression_ratio(data)
                compressed_data, metadata = strategy.compress(data)
                
                stats[name] = {
                    "estimated_ratio": ratio,
                    "actual_compressed_size": len(compressed_data),
                    "metadata": metadata
                }
            except Exception as e:
                stats[name] = {
                    "error": str(e),
                    "estimated_ratio": 1.0
                }
        
        return stats