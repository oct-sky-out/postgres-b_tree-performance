"""
Example usage of PostgreSQL-style B-tree implementation

This demonstrates key features that mirror PostgreSQL's B-tree index behavior:
- High fanout for efficient disk access
- Support for duplicate keys
- Range queries for efficient scans
- Statistics collection for query planning
"""

from btree import PostgreSQLBTree
import random
import time


def demonstrate_basic_operations():
    """Demonstrate basic B-tree operations"""
    print("=== Basic B-tree Operations ===")
    
    # Create B-tree with PostgreSQL-like order (256 for integer keys)
    btree = PostgreSQLBTree(order=256)
    
    # Insert some data
    data = [
        (10, "Employee John"),
        (5, "Employee Alice"),
        (15, "Employee Bob"),
        (3, "Employee Charlie"),
        (7, "Employee Diana"),
        (12, "Employee Eve"),
        (18, "Employee Frank")
    ]
    
    print("Inserting data...")
    for employee_id, name in data:
        btree.insert(employee_id, name)
        print(f"  Inserted: {employee_id} -> {name}")
    
    print(f"\nTree size: {btree.size}")
    print(f"Tree height: {btree.height}")
    
    # Search for specific keys
    print("\nSearching for keys...")
    for key in [5, 12, 20]:
        result = btree.search(key)
        print(f"  Key {key}: {result}")
    
    # Range query (PostgreSQL's strength)
    print("\nRange query [7, 15]:")
    for key, value in btree.range_query(7, 15):
        print(f"  {key} -> {value}")


def demonstrate_duplicate_keys():
    """Demonstrate PostgreSQL-style duplicate key handling"""
    print("\n=== Duplicate Key Handling ===")
    
    btree = PostgreSQLBTree(order=10)
    
    # Insert duplicate keys (common in database indexes)
    duplicates = [
        (100, "Order #001"),
        (100, "Order #002"),
        (100, "Order #003"),
        (200, "Order #004"),
        (200, "Order #005"),
        (300, "Order #006")
    ]
    
    print("Inserting orders with duplicate customer IDs...")
    for customer_id, order in duplicates:
        btree.insert(customer_id, order)
        print(f"  Customer {customer_id}: {order}")
    
    # Search for customer orders
    print("\nFinding all orders for customer 100:")
    orders = btree.search(100)
    for order in orders:
        print(f"  {order}")
    
    # Range query including duplicates
    print("\nRange query [100, 200] (includes duplicates):")
    for customer_id, order in btree.range_query(100, 200):
        print(f"  Customer {customer_id}: {order}")


def demonstrate_large_dataset():
    """Demonstrate performance with large dataset"""
    print("\n=== Large Dataset Performance ===")
    
    btree = PostgreSQLBTree(order=256)  # PostgreSQL-like fanout
    n = 10000
    
    print(f"Inserting {n:,} records...")
    start_time = time.time()
    
    # Generate realistic data (like a database table)
    for i in range(n):
        user_id = random.randint(1, n * 2)  # Some duplicates
        username = f"user_{user_id}_{i}"
        btree.insert(user_id, username)
    
    insert_time = time.time() - start_time
    print(f"  Insertion time: {insert_time:.3f} seconds")
    print(f"  Rate: {n/insert_time:,.0f} inserts/second")
    
    # Get statistics (like PostgreSQL's pg_stat_user_indexes)
    stats = btree.get_statistics()
    print(f"\nB-tree statistics:")
    print(f"  Height: {stats['height']}")
    print(f"  Leaf pages: {stats['leaf_pages']}")
    print(f"  Internal pages: {stats['internal_pages']}")
    print(f"  Total keys: {stats['total_keys']:,}")
    print(f"  Avg keys per leaf: {stats['avg_keys_per_leaf']:.1f}")
    
    # Test search performance
    print(f"\nTesting search performance...")
    search_keys = random.sample(range(1, n * 2), 1000)
    
    start_time = time.time()
    found_count = 0
    for key in search_keys:
        results = btree.search(key)
        if results:
            found_count += len(results)
    
    search_time = time.time() - start_time
    print(f"  Searched 1000 keys in {search_time:.3f} seconds")
    print(f"  Rate: {1000/search_time:,.0f} searches/second")
    print(f"  Found {found_count} total records")


def demonstrate_range_queries():
    """Demonstrate efficient range queries"""
    print("\n=== Range Query Performance ===")
    
    btree = PostgreSQLBTree(order=128)
    
    # Insert timestamp-like data (common use case in PostgreSQL)
    print("Inserting timestamp-based data...")
    base_timestamp = 1640995200  # 2022-01-01
    
    for i in range(5000):
        timestamp = base_timestamp + i * 3600  # Hourly data
        event_data = f"event_{i}"
        btree.insert(timestamp, event_data)
    
    # Range queries (like PostgreSQL WHERE timestamp BETWEEN ... AND ...)
    print("\nExecuting range queries...")
    
    # Query 1: First day's data
    day_start = base_timestamp
    day_end = base_timestamp + 24 * 3600
    
    start_time = time.time()
    day_results = list(btree.range_query(day_start, day_end))
    query_time = time.time() - start_time
    
    print(f"  Day 1 query: {len(day_results)} records in {query_time:.4f} seconds")
    
    # Query 2: Week's data
    week_start = base_timestamp
    week_end = base_timestamp + 7 * 24 * 3600
    
    start_time = time.time()
    week_results = list(btree.range_query(week_start, week_end))
    query_time = time.time() - start_time
    
    print(f"  Week 1 query: {len(week_results)} records in {query_time:.4f} seconds")
    
    # Query 3: Show some actual results
    print(f"\nSample results from day 1:")
    for i, (timestamp, event) in enumerate(day_results[:5]):
        print(f"  {timestamp}: {event}")
    if len(day_results) > 5:
        print(f"  ... and {len(day_results) - 5} more")


def demonstrate_deletion():
    """Demonstrate deletion and tree rebalancing"""
    print("\n=== Deletion and Rebalancing ===")
    
    btree = PostgreSQLBTree(order=5)  # Small order to show rebalancing
    
    # Insert data
    keys = list(range(1, 21))
    print("Inserting keys 1-20...")
    for key in keys:
        btree.insert(key, f"value_{key}")
    
    print(f"Initial tree height: {btree.height}")
    btree.print_tree()
    
    # Delete some keys
    keys_to_delete = [5, 10, 15]
    print(f"\nDeleting keys: {keys_to_delete}")
    
    for key in keys_to_delete:
        success = btree.delete(key, f"value_{key}")
        print(f"  Deleted key {key}: {'Success' if success else 'Failed'}")
    
    print(f"Final tree height: {btree.height}")
    print(f"Final tree size: {btree.size}")
    
    # Verify remaining keys
    remaining_keys = [k for k in range(1, 21) if k not in keys_to_delete]
    print(f"\nVerifying remaining {len(remaining_keys)} keys...")
    for key in remaining_keys:
        result = btree.search(key)
        assert result == [f"value_{key}"], f"Key {key} not found or incorrect"
    print("  All remaining keys verified successfully!")


if __name__ == "__main__":
    print("PostgreSQL-style B-tree Implementation Demo")
    print("=" * 50)
    
    demonstrate_basic_operations()
    demonstrate_duplicate_keys()
    demonstrate_large_dataset()
    demonstrate_range_queries()
    demonstrate_deletion()
    
    print("\n" + "=" * 50)
    print("Demo completed successfully!")
    print("\nThis implementation demonstrates key PostgreSQL B-tree features:")
    print("- High fanout (order 256) for disk efficiency")
    print("- Duplicate key support for non-unique indexes")
    print("- Efficient range queries for WHERE clauses")
    print("- Statistics collection for query planning")
    print("- Automatic rebalancing during deletions")