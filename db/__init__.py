# MA Data Platform - Database Layer
#
# DuckDB-based query engine reading from S3 Parquet files.
# All queries are audited for lineage tracking.
#
# Usage:
#     from db import query, get_engine
#
#     # Simple query
#     df = query("SELECT * FROM agg_industry_totals")
#
#     # With audit
#     engine = get_engine()
#     df, audit_id = engine.query_with_audit(sql, user_id="api")

from .duckdb_layer import (
    MAQueryEngine,
    query,
    get_engine,
)

__all__ = ['MAQueryEngine', 'query', 'get_engine']
