# 1. duckdb-as-our-sql-engine

Date: 2025-03-xx

## Status

Accepted

## Context

There are many ways to skin a cat. When approaching the problem of addressing matching and cleaning, we needed a robust, efficient and performant way of processing large datasets of addresses. We had a similar problem in Splink, where we needed to process large quantities of data efficiently

## Decision

After evaluating various options, we decided to use DuckDB as our SQL engine:
- DuckDB is an in-process SQL OLAP database management system. Users
- It is incredibly lightweight and easy to set up, requiring no separate server process.
- DuckDB is designed for analytical query workloads, making it well-suited for our data processing needs.
- It supports a wide range of SQL features, including complex joins, window functions, aggregation functions and string comparisons which will all play a role in our linking solution.
- DuckDB has strong support for various data formats and sources, including Parquet, CSV, and JSON, making it easy to integrate with common data storage solutions.
- Simple and powerful support for C/C++ UDFs, which allows us to extend its functionality as needed.

## Consequences

- Using DuckDB limits us to a single machine's resources, which may be a constraint for extremely large datasets. In Splink we have got around this by offering users choices for backends, but in UK Address Matcher we are focusing on DuckDB for simplicity.
- DuckDB's in-process nature means that it may not be suitable for multi-user environments or scenarios requiring high concurrency.
