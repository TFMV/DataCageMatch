# Pandas vs. DuckDB Benchmarking Experiment

![pvd](assets/cage.webp)

## Overview

This project benchmarks the performance of Pandas and DuckDB for loading and querying large TPCH datasets. The aim is to compare the efficiency of both tools in terms of data loading time and query execution time for common data operations such as filtering, aggregation, and joins.

## Setup

### Dataset

Ensure the TPCH datasets (`lineitem` and `orders` tables in Parquet format) are available in the specified data directory (`data/pvd-tfmv/`).

## Configuration

Modify the `config.yaml` file to specify the data path, tables, and experiments:

```yaml
data_path: data/pvd-tfmv/
tables:
  - lineitem
  - orders
experiments:
  - name: "Filter and Aggregate"
    description: "Filter orders by a date range and aggregate total price"
    table: "orders"
    query: "SELECT COUNT(*), SUM(o_totalprice) FROM orders WHERE o_orderdate BETWEEN '1995-01-01' AND '1995-12-31'"
  - name: "Join and Filter"
    description: "Join orders and lineitem tables and filter by quantity"
    table: "orders, lineitem"
    query: "SELECT COUNT(*) FROM orders o JOIN lineitem l ON o.o_orderkey = l.l_orderkey WHERE l.l_quantity > 30"
  - name: "Group By"
    description: "Group lineitem by returnflag and linestatus"
    table: "lineitem"
    query: "SELECT l_returnflag, l_linestatus, COUNT(*), SUM(l_quantity) FROM lineitem GROUP BY l_returnflag, l_linestatus"
```
