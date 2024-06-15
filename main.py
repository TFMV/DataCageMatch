import yaml
import pandas as pd
import duckdb
import polars as pl
import time
import os

def load_config(config_path='config.yaml'):
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

def load_data_pandas(data_path, table_name):
    start_time = time.time()
    file_prefix = 'order' if table_name == 'orders' else table_name
    files = [os.path.join(data_path, f) for f in os.listdir(data_path) if f.startswith(file_prefix)]
    dfs = [pd.read_parquet(file) for file in files]
    data = pd.concat(dfs, ignore_index=True)
    end_time = time.time()
    load_time = end_time - start_time
    print(f"Time taken to load {table_name} data with Pandas: {load_time} seconds")
    return data, load_time

def load_data_duckdb(data_path, table_name, conn):
    start_time = time.time()
    file_prefix = 'order' if table_name == 'orders' else table_name
    files = [os.path.join(data_path, f) for f in os.listdir(data_path) if f.startswith(file_prefix)]
    conn.execute(f"""
        CREATE TABLE {table_name} AS
        SELECT * FROM read_parquet([{', '.join(f"'{file}'" for file in files)}])
    """)
    end_time = time.time()
    load_time = end_time - start_time
    print(f"Time taken to load {table_name} data with DuckDB: {load_time} seconds")
    return load_time

def load_data_polars(data_path, table_name):
    start_time = time.time()
    file_prefix = 'order' if table_name == 'orders' else table_name
    files = [os.path.join(data_path, f) for f in os.listdir(data_path) if f.startswith(file_prefix)]
    dfs = [pl.read_parquet(file) for file in files]
    data = pl.concat(dfs)
    
    # Ensure o_orderdate is a datetime
    if 'o_orderdate' in data.columns:
        data = data.with_columns(pl.col('o_orderdate').cast(pl.Utf8).str.strptime(pl.Date, "%Y-%m-%d", strict=False))
    
    end_time = time.time()
    load_time = end_time - start_time
    print(f"Time taken to load {table_name} data with Polars: {load_time} seconds")
    return data, load_time

def run_experiment_pandas(query, data, table_names):
    start_time = time.time()
    
    query_parts = query.split()
    table_aliases = {}
    for i, part in enumerate(query_parts):
        if part.upper() in ['FROM', 'JOIN']:
            table_name = query_parts[i + 1]
            if i + 2 < len(query_parts):
                alias = query_parts[i + 2]
                if alias not in ['WHERE', 'ON', 'ORDER', 'GROUP']:
                    table_aliases[alias] = table_name

    if 'JOIN' in query_parts:
        left_table_alias = query_parts[query_parts.index('JOIN') - 1]
        right_table_alias = query_parts[query_parts.index('JOIN') + 1]
        
        left_table_name = table_aliases.get(left_table_alias, left_table_alias)
        right_table_name = table_aliases.get(right_table_alias, right_table_alias)

        left_table_df = data[left_table_name]
        right_table_df = data[right_table_name]
        
        merged_df = left_table_df.merge(right_table_df, left_on='o_orderkey', right_on='l_orderkey')
        filtered_df = merged_df[merged_df['l_quantity'] > 30]
        result = filtered_df.shape[0]
    else:
        table_alias = query_parts[query_parts.index('FROM') + 1]
        table_name = table_aliases.get(table_alias, table_alias)
        
        table_df = data[table_name]

        if 'o_orderdate' in table_df.columns:
            table_df['o_orderdate'] = pd.to_datetime(table_df['o_orderdate'])

        if "COUNT(*)" in query and 'o_orderdate' in table_df.columns:
            filtered_df = table_df[(table_df['o_orderdate'] >= '1995-01-01') & (table_df['o_orderdate'] <= '1995-12-31')]
            result = len(filtered_df), filtered_df['o_totalprice'].sum()
        elif "GROUP BY" in query:
            grouped_df = table_df.groupby(['l_returnflag', 'l_linestatus']).agg({'l_quantity': 'sum', 'l_extendedprice': 'sum'}).reset_index()
            result = grouped_df
        else:
            result = None
    
    end_time = time.time()
    return result, end_time - start_time

def run_experiment_duckdb(query, conn):
    start_time = time.time()
    result = conn.execute(query).fetchdf()
    end_time = time.time()
    return result, end_time - start_time

def run_experiment_polars(query, data):
    start_time = time.time()
    result = None

    query_parts = query.split()
    table_aliases = {}
    for i, part in enumerate(query_parts):
        if part.upper() in ['FROM', 'JOIN']:
            table_name = query_parts[i + 1]
            if i + 2 < len(query_parts):
                alias = query_parts[i + 2]
                if alias not in ['WHERE', 'ON', 'ORDER', 'GROUP']:
                    table_aliases[alias] = table_name

    if "JOIN" in query_parts:
        left_table_alias = query_parts[query_parts.index('JOIN') - 1]
        right_table_alias = query_parts[query_parts.index('JOIN') + 1]
        
        left_table_name = table_aliases.get(left_table_alias, left_table_alias) + "_polars"
        right_table_name = table_aliases.get(right_table_alias, right_table_alias) + "_polars"

        left_table = data[left_table_name]
        right_table = data[right_table_name]

        merged_df = left_table.join(right_table, left_on='o_orderkey', right_on='l_orderkey')
        filtered_df = merged_df.filter(pl.col('l_quantity') > 30)
        result = filtered_df.shape[0]
    else:
        table_alias = query_parts[query_parts.index('FROM') + 1]
        table_name = table_aliases.get(table_alias, table_alias) + "_polars"
        
        table_df = data[table_name]

        if "COUNT(*)" in query and 'o_orderdate' in table_df.columns:
            filtered_df = table_df.filter((pl.col('o_orderdate') >= pl.date(1995, 1, 1)) & (pl.col('o_orderdate') <= pl.date(1995, 12, 31)))
            result = len(filtered_df), filtered_df.select(pl.sum('o_totalprice')).item()
        elif "GROUP BY" in query:
            grouped_df = table_df.groupby(['l_returnflag', 'l_linestatus']).agg(pl.sum('l_quantity'), pl.sum('l_extendedprice'))
            result = grouped_df
        else:
            result = None

    end_time = time.time()
    return result, end_time - start_time

def main():
    config = load_config()
    data = {}
    load_times = {}
    duckdb_conn = duckdb.connect(database=':memory:')

    for table in config['tables']:
        data[table], pandas_load_time = load_data_pandas(config['data_path'], table)
        duckdb_load_time = load_data_duckdb(config['data_path'], table, duckdb_conn)
        polars_data, polars_load_time = load_data_polars(config['data_path'], table)
        data[table + "_polars"] = polars_data
        load_times[table] = {"pandas": pandas_load_time, "duckdb": duckdb_load_time, "polars": polars_load_time}
    
    results = []

    for experiment in config['experiments']:
        tables_in_experiment = [tbl.strip() for tbl in experiment['table'].split(",")]
        for table in tables_in_experiment:
            if table in data:
                pandas_result, pandas_time = run_experiment_pandas(experiment['query'], data, tables_in_experiment)
                
                duckdb_result, duckdb_time = run_experiment_duckdb(experiment['query'], duckdb_conn)
                
                polars_result, polars_time = run_experiment_polars(experiment['query'], data)
                
                results.append({
                    "experiment": experiment['name'],
                    "pandas_result": str(pandas_result),
                    "pandas_time": pandas_time,
                    "duckdb_result": str(duckdb_result),
                    "duckdb_time": duckdb_time,
                    "polars_result": str(polars_result),
                    "polars_time": polars_time
                })
            else:
                print(f"Table {table} not found in loaded data.")

    print("\nResults Summary:")
    print(f"{'Experiment':<25} {'Pandas Time (s)':<15} {'DuckDB Time (s)':<15} {'Polars Time (s)':<15} {'Pandas Result':<50} {'DuckDB Result':<50} {'Polars Result':<50}")
    print("="*200)
    for result in results:
        print(f"{result['experiment']:<25} {result['pandas_time']:<15.6f} {result['duckdb_time']:<15.6f} {result['polars_time']:<15.6f} {result['pandas_result']:<50} {result['duckdb_result']:<50} {result['polars_result']:<50}")

    print("\nLoad Times:")
    print(f"{'Table':<15} {'Pandas Load Time (s)':<20} {'DuckDB Load Time (s)':<20} {'Polars Load Time (s)':<20}")
    print("="*75)
    for table, times in load_times.items():
        print(f"{table:<15} {times['pandas']:<20.6f} {times['duckdb']:<20.6f} {times['polars']:<20.6f}")

if __name__ == "__main__":
    main()
