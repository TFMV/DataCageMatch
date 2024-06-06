import yaml
import pandas as pd
import duckdb
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

    print(f"Table aliases: {table_aliases}")
    
    if 'JOIN' in query_parts:
        left_table_alias = query_parts[query_parts.index('JOIN') - 1]
        right_table_alias = query_parts[query_parts.index('JOIN') + 1]
        
        left_table_name = table_aliases.get(left_table_alias, left_table_alias)
        right_table_name = table_aliases.get(right_table_alias, right_table_alias)

        print(f"Left table name: {left_table_name}, Right table name: {right_table_name}")
        print(f"Available data keys: {data.keys()}")
        
        left_table_df = data[left_table_name]
        right_table_df = data[right_table_name]
        
        merged_df = left_table_df.merge(right_table_df, left_on='o_orderkey', right_on='l_orderkey')
        filtered_df = merged_df[merged_df['l_quantity'] > 30]
        result = filtered_df.shape[0]
    else:
        table_alias = query_parts[query_parts.index('FROM') + 1]
        table_name = table_aliases.get(table_alias, table_alias)

        print(f"Table name: {table_name}")
        print(f"Available data keys: {data.keys()}")
        
        table_df = data[table_name]

        # Convert the o_orderdate column to datetime if it exists
        if 'o_orderdate' in table_df.columns:
            table_df['o_orderdate'] = pd.to_datetime(table_df['o_orderdate'])

        # Implement the query using pandas methods
        if "COUNT(*)" in query and 'o_orderdate' in table_df.columns:
            filtered_df = table_df[(table_df['o_orderdate'] >= '1995-01-01') & (table_df['o_orderdate'] <= '1995-12-31')]
            result = len(filtered_df), filtered_df['o_totalprice'].sum()
        elif "GROUP BY" in query:
            grouped_df = table_df.groupby(['l_returnflag', 'l_linestatus']).agg({'l_quantity': 'sum', 'l_extendedprice': 'sum'}).reset_index()
            result = grouped_df
        else:
            result = None  # Add other conditions as needed
    
    end_time = time.time()
    return result, end_time - start_time

def run_experiment_duckdb(query, conn):
    start_time = time.time()
    result = conn.execute(query).fetchdf()
    end_time = time.time()
    return result, end_time - start_time

def main():
    config = load_config()
    data = {}
    load_times = {}
    duckdb_conn = duckdb.connect(database=':memory:')

    # Load data with Pandas and DuckDB
    for table in config['tables']:
        print(f"Loading {table} data...")
        data[table], pandas_load_time = load_data_pandas(config['data_path'], table)
        duckdb_load_time = load_data_duckdb(config['data_path'], table, duckdb_conn)
        load_times[table] = {"pandas": pandas_load_time, "duckdb": duckdb_load_time}
    
    print(f"Data loaded: {list(data.keys())}")
    
    results = []

    for experiment in config['experiments']:
        tables_in_experiment = [tbl.strip() for tbl in experiment['table'].split(",")]
        for table in tables_in_experiment:
            if table in data:
                print(f"Running experiment {experiment['name']} with Pandas...")
                pandas_query = experiment['query']
                pandas_result, pandas_time = run_experiment_pandas(pandas_query, data, tables_in_experiment)
                print(f"Pandas result: {pandas_result}\nTime taken: {pandas_time} seconds")
                
                print(f"Running experiment {experiment['name']} with DuckDB...")
                duckdb_query = experiment['query']
                try:
                    duckdb_result, duckdb_time = run_experiment_duckdb(duckdb_query, duckdb_conn)
                    print(f"DuckDB result: {duckdb_result}\nTime taken: {duckdb_time} seconds")
                except Exception as e:
                    print(f"DuckDB error: {e}")
                    duckdb_result, duckdb_time = None, None
                
                results.append({
                    "experiment": experiment['name'],
                    "pandas_result": str(pandas_result),
                    "pandas_time": pandas_time,
                    "duckdb_result": str(duckdb_result),
                    "duckdb_time": duckdb_time
                })
            else:
                print(f"Table {table} not found in loaded data.")

    # Print results in a table format
    print("\nResults Summary:")
    print(f"{'Experiment':<25} {'Pandas Time (s)':<15} {'DuckDB Time (s)':<15} {'Pandas Result':<50} {'DuckDB Result':<50}")
    print("="*155)
    for result in results:
        print(f"{result['experiment']:<25} {result['pandas_time']:<15.6f} {result['duckdb_time']:<15.6f} {result['pandas_result']:<50} {result['duckdb_result']:<50}")

    print("\nLoad Times:")
    print(f"{'Table':<15} {'Pandas Load Time (s)':<20} {'DuckDB Load Time (s)':<20}")
    print("="*55)
    for table, times in load_times.items():
        print(f"{table:<15} {times['pandas']:<20.6f} {times['duckdb']:<20.6f}")

if __name__ == "__main__":
    main()
