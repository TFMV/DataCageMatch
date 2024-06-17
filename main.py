import yaml
import pandas as pd
import duckdb
import polars as pl
import time
import os
import matplotlib.pyplot as plt
from fpdf import FPDF

def load_config(config_path='config.yaml'):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def load_data_pandas(data_path, table_name):
    start_time = time.time()
    file_prefix = 'order' if table_name == 'orders' else table_name
    files = [os.path.join(data_path, f) for f in os.listdir(data_path) if f.startswith(file_prefix)]
    dfs = [pd.read_parquet(file) for file in files]
    data = pd.concat(dfs, ignore_index=True)
    if 'o_orderdate' in data.columns:
        data['o_orderdate'] = pd.to_datetime(data['o_orderdate'])
    return data, time.time() - start_time

def load_data_duckdb(data_path, table_name, conn):
    start_time = time.time()
    file_prefix = 'order' if table_name == 'orders' else table_name
    files = [os.path.join(data_path, f) for f in os.listdir(data_path) if f.startswith(file_prefix)]
    conn.execute(f"""
        CREATE TABLE {table_name} AS
        SELECT * FROM read_parquet([{', '.join(f"'{file}'" for file in files)}])
    """)
    return time.time() - start_time

def load_data_polars(data_path, table_name):
    start_time = time.time()
    file_prefix = 'order' if table_name == 'orders' else table_name
    files = [os.path.join(data_path, f) for f in os.listdir(data_path) if f.startswith(file_prefix)]
    dfs = [pl.read_parquet(file) for file in files]
    data = pl.concat(dfs)
    if 'o_orderdate' in data.columns:
        data = data.with_columns(pl.col('o_orderdate').cast(pl.Utf8).str.strptime(pl.Date, "%Y-%m-%d"))
    return data, time.time() - start_time

def run_experiment_pandas(query, data, table_name):
    start_time = time.time()
    result = pd.DataFrame()
    if "JOIN" in query:
        left_table, right_table = query.split("JOIN")[0].strip().split("FROM")[1].strip(), query.split("JOIN")[1].strip()
        left_table, right_table = left_table.split()[0], right_table.split()[0]
        merged_df = data[left_table].merge(data[right_table], left_on='o_orderkey', right_on='l_orderkey')
        result = merged_df.query('l_quantity > 30')
    elif "GROUP BY" in query:
        table_name = query.split("FROM")[1].strip().split()[0]
        table_df = data[table_name]
        result = table_df.groupby(['l_returnflag', 'l_linestatus']).agg({'l_quantity': 'sum', 'l_extendedprice': 'sum'}).reset_index()
    elif "COUNT(*)" in query:
        table_name = query.split("FROM")[1].strip().split()[0]
        table_df = data[table_name]
        result = table_df.query("o_orderdate >= '1995-01-01' and o_orderdate <= '1995-12-31'")
        result = len(result), result['o_totalprice'].sum()
    return result, time.time() - start_time

def run_experiment_duckdb(query, conn):
    start_time = time.time()
    result = conn.execute(query).fetchdf()
    return result, time.time() - start_time

def run_experiment_polars(query, data):
    start_time = time.time()
    result = None
    if "JOIN" in query:
        left_table, right_table = query.split("JOIN")[0].strip().split("FROM")[1].strip(), query.split("JOIN")[1].strip()
        left_table, right_table = left_table.split()[0] + "_polars", right_table.split()[0] + "_polars"
        merged_df = data[left_table].join(data[right_table], left_on='o_orderkey', right_on='l_orderkey')
        result = merged_df.filter(pl.col('l_quantity') > 30)
    elif "GROUP BY" in query:
        table_name = query.split("FROM")[1].strip().split()[0] + "_polars"
        result = data[table_name].groupby(['l_returnflag', 'l_linestatus']).agg(pl.sum('l_quantity'), pl.sum('l_extendedprice'))
    elif "COUNT(*)" in query:
        table_name = query.split("FROM")[1].strip().split()[0] + "_polars"
        result = data[table_name].filter((pl.col('o_orderdate') >= pl.date(1995, 1, 1)) & (pl.col('o_orderdate') <= pl.date(1995, 12, 31)))
        result = len(result), result.select(pl.sum('o_totalprice')).item()
    return result, time.time() - start_time

def plot_times(load_times, results):
    # Plot load times
    fig, ax = plt.subplots()
    table_names = list(load_times.keys())
    libraries = ['pandas', 'duckdb', 'polars']
    for lib in libraries:
        times = [load_times[table][lib] for table in table_names]
        ax.plot(table_names, times, marker='o', label=f'{lib} load time')
    ax.set_xlabel('Tables')
    ax.set_ylabel('Time (s)')
    ax.set_title('Data Load Times')
    ax.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('load_times.png')
    plt.close()

    # Plot experiment times
    fig, ax = plt.subplots()
    experiments = [result['experiment'] for result in results]
    for lib in libraries:
        times = [result[f'{lib}_time'] for result in results]
        ax.plot(experiments, times, marker='o', label=f'{lib} query time')
    ax.set_xlabel('Experiments')
    ax.set_ylabel('Time (s)')
    ax.set_title('Query Execution Times')
    ax.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('query_times.png')
    plt.close()

def save_to_pdf():
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", size = 12)
    pdf.cell(200, 10, txt = "Data Load and Query Execution Times", ln = True, align = 'C')
    
    pdf.image("load_times.png", x = 10, y = 20, w = 180)
    pdf.add_page()
    pdf.image("query_times.png", x = 10, y = 20, w = 180)
    
    pdf.output("report.pdf")

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
        table_name = experiment['table'].split(",")[0].strip()
        pandas_result, pandas_time = run_experiment_pandas(experiment['query'], data, table_name)
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

    plot_times(load_times, results)
    save_to_pdf()

if __name__ == "__main__":
    main()
