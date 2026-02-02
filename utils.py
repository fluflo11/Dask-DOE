import random
import csv
import pandas as pd
import numpy as np
import os
import shutil
# NEW FUNCTIONS GENERATED WITH GEMINI 3
# --- ORIGINAL FUNCTIONS ---

def generate_boat_data(size):
    ports = ['BREST', 'VALENCIA', 'PALERMO', 'BRIGHTON', 'AMSTERDAM']
    return {
        'ID': range(size),
        'Speed': [random.randint(0, 15) for _ in range(size)],
        'Destination': [random.choice(ports) for _ in range(size)]
    }

def generate_test_files(case):
    if case == 0 :
        for i in range(500):
            pd.DataFrame(generate_boat_data(50000)).to_csv(f'small_boat_{i}.csv', index=False)
    else:
           pd.DataFrame(generate_boat_data(100000000)).to_csv(f'large_boat.csv', index=False)

def generate_big_csv(file, rows, chunk_size=1_000_000):
    ports = ['BREST', 'VALENCIA', 'PALERMO', 'BRIGHTON', 'AMSTERDAM']

    with open(file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['ID', 'Speed', 'Destination'])

        rows_written = 0
        while rows_written < rows:
            rows_to_write = min(chunk_size, rows - rows_written)
            data_chunk = [
                [rows_written + i, random.randint(0, 15), random.choice(ports)]
                for i in range(rows_to_write)
            ]
            writer.writerows(data_chunk)
            rows_written += rows_to_write

# --- FINAL FUNCTION ---

def generate_doe_dataset(size_label, data_type, file_type, output_dir="benchmark_data"):
    """
    Args:
        size_label (str): 'Small' or '20GB' (Factor B)
        data_type (str): 'int' or 'float' (Factor D)
        file_type (str): 'csv' or 'parquet' (Factor E)
    """
    
    # 1. Define size (Estimated number of rows)
    # Estimation: 1 row ~= 30-40 bytes. 
    # Small (e.g., 1GB) ~= 25 Million rows (similar to your loop of 500 files)
    # 20GB ~= 600 Million rows
    if size_label == '20GB':
        total_rows = 600_000_000 
    else: # Small
        total_rows = 25_000_000

    chunk_size = 5_000_000 # Process in 5M chunks to avoid RAM saturation
    ports = ['BREST', 'VALENCIA', 'PALERMO', 'BRIGHTON', 'AMSTERDAM']
    
    # Prepare output directory
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Base filename
    base_name = f"data_{size_label}_{data_type}"
    file_path = os.path.join(output_dir, base_name)

    print(f"--- Starting Generation: {size_label} ({total_rows} rows) | {data_type} | {file_type} ---")

    # If CSV, open the file only once
    csv_file_handle = None
    csv_writer = None
    if file_type == 'csv':
        full_path = file_path + ".csv"
        csv_file_handle = open(full_path, 'w', newline='', encoding='utf-8')
        csv_writer = csv.writer(csv_file_handle)
        csv_writer.writerow(['ID', 'Speed', 'Destination']) # Header

    # Chunk generation loop
    rows_written = 0
    chunk_idx = 0
    
    while rows_written < total_rows:
        current_chunk_size = min(chunk_size, total_rows - rows_written)
        #Small optimisation here that I did after generating the new functions because it was way too slow with random.randint(range) 
        ids = np.arange(rows_written, rows_written + current_chunk_size)
        
        # Data Type (Int vs Float)
        if data_type == 'float':
            # Generate floats between 0 and 15
            speeds = np.random.uniform(0, 15, current_chunk_size)
        else:
            # Generate ints between 0 and 15
            speeds = np.random.randint(0, 16, current_chunk_size)
            
        destinations = np.random.choice(ports, current_chunk_size)
        
        # Create temporary DataFrame
        df_chunk = pd.DataFrame({
            'ID': ids,
            'Speed': speeds,
            'Destination': destinations
        })
        
        # File Type
        if file_type == 'csv':
            # Simulated 'append' mode via writerows
            # Convert DF to list of lists for csv.writer (faster than df.to_csv in append mode)
            csv_writer.writerows(df_chunk.values)
            
        elif file_type == 'parquet':
            # For Parquet, write each chunk into a folder (partitioning)
            # This is the standard method for "Big Data" with Dask/Parquet
            parquet_dir = file_path + ".parquet"
            if not os.path.exists(parquet_dir):
                os.makedirs(parquet_dir)
            
            # Save the part: part_0.parquet, part_1.parquet...
            df_chunk.to_parquet(f"{parquet_dir}/part_{chunk_idx}.parquet", index=False)

        rows_written += current_chunk_size
        chunk_idx += 1
        print(f"Chunk {chunk_idx} written... ({rows_written}/{total_rows} rows)")

    # Clean close
    if csv_file_handle:
        csv_file_handle.close()
        print(f"CSV File finished: {full_path}")
    elif file_type == 'parquet':
        print(f"Parquet Dataset finished: {file_path}.parquet (Partitioned folder)")