import pandas as pd
import numpy as np
import glob
import os

def optimize_memory(df: pd.DataFrame) -> pd.DataFrame:
    """Downcasts numeric columns to save memory (Pandas 2.0+ compatible)."""
    start_mem = df.memory_usage().sum() / 1024**2
    
    for col in df.columns:
        col_type = df[col].dtype
        
        # Updated check: Check if it's NOT an object and NOT categorical
        if col_type != object and not isinstance(col_type, pd.CategoricalDtype):
            c_min = df[col].min()
            c_max = df[col].max()
            
            if str(col_type)[:3] == 'int':
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
            else:
                # For floats, we usually downcast to float32
                if c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
                    
    end_mem = df.memory_usage().sum() / 1024**2
    print(f"Memory reduced by {100 * (start_mem - end_mem) / start_mem:.2f}%")
    return df

def missing_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a summary of missing values per column."""
    stats = pd.DataFrame(df.isna().sum(), columns=['missing_count'])
    stats['percentage'] = (stats['missing_count'] / len(df)) * 100
    return stats.sort_values('percentage', ascending=False)

import re

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans column names to snake_case and removes special characters."""
    def slugify(name):
        name = name.lower().strip()
        name = re.sub(r'[^\w\s-]', '', name)
        return re.sub(r'[\s-]+', '_', name)
    
    df.columns = [slugify(c) for c in df.columns]
    return df

def remove_outliers_iqr(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """Removes outliers from specified columns using the 1.5*IQR rule."""
    for col in columns:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        df = df[(df[col] >= lower_bound) & (df[col] <= upper_bound)]
    return df

def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Flattens MultiIndex columns into a single level."""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join(map(str, col)).strip() for col in df.columns.values]
    return df

def expand_dates(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Extracts features from a datetime column."""
    df[col] = pd.to_datetime(df[col])
    df[f'{col}_year'] = df[col].dt.year
    df[f'{col}_month'] = df[col].dt.month
    df[f'{col}_day'] = df[col].dt.day
    df[f'{col}_dayofweek'] = df[col].dt.dayofweek
    df[f'{col}_is_weekend'] = df[col].dt.dayofweek.isin([5, 6]).astype(int)
    return df

def value_counts_plus(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Returns counts and percentages for a categorical column."""
    counts = df[col].value_counts()
    percent = df[col].value_counts(normalize=True) * 100
    return pd.concat([counts, percent], axis=1, keys=['count', 'percentage'])

def filter_isin(df: pd.DataFrame, col: str, values: list, exclude: bool = False) -> pd.DataFrame:
    """Filters a DataFrame by a list of values; can also perform an exclusion filter."""
    if exclude:
        return df[~df[col].isin(values)]
    return df[df[col].isin(values)]

def safe_merge(left: pd.DataFrame, right: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """Merges two dataframes and prints the shape change."""
    original_rows = len(left)
    result = pd.merge(left, right, **kwargs)
    new_rows = len(result)
    print(f"Merge complete. Row count: {original_rows} -> {new_rows} ({new_rows - original_rows:+d})")
    return result

def highlight_stats(df: pd.DataFrame):
    """Styles the dataframe to highlight max and min values."""
    return df.style.highlight_max(color='lightgreen').highlight_min(color='#ffcccb')

def load_all_csvs(directory_path: str, pattern: str = "*.csv") -> pd.DataFrame:
    """Combines all CSVs in a directory into a single DataFrame."""
    files = glob.glob(os.path.join(directory_path, pattern))
    df_list = [pd.read_csv(f) for f in files]
    return pd.concat(df_list, ignore_index=True)

def excel_to_dict(file_path: str) -> dict:
    """Reads all sheets from an Excel file into a dictionary of DataFrames."""
    return pd.read_excel(file_path, sheet_name=None)

def flatten_json_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Expands a column containing nested JSON/dicts into multiple columns."""
    flat_col = pd.json_normalize(df[column].tolist())
    flat_col.columns = [f"{column}_{c}" for c in flat_col.columns]
    return pd.concat([df.drop(column, axis=1), flat_col], axis=1)

def read_jsonl(file_path: str) -> pd.DataFrame:
    """Reads a Newline Delimited JSON (JSONL) file."""
    return pd.read_json(file_path, lines=True)

def read_fast_arrow(file_path: str) -> pd.DataFrame:
    """Reads a CSV using the PyArrow engine for speed and memory efficiency."""
    return pd.read_csv(file_path, engine='pyarrow', dtype_backend='pyarrow')

def save_as_compressed_parquet(df: pd.DataFrame, name: str):
    """Saves DataFrame as a Parquet file with Snappy compression."""
    df.to_parquet(f"{name}.parquet", compression='snappy', index=False)

def to_feather_tmp(df: pd.DataFrame, filename: str = "temp_data.feather"):
    """Saves to Feather format for lightning-fast transient storage."""
    df.to_feather(filename)
    print(f"Stored {len(df)} rows in Feather format.")

def process_by_chunks(file_path: str, chunk_size: int = 100000):
    """Processes a massive CSV in chunks to avoid MemoryErrors."""
    reader = pd.read_csv(file_path, chunksize=chunk_size)
    for i, chunk in enumerate(reader):
        # Perform your logic here (e.g., filtering or aggregating)
        print(f"Processing chunk {i} with {len(chunk)} rows...")
        # yield chunk # Use yield if you want to create a generator

def export_clean_csv(df: pd.DataFrame, filename: str):
    """Exports CSV without the index and ensures UTF-8 encoding."""
    df.to_csv(filename, index=False, encoding='utf-8')
    print(f"Successfully exported to {filename}")

def convert_dir_to_parquet(input_dir: str, output_file: str):
    """Batch converts a directory of CSVs into a single optimized Parquet file."""
    df = load_all_csvs(input_dir) # Reusing Function #11
    df.to_parquet(output_file, engine='pyarrow', compression='snappy')
    print(f"Converted directory {input_dir} to {output_file}")