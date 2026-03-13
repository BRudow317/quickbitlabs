# pip install python-whois
# pip install tqdm
import csv
import socket
import os
import itertools
import whois
from tqdm import tqdm

# FUNCTIONS

def get_existing_domains(full_file_path):
    """Reads the CSV to find domains that have already been processed."""
    existing = set()
    if not os.path.exists(full_file_path):
        return existing
    
    try:
        with open(full_file_path, encoding="utf-8", mode='r', newline='') as f:
            reader = csv.reader(f)
            next(reader, None) # Skip header
            for row in reader:
                if row:
                    existing.add(row[0].lower()) # Store as lowercase for comparison
    except Exception as e:
        print(f"Warning: Could not read existing file: {e}")
    
    return existing

def get_column_as_set(file_path, column_num, has_header=True):
    """gets a single column from a csv if that cell has a value"""
    out_set = set()
    if not os.path.exists(file_path):
        return out_set
    
    try:
        with open(file_path, encoding="utf-8", mode='r', newline='') as f:
            reader = csv.reader(f)
            if has_header:
                next(reader, None) # Skip header
            for row in reader:
                if row:
                    out_set.add(row[column_num].lower()) # Save cell value to out_set
    except Exception as e:
        print(f"Warning: Could not read existing file: {e}")
    
    return out_set


def is_domain_registered(domain_name):
    """Checks availability via DNS then WHOIS."""
    try:
        # 1. Fast check: DNS
        socket.gethostbyname(domain_name)
        return True
    except socket.error:
        # 2. Slow check: WHOIS
        try:
            w = whois.whois(domain_name)
            # If creation_date exists, it is registered
            if w.domain_name:
                return True
            # Sometimes whois returns None but no error if free
            return False
        except Exception:
            # If WHOIS fails (e.g. 'No match for domain'), it is likely free
            return False

def generate_patterns(pattern_array_config):
    """
    Iterates through the configuration list.
    'itertools.product(*config)' dynamically handles any number of word lists.
    Yields unique domain names based on the provided patterns.
    """
    out_set = set()
    for config in pattern_array_config:
        for parts in itertools.product(*config):
            domain = yield "".join(parts)
            domain = f"{domain}.com"
            # Simple deduplication (e.g. if 'code' is in both roots and first_only)
            if domain not in out_set:
                out_set.add(domain)
                # print(domain) # For debugging
            
    return out_set

def append_to_csv(file_path, tabular_iter):
    """append_to_csv."""
    # with open(domain_log, mode=io_mode, newline='') as file:
    try:
        file_exists = os.path.exists(file_path)
        io_mode = 'a' if file_exists else 'w'
        with open(file_path, encoding="utf-8", mode=io_mode, newline='') as file:
            writer = csv.writer(file)
            if not file_exists:
                writer.writerow(["Domain", "Status"])
            list_length = len(tabular_iter)
            counter = 0
            for row in tabular_iter:
                writer.writerow(row)
                counter += 1
                progress_bar(counter, list_length, description="Appending to CSV")
    except Exception as e:
        print(f"Error writing to CSV: {e}")

def progress_bar(cur_count, total_count, description="Processing"):
    """Example progress bar usage."""
    tqdm(range(total_count), desc=description).update(cur_count)
