import itertools
from collections.abc import Generator

def write_batches_to_disk(data_generator: Generator, batch_size: int=10000):
    batch_count = 0
    while True:
        batch = list(itertools.islice(data_generator, batch_size))
        if not batch: break
        # write_to_csv_or_parquet(batch)
        batch_count += 1