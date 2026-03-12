
## https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/datafiles_prepare_csv.htm
import csv
import io
from enum import Enum
import os
import sys
from typing import Generator, Iterator, Optional, Literal



class ColumnDelimiter(str, Enum):
    BACKQUOTE = "BACKQUOTE"  # (`)
    CARET = "CARET"  # (^)
    COMMA = "COMMA"  # (,)
    PIPE = "PIPE"  # (|)
    SEMICOLON = "SEMICOLON"  # (;)
    TAB = "TAB"  # (\t)

DELIMITERS: dict[str,str] = {
    ColumnDelimiter.BACKQUOTE: "`",
    ColumnDelimiter.CARET: "^",
    ColumnDelimiter.COMMA: ",",
    ColumnDelimiter.PIPE: "|",
    ColumnDelimiter.SEMICOLON: ";",
    ColumnDelimiter.TAB: "\t",
    }

class LineEnding(str,Enum):
    LF = "LF"
    CRLF = "CRLF"
LINE_ENDINGS: dict[str,str] = {LineEnding.LF: "\n", LineEnding.CRLF: "\r\n"}

QUOTING_TYPE = Literal[0, 1, 2, 3, 4, 5]

MAX_INGEST_JOB_FILE_SIZE = 100 * 1024 * 1024
MAX_INGEST_JOB_PARALLELISM = 10  # TODO: ? Salesforce limits
DEFAULT_QUERY_PAGE_SIZE = 50000

def split_csv(
        filename: Optional[str] = None,
        records: Optional[str] = None,
        max_records: Optional[int] = None,
        line_ending: LineEnding = LineEnding.LF,
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        quoting: QUOTING_TYPE = csv.QUOTE_MINIMAL
        ) -> Generator[tuple[int, str], None, None]:
    """Split a CSV file into chunks to avoid exceeding the Salesforce
    bulk 2.0 API limits.
    Arguments:
        * filename -- csv file
        * max_records -- the number of records per chunk, None for auto size
    """
    total_records = count_csv(filename=filename,
                               skip_header=True,
                               line_ending=line_ending,
                               column_delimiter=column_delimiter,
                               quoting=quoting
                               ) 
    if filename:
        total_records = count_csv(filename=filename,
                                   skip_header=True,
                                   line_ending=line_ending,
                                   column_delimiter=column_delimiter,
                                   quoting=quoting
                                   )
    else:
        total_records = count_csv(data=records,
                                   skip_header=True,
                                   line_ending=line_ending,
                                   column_delimiter=column_delimiter,
                                    quoting=quoting
                                    )
    csv_data_size = os.path.getsize(filename) if filename else sys.getsizeof(
        records
        )
    _max_records: int = max_records or total_records
    _max_records = min(_max_records,
                       total_records
                       )
    max_bytes = min(csv_data_size,
        MAX_INGEST_JOB_FILE_SIZE - 1 * 1024 * 1024
        )  # -1 MB for sentinel

    dl = DELIMITERS[column_delimiter]
    le = LINE_ENDINGS[line_ending]

    def flush(header: list[str], records: list[list[str]]) -> io.StringIO:
        buffer = io.StringIO()
        writer = csv.writer(
            buffer,
            delimiter=dl,
            lineterminator=le,
            quoting=quoting,
        )
        writer.writerow(header)
        writer.writerows(records)
        return buffer

    def split(csv_reader: Iterator[list[str]]) -> Generator[tuple[int, str], None, None]:
        fieldnames = next(csv_reader)
        records_size = 0
        bytes_size = 0
        buff: list[list[str]] = []
        for line in csv_reader:
            line_data_size = len(f"{dl}".join(line).encode("utf-8"))  # rough estimate
            records_size += 1
            bytes_size += line_data_size
            if records_size > _max_records or bytes_size > max_bytes:
                if buff:
                    yield records_size - 1, flush(fieldnames, buff).getvalue()
                records_size = 1
                bytes_size = line_data_size
                buff = [line]
            else:
                buff.append(line)
        if buff:
            yield records_size, flush(fieldnames, buff).getvalue()

    if filename:
        with open(filename, encoding="utf-8") as bis:
            reader = csv.reader(
                bis, delimiter=dl, lineterminator=le, quoting=quoting
            )
            yield from split(reader)
    elif records:
        reader = csv.reader(
            io.StringIO(records), delimiter=dl, lineterminator=le, quoting=quoting
        )
        yield from split(reader)
    else:
        raise ValueError("Either filename or records must be provided")


def count_csv(
        filename: Optional[str] = None,
        data: Optional[str] = None,
        skip_header: bool = False,
        line_ending: LineEnding = LineEnding.LF,
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        quoting: QUOTING_TYPE = csv.QUOTE_MINIMAL
        ) -> int:
    """Count the number of records in a CSV file."""
    dl = DELIMITERS[column_delimiter]
    le = LINE_ENDINGS[line_ending]
    if filename:
        with open(filename, encoding="utf-8") as bis:
            reader = csv.reader(
                bis, delimiter=dl, lineterminator=le, quoting=quoting
            )
            if skip_header: next(reader)
            count = sum(1 for _ in reader)
    elif data:
        reader = csv.reader(
            io.StringIO(data), delimiter=dl, lineterminator=le, quoting=quoting
        )
        if skip_header: next(reader)
        count = sum(1 for _ in reader)
    else:
        raise ValueError("Either filename or data must be provided")

    return count


def convert_dict_to_csv(
        data: Optional[list[dict[str, str]]],
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        line_ending: LineEnding = LineEnding.LF,
        quoting: QUOTING_TYPE = csv.QUOTE_MINIMAL,
        sort_keys: bool = False,
        ) -> Optional[str]:
    """Converts list of dicts to CSV like object."""
    if not data:
        return None
    dl = DELIMITERS[column_delimiter]
    le = LINE_ENDINGS[line_ending]
    keys = set(i for s in [d.keys() for d in data] for i in s)
    if sort_keys:
        keys = list(sorted(keys))
    dict_to_csv_file = io.StringIO()
    writer = csv.DictWriter(dict_to_csv_file,
                            fieldnames=keys,
                            delimiter=dl,
                            lineterminator=le,
                            quoting=quoting
                            )
    writer.writeheader()
    for row in data:
        writer.writerow(row)
    return dict_to_csv_file.getvalue()


def get_csv_fieldnames(
        filename: Optional[str] = None,
        records: Optional[list[dict[str, str]]] = None,
        line_ending: LineEnding = LineEnding.LF,
        column_delimiter: ColumnDelimiter = ColumnDelimiter.COMMA,
        quoting: QUOTING_TYPE = csv.QUOTE_MINIMAL
) -> list[str]:
    """Get fieldnames from a CSV file or list of records."""
    dl = DELIMITERS[column_delimiter]
    le = LINE_ENDINGS[line_ending]
    if filename:
        with open(filename, encoding="utf-8") as bis:
            reader = csv.reader(
                bis, delimiter=dl, lineterminator=le, quoting=quoting
            )
            filenames = next(reader)
    elif records:
        filenames = list(records[0].keys())
    else:
        raise ValueError("Either filename or records must be provided")
    return filenames