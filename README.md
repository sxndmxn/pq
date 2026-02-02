# pq - Fast Parquet CLI

A jq-like CLI for Parquet files. Fast startup, pretty output, sensible defaults.

## Installation

```bash
cargo install --path .
```

## Usage

```
pq <COMMAND> [OPTIONS] <FILE>

Commands:
  schema    Show schema (column names, types, nullability)
  head      Show first N rows (default 10)
  tail      Show last N rows (default 10)
  count     Count total rows
  stats     Column statistics (min, max, nulls)
  query     Run SQL query against file
  convert   Convert to CSV, JSON, or JSONL
  merge     Merge multiple parquet files
  info      File metadata (row groups, compression, size)
```

### Global Options

```
-o, --output <FORMAT>   Output format: table, json, jsonl, csv [default: table]
-n, --rows <N>          Number of rows for head/tail [default: 10]
-q, --quiet             Suppress headers and formatting
```

## Examples

### View schema

```bash
$ pq schema data.parquet
+--------+------------+----------+
| Column | Type       | Nullable |
+================================+
| id     | INT64      | Yes      |
| name   | BYTE_ARRAY | Yes      |
| amount | DOUBLE     | Yes      |
+--------+------------+----------+
```

### Preview data

```bash
$ pq head data.parquet -n 5
+----+---------+--------+
| id | name    | amount |
+=========================+
| 1  | Alice   | 100.5  |
| 2  | Bob     | 200.75 |
| 3  | Charlie | 150.25 |
+----+---------+--------+

$ pq tail data.parquet -n 2
```

### Count rows

```bash
$ pq count data.parquet
1000000

$ pq count *.parquet
part1.parquet: 500000
part2.parquet: 500000
Total: 1000000
```

### Column statistics

```bash
$ pq stats data.parquet
+--------+--------+-------+-------+------+
| Column | Type   | Nulls | Min   | Max  |
+==========================================+
| id     | INT64  | 0     | 1     | 1000 |
| name   | STRING | 5     | Alice | Zoe  |
+--------+--------+-------+-------+------+
```

### SQL queries

```bash
$ pq query "SELECT name, SUM(amount) FROM tbl GROUP BY name" data.parquet
+---------+-------------+
| name    | SUM(amount) |
+===========================+
| Alice   | 1500.50     |
| Bob     | 2300.75     |
+---------+-------------+

$ pq query "SELECT * FROM tbl WHERE amount > 100" data.parquet --output json
```

### File info

```bash
$ pq info data.parquet
+-------------+----------------------------------+
| Key         | Value                            |
+================================================+
| File        | data.parquet                     |
| File Size   | 1.26 KB                          |
| Rows        | 1000                             |
| Columns     | 4                                |
| Row Groups  | 1                                |
| Compression | SNAPPY                           |
+-------------+----------------------------------+
```

### Convert formats

```bash
$ pq convert data.parquet output.csv
$ pq convert data.parquet output.json
$ pq convert data.parquet output.jsonl
```

### Merge files

```bash
$ pq merge part1.parquet part2.parquet -o combined.parquet
```

### Output formats

All commands support multiple output formats:

```bash
$ pq head data.parquet --output table   # Pretty table (default)
$ pq head data.parquet --output json    # JSON array
$ pq head data.parquet --output jsonl   # JSON Lines
$ pq head data.parquet --output csv     # CSV
```

### Glob support

```bash
$ pq count data/*.parquet
$ pq schema *.parquet
```

## Features

- Sub-100ms startup time
- Streams data for files larger than RAM
- SQL queries via DataFusion
- Multiple output formats
- Glob pattern support
- Snappy compression for merge output

## License

MIT
