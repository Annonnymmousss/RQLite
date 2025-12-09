# SQLite Database Reader

A Rust implementation of a SQLite database file reader that can parse and query SQLite database files without using the SQLite library. This tool reads the binary format of SQLite files directly and supports basic SQL operations.

## Overview

This project implements a custom SQLite database reader that parses the SQLite file format specification to read database contents. It supports reading table schemas, counting rows, and executing basic SELECT queries with WHERE clauses and indexed lookups.

## Features

- Database metadata inspection (.dbinfo command)
- List all tables in a database (.tables command)
- Execute SELECT queries with single or multiple columns
- Support for WHERE clauses with equality comparisons
- Index-based query optimization when applicable
- COUNT(*) aggregate function support
- B-tree traversal for both table and index pages

## Requirements

- Rust (latest stable version recommended)
- Cargo (comes with Rust)

### Dependencies

```toml
[dependencies]
anyhow = "*"
```

## Installation

1. Clone or download the project
2. Navigate to the project directory
3. Build the project:

```bash
cargo build --release
```

The compiled binary will be available at `target/release/[binary_name]`

## Usage

### Basic Command Structure

```bash
./program <database_path> <command>
```

### Supported Commands

#### 1. Database Information

Display basic database metadata including page size and number of tables:

```bash
./program sample.db .dbinfo
```

Output:
```
database page size: 4096
number of tables: 3
```

#### 2. List Tables

Display all table names in the database:

```bash
./program sample.db .tables
```

Output:
```
apples oranges grapes
```

#### 3. Count Rows

Count the total number of rows in a specific table:

```bash
./program sample.db "SELECT COUNT(*) FROM apples"
```

Output:
```
4
```

#### 4. Select Single Column

Retrieve all values from a specific column:

```bash
./program sample.db "SELECT name FROM apples"
```

Output:
```
Granny Smith
Fuji
Honeycrisp
Golden Delicious
```

#### 5. Select Multiple Columns

Retrieve multiple columns from a table:

```bash
./program sample.db "SELECT id, name, color FROM apples"
```

Output:
```
1|Granny Smith|Light Green
2|Fuji|Red
3|Honeycrisp|Blush Red
4|Golden Delicious|Yellow
```

#### 6. Select with WHERE Clause

Filter rows based on a condition:

```bash
./program sample.db "SELECT id, name FROM apples WHERE color = 'Red'"
```

Output:
```
2|Fuji
```

The WHERE clause supports:
- Equality comparisons (=)
- String values in single or double quotes
- Automatic index usage when available

## Architecture

### Key Components

#### 1. Database Header Parsing

The first 100 bytes of a SQLite database file contain the database header with metadata such as:
- Page size (bytes 16-17)
- File format version
- Database encoding

#### 2. Page Structure

SQLite organizes data into fixed-size pages. The program handles:
- **B-tree interior pages** (0x05 for tables, 0x02 for indexes)
- **B-tree leaf pages** (0x0D for tables, 0x0A for indexes)

Each page contains:
- Page header (8 or 12 bytes)
- Cell pointer array
- Cells containing actual data

#### 3. Schema Table

The sqlite_schema (formerly sqlite_master) table is stored on page 1 and contains:
- Table definitions
- Index definitions
- Root page numbers
- SQL CREATE statements

#### 4. Variable-Length Integer (Varint) Encoding

SQLite uses a variable-length integer format where:
- Most significant bit indicates continuation
- Values can be 1-9 bytes long
- Used for sizes, rowids, and serial types

#### 5. Serial Type System

Column values are encoded with serial types that indicate:
- NULL (0)
- Integers (1-7)
- Float (7)
- Blob and Text (12+)

### Query Processing Flow

1. **Parse SQL command** - Extract table name, columns, and conditions
2. **Read schema** - Locate table definition in sqlite_schema
3. **Get column indexes** - Parse CREATE TABLE statement
4. **Check for indexes** - Look for applicable indexes for WHERE clauses
5. **Traverse B-tree** - Navigate table or index pages
6. **Extract data** - Parse cell records and extract requested columns
7. **Format output** - Return results in pipe-delimited format

### Index Optimization

When a WHERE clause references an indexed column:
1. Search the index B-tree for matching keys
2. Retrieve rowid(s) from index entries
3. Look up specific rows in table B-tree using rowids
4. Extract and return requested columns

This avoids full table scans for indexed queries.

## File Format Details

### Database Header (100 bytes)

- Bytes 0-15: Magic header string "SQLite format 3\0"
- Bytes 16-17: Page size (big-endian uint16)
- Bytes 18-19: File format versions
- Remaining bytes: Various database properties

### Page Header (Table Leaf - 0x0D)

- Byte 0: Page type
- Bytes 1-2: First freeblock offset
- Bytes 3-4: Cell count (big-endian uint16)
- Bytes 5-6: Cell content area offset
- Byte 7: Fragmented free bytes
- Bytes 8+: Cell pointer array (2 bytes per cell)

### Cell Structure (Table Leaf)

- Varint: Payload size
- Varint: Rowid
- Varint: Record header size
- Varint[]: Serial types for each column
- Bytes: Column data based on serial types

## Error Handling

The program uses the `anyhow` crate for error handling and will return appropriate error messages for:
- Missing or invalid database files
- Malformed SQL commands
- Non-existent tables or columns
- File I/O errors
- Invalid database format

## Limitations

### SQL Support

This implementation supports only a subset of SQL:
- SELECT with single table (no JOINs)
- WHERE with single equality condition
- COUNT(*) aggregate
- No support for: ORDER BY, GROUP BY, LIMIT, subqueries, functions (except COUNT)

### Data Types

- Text and integer types are fully supported
- BLOB data is read as text (may produce invalid UTF-8)
- Real/Float types are read but not specially formatted
- NULL values are handled but may appear as empty strings

### Performance

- No query optimization beyond index usage
- No caching mechanism
- Full page reads even for small queries
- Not suitable for very large databases

## Implementation Notes

### B-tree Traversal

The code implements recursive B-tree traversal:
- Interior pages contain child page pointers
- Leaf pages contain actual data
- Right-most child is stored separately in interior page header

### Memory Safety

- Uses Rust's ownership system for memory safety
- No unsafe code blocks
- Bounded array access with proper length checks

### Column Index Resolution

Column indexes are determined by parsing the CREATE TABLE statement:
- Splits on commas to find column definitions
- Handles quoted identifiers (", `, [])
- Case-insensitive column name matching

## Testing

Test the program with various SQLite databases:

```bash
# Create a test database
sqlite3 test.db "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT);"
sqlite3 test.db "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');"
sqlite3 test.db "CREATE INDEX idx_email ON users(email);"

# Run queries
./program test.db .tables
./program test.db "SELECT * FROM users"
./program test.db "SELECT name FROM users WHERE email = 'alice@example.com'"
```

## Future Enhancements

Potential improvements for this implementation:

- Support for additional SQL clauses (ORDER BY, LIMIT, OFFSET)
- Multiple WHERE conditions with AND/OR
- JOIN operations
- Additional aggregate functions (SUM, AVG, MIN, MAX)
- LIKE pattern matching
- Type-aware formatting for numeric and date types
- Query result caching
- Better error messages with line/column information
- Support for reading overflow pages
- Transaction log (WAL) support

## References

- [SQLite File Format Documentation](https://www.sqlite.org/fileformat.html)
- [SQLite Documentation](https://www.sqlite.org/docs.html)
- [Varint Encoding](https://www.sqlite.org/fileformat.html#varint)

## License

This is an educational implementation. Refer to your project's license file for usage terms.

## Contributing

Contributions are welcome. Please ensure:
- Code follows Rust style guidelines (rustfmt)
- New features include appropriate error handling
- Complex logic includes inline comments
- Test cases are provided for new functionality
